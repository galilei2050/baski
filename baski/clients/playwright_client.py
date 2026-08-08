"""Async Playwright client that fetches pages and converts them to cleaned markdown."""

import asyncio
import io
import logging
from datetime import UTC
from datetime import datetime as _dt
from http import HTTPStatus
from pathlib import Path
from typing import Any, Self, cast

import httpx
import trafilatura
from anyio import Path as AsyncPath
from bs4 import BeautifulSoup
from httpx import HTTPStatusError
from httpx import Request as HttpxRequest
from httpx import Response as HttpxResponse
from markdownify import markdownify as md
from playwright.async_api import Browser, BrowserContext, Page, Playwright, StorageState, async_playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from pypdf import PdfReader

__all__ = ["PlaywrightClient"]

logger = logging.getLogger(__name__)

_SAFARI_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_2) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.4.1 Safari/605.1.15"
)
_ERROR_DIR = Path("/tmp/playwright_errors")  # noqa: S108 — intentional temp dir for debug dumps
_STATUS_INVALID_SENTINEL = 999  # Cloudflare / non-standard "success" status to skip error-raising


class PlaywrightClient:
    """Async context manager for Playwright browser operations.

    Usage:
        async with PlaywrightClient(headless=True) as client:
            markdown = await client.fetch_page_markdown(url)

    Note: Requires 'playwright install chromium' after pip install
    """

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout: int = 90000,
        storage_state: str | None = None,
        cdp_url: str | None = None,
    ) -> None:
        """Configure browser launch options; the browser is started in ``__aenter__``.

        ``storage_state`` is a path to a Playwright storage-state file (cookies + localStorage). When
        it points at an existing file, the context starts from that saved session, so pages behind a
        login are reachable. A missing file is ignored — the context starts logged-out.

        ``cdp_url`` attaches to a remote browser over CDP (e.g. a managed/fortified browser like
        Browserbase) instead of launching a local Chromium — the way to act on sites whose bot
        protection rejects an automated local browser. In CDP mode there is a single shared context
        (the remote browser's), so ``new_context`` returns it and merges any ``storage_state`` cookies
        in rather than opening a fresh isolated jar.
        """
        self.headless = headless
        self.timeout = timeout
        self.storage_state = storage_state
        self.cdp_url = cdp_url
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    async def __aenter__(self) -> Self:
        """Start Playwright and open the default browser context, loading a saved session when present."""
        self._playwright = await async_playwright().start()
        if self.cdp_url:
            self._browser = await self._playwright.chromium.connect_over_cdp(self.cdp_url)
            self._context = self._browser.contexts[0] if self._browser.contexts else await self._browser.new_context()
        else:
            self._browser = await self._playwright.chromium.launch(headless=self.headless)
            self._context = await self.new_context(self.storage_state)
        return self

    async def new_context(
        self, storage_state: str | StorageState | None = None, proxy: dict[str, str] | None = None
    ) -> BrowserContext:
        """Open an isolated browser context with the shared config, loading a saved session when present.

        Each context is a separate cookie/storage jar — callers that need per-tenant logins (e.g. one
        per chat) open one context each. ``storage_state`` is either a path to a Playwright
        storage-state file (a missing file is ignored — logged-out) or the state dict itself (cookies
        + origins, e.g. fetched from a DB); the dict is passed straight through. ``proxy`` is
        Playwright's context proxy (``server``/``username``/``password``); None = direct.
        """
        if not self._browser:
            raise RuntimeError("PlaywrightClient not initialized. Use async with context manager.")
        if self.cdp_url:
            # Remote browser: one shared context (its fingerprint/proxy live there); reuse it and merge
            # in the saved cookies rather than opening an isolated jar that would lose that setup.
            context = self._browser.contexts[0] if self._browser.contexts else await self._browser.new_context()
            cookies = storage_state.get("cookies") if isinstance(storage_state, dict) else None
            if cookies:
                await context.add_cookies(cast("Any", cookies))  # StorageState cookie ≈ SetCookieParam at runtime
            context.set_default_timeout(self.timeout)
            context.set_default_navigation_timeout(self.timeout)
            return context
        context_args: dict[str, Any] = {"user_agent": _SAFARI_UA, "viewport": {"width": 1600, "height": 900}}
        if isinstance(storage_state, dict) or (storage_state and await AsyncPath(storage_state).exists()):
            context_args["storage_state"] = storage_state
        if proxy:
            context_args["proxy"] = proxy
        context = await self._browser.new_context(**context_args)
        context.set_default_timeout(self.timeout)
        context.set_default_navigation_timeout(self.timeout)
        return context

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Close browser context, browser, and stop Playwright."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def new_page(self) -> Page:
        """Open a blank page in the shared context for a caller to drive (interactive/headed use)."""
        if not self._context:
            raise RuntimeError("PlaywrightClient not initialized. Use async with context manager.")
        return await self._context.new_page()

    async def save_storage_state(self, path: str) -> None:
        """Write the context's session (cookies + localStorage) to a Playwright storage-state file."""
        if not self._context:
            raise RuntimeError("PlaywrightClient not initialized. Use async with context manager.")
        await self._context.storage_state(path=path)

    async def fetch_page_markdown(self, url: str) -> str:
        """Fetch webpage content and convert to markdown with retry logic.

        Args:
            url: URL to fetch

        Returns:
            Markdown content of the page

        """
        if not self._context:
            raise RuntimeError("PlaywrightClient not initialized. Use async with context manager.")

        page = await self._context.new_page()
        logger.info("Fetching page", extra={"url": url})

        try:
            await self._safe_goto(page, url)
            html_content = await page.content()
        except PlaywrightError as e:
            if _DOWNLOAD_MARKER not in str(e):
                await self._dump_error_context(page, url, e)
                raise
            logger.info("Url serves a document, not a page", extra={"url": url})
            return await _fetch_document(url)
        except Exception as e:
            await self._dump_error_context(page, url, e)
            raise
        finally:
            await page.close()

        return _html_to_markdown(html_content)

    async def _safe_goto(self, page: Page, url: str) -> None:
        """Navigate to URL, retrying only failures that could plausibly go the other way.

        One deadline covers the whole attempt. Without it the three waits inside `_attempt_goto`
        (navigate, load, body) each claim the full timeout, so a page that never renders burned
        3 x 3 x 90 s; the recorded failures ran to 464 s while the slowest SUCCESSFUL fetch in the
        same corpus took 122 s and p99 took 80 s. The caller is a person waiting in a chat.
        """
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                async with asyncio.timeout(self.timeout / 1000):
                    http_error = await self._attempt_goto(page, url)
            except TimeoutError as e:
                raise TimeoutError(f"Page did not load within {self.timeout / 1000:.0f}s: {url}") from e
            except (PlaywrightError, PlaywrightTimeoutError) as e:
                if not _is_retryable_error(str(e)):
                    raise
                last_error = e
                logger.info(
                    f"Attempt {attempt} failed, retrying",
                    extra={"url": url, "error": str(e)[:100]},
                )
                continue
            if http_error is None:
                return
            last_error = http_error
            logger.info(
                f"Attempt {attempt} failed",
                extra={"url": url, "status": http_error.response.status_code},
            )
        raise last_error or RuntimeError(f"Failed to fetch page: {url}")

    async def _attempt_goto(self, page: Page, url: str) -> HTTPStatusError | None:
        """Single navigation attempt; returns an HTTPStatusError on bad status, else None."""
        response = await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_load_state("load")
        await page.wait_for_selector("body")
        if response and response.status >= HTTPStatus.BAD_REQUEST and response.status != _STATUS_INVALID_SENTINEL:
            mock_request = HttpxRequest("GET", url)
            mock_response = HttpxResponse(status_code=response.status, request=mock_request)
            return HTTPStatusError(
                message=f"HTTP {response.status} error for {url}",
                request=mock_request,
                response=mock_response,
            )
        return None

    async def _dump_error_context(self, page: Page, url: str, error: Exception) -> None:
        """Save screenshot and HTML on error for debugging."""
        await asyncio.to_thread(_ERROR_DIR.mkdir, parents=True, exist_ok=True)
        timestamp = _dt.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        error_prefix = _ERROR_DIR / timestamp

        try:
            screenshot_bytes = await page.screenshot(type="png")
            html_content = await page.content()

            screenshot_path = f"{error_prefix}_screenshot.png"
            html_path = f"{error_prefix}_page.html"

            await asyncio.to_thread(Path(screenshot_path).write_bytes, screenshot_bytes)
            await asyncio.to_thread(Path(html_path).write_text, html_content)

            logger.info(
                "Error context saved",
                extra={
                    "url": url,
                    "screenshot": screenshot_path,
                    "html": html_path,
                    "error": str(error)[:100],
                },
            )
        except Exception as e:  # noqa: BLE001 — debug dump must not crash the main error handler
            logger.info("Failed to dump error context", extra={"error": str(e)})


# Chromium answers a url that serves a file — a PDF, a CSV — by starting a download instead of
# rendering, and reports it as this error. Measured over 663 recorded runs: 51 of the 93 browse
# failures were exactly this, 44 of them ending in `.pdf`, and the hosts were the authoritative ones
# — cdtfa.ca.gov, chp.ca.gov, Stanford, BYU, NORC. The agent was losing primary sources and falling
# back to second-hand pages, or to inventing the number.
_DOWNLOAD_MARKER = "Download is starting"
_PDF_MAGIC = b"%PDF-"
_DOCUMENT_TIMEOUT = 60.0


async def _fetch_document(url: str) -> str:
    """Download what the browser refused to render, and return its text.

    Only PDFs are decoded; anything else comes back as text if it is text, and raises otherwise. The
    point is to stop losing the source, not to become a file converter.
    """
    async with httpx.AsyncClient(follow_redirects=True, headers={"user-agent": _SAFARI_UA}) as client:
        response = await client.get(url, timeout=_DOCUMENT_TIMEOUT)
        response.raise_for_status()
    body = response.content
    if body.startswith(_PDF_MAGIC):
        return await asyncio.to_thread(_pdf_to_text, body, url)
    if response.headers.get("content-type", "").startswith("text/"):
        return response.text
    raise ValueError(f"{url} serves {response.headers.get('content-type', 'an unknown type')}, which is not readable")


def _pdf_to_text(body: bytes, url: str) -> str:
    """Page text, one blank line between pages, so a citation can name the page it came from."""
    reader = PdfReader(io.BytesIO(body))
    pages = [page.extract_text() or "" for page in reader.pages]
    text = "\n\n".join(
        f"[page {number}]\n{content.strip()}" for number, content in enumerate(pages, 1) if content.strip()
    )
    if not text:
        # A scanned PDF is images; saying so beats handing the agent an empty page it will fill in itself.
        raise ValueError(f"{url} is a PDF with no extractable text ({len(pages)} pages — likely scanned images)")
    return text


_NAV_SELECTORS = (
    "nav",
    "navigation",
    "navbar",
    "menu",
    "sidebar",
    "breadcrumb",
    "site-header",
    "site-footer",
    "top-bar",
    "header-nav",
)
_STRIP_TAGS = ("script", "style", "meta", "link", "noscript", "iframe", "nav", "header", "footer", "aside")


def _html_to_markdown(html: str) -> str:
    """Extract the main article as markdown, dropping nav/ads/cookie banners/boilerplate.

    trafilatura keeps the main-content node and discards the rest by default — far better
    than a tag blocklist, which can't catch ads/cookie/share widgets. It returns None when
    there's no clear main content (search-result pages, SPAs, sparse pages); fall back to
    selector stripping so those still yield something.

    Comments are excluded: trafilatura keeps them by default, so every article with a comment
    section, every forum thread and every review pile arrived as body text — bulk that a reader
    cannot cite and an agent pays to carry. `deduplicate` drops the repeated blocks that syndicated
    and paginated pages emit.
    """
    extracted = trafilatura.extract(
        html, output_format="markdown", include_images=False, include_comments=False, deduplicate=True
    )
    return extracted or _strip_to_markdown(html)


def _strip_to_markdown(html: str) -> str:
    """Fallback extractor: strip known boilerplate tags/selectors, convert the rest to markdown."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(_STRIP_TAGS):
        tag.decompose()
    for selector in _NAV_SELECTORS:
        _strip_by_attr(soup, "class", selector)
        _strip_by_attr(soup, "id", selector)
    return md(str(soup), heading_style="atx")


# Only failures a second attempt could plausibly survive: a dropped connection or a proxy hiccup.
# A timeout is not one of them and used to be listed here. Measured over 663 recorded runs: of 93
# failed fetches, exactly ONE url ever loaded successfully anywhere else — and none of the 24
# timeouts did. Retrying them bought nothing and cost the person in the chat minutes of silence.
_RETRYABLE_PATTERNS = (
    "net::ERR_ABORTED",
    "net::ERR_HTTP_RESPONSE_CODE_FAILURE",
    "net::ERR_PROXY_CONNECTION_FAILED",
)


def _is_retryable_error(error_msg: str) -> bool:
    return any(pattern in error_msg for pattern in _RETRYABLE_PATTERNS)


def _strip_by_attr(soup: BeautifulSoup, attr: str, needle: str) -> None:
    def matches(value: Any) -> bool:  # noqa: ANN401 — bs4 attr filter receives str | list | None
        if not value:
            return False
        text = " ".join(value) if isinstance(value, list) else value
        return needle in text.lower()

    for element in soup.find_all(attrs={attr: matches}):
        element.decompose()
