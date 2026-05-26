import asyncio
from datetime import UTC
from datetime import datetime as _dt
from http import HTTPStatus
from pathlib import Path
from typing import Self

from bs4 import BeautifulSoup
from httpx import HTTPStatusError
from httpx import Request as HttpxRequest
from httpx import Response as HttpxResponse
from markdownify import markdownify as md
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from ..server.logger import Logger

__all__ = ["PlaywrightClient"]

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

    Note: Requires 'playwright install firefox' after pip install
    """

    def __init__(self, headless: bool = True, logger: Logger | None = None, timeout: int = 90000) -> None:
        self.headless = headless
        self.logger = logger
        self.timeout = timeout
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    async def __aenter__(self) -> Self:
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.firefox.launch(headless=self.headless)
        self._context = await self._browser.new_context(user_agent=_SAFARI_UA, viewport={"width": 1600, "height": 900})
        self._context.set_default_timeout(self.timeout)
        self._context.set_default_navigation_timeout(self.timeout)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

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

        if self.logger:
            self.logger.info("Fetching page", labels={"url": url})

        try:
            await self._safe_goto(page, url)
            html_content = await page.content()
        except Exception as e:
            await self._dump_error_context(page, url, e)
            raise
        finally:
            await page.close()

        soup = BeautifulSoup(html_content, "html.parser")

        for tag in soup(["script", "style", "meta", "link", "noscript", "iframe", "nav", "header", "footer", "aside"]):
            tag.decompose()

        nav_selectors = [
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
        ]

        for selector in nav_selectors:
            for element in soup.find_all(attrs={"class": lambda x, s=selector: x and s in " ".join(x).lower()}):
                element.decompose()
            for element in soup.find_all(attrs={"id": lambda x, s=selector: x and s in x.lower()}):
                element.decompose()

        return md(str(soup), heading_style="atx")

    async def _safe_goto(self, page: Page, url: str) -> None:
        """Navigate to URL with retry logic for network errors and timeouts."""
        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
                response = await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_load_state("load")
                await page.wait_for_selector("body")

                if (
                    response
                    and response.status >= HTTPStatus.BAD_REQUEST
                    and response.status != _STATUS_INVALID_SENTINEL
                ):
                    mock_request = HttpxRequest("GET", url)
                    mock_response = HttpxResponse(status_code=response.status, request=mock_request)
                    last_error = HTTPStatusError(
                        message=f"HTTP {response.status} error for {url}",
                        request=mock_request,
                        response=mock_response,
                    )
                    if self.logger:
                        self.logger.info(f"Attempt {attempt} failed", labels={"url": url, "status": response.status})
                    continue

            except (PlaywrightError, PlaywrightTimeoutError) as e:
                error_msg = str(e)
                is_retryable = any(
                    pattern in error_msg
                    for pattern in [
                        "net::ERR_ABORTED",
                        "net::ERR_HTTP_RESPONSE_CODE_FAILURE",
                        "net::ERR_TIMED_OUT",
                        "Timeout",
                        "net::ERR_PROXY_CONNECTION_FAILED",
                    ]
                )

                if not is_retryable:
                    raise

                last_error = e
                if self.logger:
                    self.logger.info(
                        f"Attempt {attempt} failed, retrying", labels={"url": url, "error": error_msg[:100]}
                    )
            else:
                return

        raise last_error or RuntimeError(f"Failed to fetch page: {url}")

    async def _dump_error_context(self, page: Page, url: str, error: Exception) -> None:
        """Save screenshot and HTML on error for debugging."""
        if not self.logger:
            return

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

            self.logger.info(
                "Error context saved",
                labels={"url": url, "screenshot": screenshot_path, "html": html_path, "error": str(error)[:100]},
            )
        except Exception as e:  # noqa: BLE001 — debug dump must not crash the main error handler
            self.logger.info("Failed to dump error context", labels={"error": str(e)})
