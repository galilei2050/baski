"""Tool for fetching Google Play developer app data via SerpApi."""

from pydantic import BaseModel, Field

from baski.clients.serpapi_client import SerpApiClient
from baski.server import Logger

from ..tool import Tool


class GooglePlayTool(Tool):
    """Tool for fetching Google Play developer apps via SerpApi."""

    name = "fetch_google_play_data"
    one_line = "Fetch Google Play developer apps"
    description = (
        "Fetch Google Play developer apps and data using developer ID. Returns list of apps published by the developer."
    )

    class Input(BaseModel):
        """Arguments for a Google Play developer lookup."""

        developer_id: str = Field(description="Google Play developer ID (e.g., 'Zibra+AI' or 'com.developer.name')")

    def __init__(self, serpapi_client: SerpApiClient, logger: Logger) -> None:
        """Store SerpApi client and logger."""
        self.serpapi_client = serpapi_client
        self.logger = logger

    async def execute(self, developer_id: str) -> str:  # type: ignore[override]
        """Fetch Google Play data for a developer and return formatted result."""
        result = await self.serpapi_client.search_google_play(developer_id)

        if not result:
            return f"No Google Play data found for developer: {developer_id}"

        app_highlight = result.get("app_highlight")
        if not app_highlight:
            return f"No featured app found for developer: {developer_id}"

        return self._format_app(app_highlight)

    def _format_app(self, app_highlight: dict) -> str:  # noqa: ANON002 — SerpAPI JSON response item, schema varies
        """Format a Google Play app highlight into a human-readable string."""
        parts = []

        title = app_highlight.get("title")
        if title:
            parts.append(f"# {title}\n")

        description = app_highlight.get("description")
        if description:
            parts.append(description)

        details = self._collect_details(app_highlight)
        if details:
            parts.append("Google Play: " + ", ".join(details))

        return "\n\n".join(parts) if parts else "No relevant Google Play data extracted"

    def _collect_details(self, app_highlight: dict) -> list[str]:  # noqa: ANON002 — SerpAPI JSON response item, schema varies
        """Collect Google Play metadata detail strings."""
        details = []
        rating = app_highlight.get("rating")
        if rating:
            details.append(f"Rating: {rating:.1f}/5")
        review_count = app_highlight.get("reviews")
        if review_count:
            details.append(f"Reviews: {review_count:,}")
        installs = app_highlight.get("installs")
        if installs:
            details.append(f"Installs: {installs}")
        return details
