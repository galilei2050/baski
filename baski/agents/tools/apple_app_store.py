"""Tool for fetching Apple App Store data via SerpApi."""

from pydantic import BaseModel, Field

from baski.clients.serpapi_client import SerpApiClient

from ..tool import Tool


class AppleAppStoreTool(Tool):
    """Tool for fetching Apple App Store data via SerpApi."""

    name = "app_store_app"
    one_line = "Fetch an app's Apple App Store listing"
    description = (
        "Fetch Apple App Store data by searching for company/app name. "
        "Returns app information including title, rating, developer, and description."
    )

    class Input(BaseModel):
        """Arguments for an App Store lookup."""

        company_name: str = Field(description="Company or app name to search for (e.g., 'Zibra AI')")

    def __init__(self, serpapi_client: SerpApiClient) -> None:
        """Store the SerpApi client."""
        self.serpapi_client = serpapi_client

    async def execute(self, company_name: str) -> str:  # type: ignore[override]
        """Search App Store by company name and return formatted app details."""
        product_id = await self._find_product_id(company_name)
        if product_id is None:
            return f"No App Store results found for '{company_name}'"

        product_data = await self.serpapi_client.get_apple_product(product_id)
        if not product_data:
            return f"No App Store product data found for ID: {product_id}"

        return self._format_product(product_data)

    async def _find_product_id(self, company_name: str) -> str | None:
        """Search App Store and return the first product ID found."""
        search_results = await self.serpapi_client.search_apple_app_store(term=company_name)
        if not search_results or not search_results.get("organic_results"):
            return None
        first_result = search_results["organic_results"][0]
        if not isinstance(first_result, dict):
            return None
        return first_result.get("product_id")

    def _format_product(self, product_data: dict) -> str:  # noqa: ANON002 — SerpAPI JSON response item, schema varies
        """Format product data into a human-readable string."""
        parts = []

        title = product_data.get("title")
        if title:
            parts.append(f"# {title}\n")

        developer = product_data.get("developer")
        if developer:
            parts.append(f"Developer: {developer}")

        description = product_data.get("description")
        if description:
            parts.append(description)

        details = self._collect_details(product_data)
        if details:
            parts.append("App Store: " + ", ".join(details))

        return "\n\n".join(parts) if parts else "No relevant App Store data extracted"

    def _collect_details(self, product_data: dict) -> list[str]:  # noqa: ANON002 — SerpAPI JSON response item, schema varies
        """Collect App Store metadata detail strings."""
        details = []
        rating = product_data.get("rating")
        if rating:
            details.append(f"Rating: {rating:.1f}/5")
        review_count = product_data.get("reviews")
        if isinstance(review_count, int):
            details.append(f"Reviews: {review_count:,}")
        version = product_data.get("version")
        if version:
            details.append(f"Version: {version}")
        return details
