"""Tool for searching Yelp business listings via SerpApi."""

from pydantic import BaseModel, Field

from baski.clients.serpapi_client import SerpApiClient

from ..tool import Tool


class YelpSearchTool(Tool):
    """Tool for searching Yelp businesses via SerpApi."""

    name = "yelp_search"
    one_line = "Search Yelp for local businesses"
    description = (
        "Search Yelp for local businesses by keyword and location. "
        "Use this when you need to find restaurants, shops, services, or other local businesses. "
        "Returns ratings, reviews, prices, categories, and links."
    )

    class Input(BaseModel):
        """Arguments for a Yelp business search."""

        query: str = Field(description="What to search for (e.g. 'auto repair', 'pizza', 'plumber')")
        location: str = Field(description="Location to search in (e.g. 'San Francisco, CA', 'Belmont, CA')")

    def __init__(self, serpapi_client: SerpApiClient) -> None:
        """Store the SerpApi client."""
        self.serpapi_client = serpapi_client

    async def execute(self, query: str, location: str) -> str:  # type: ignore[override]
        """Search Yelp and return formatted business listings."""
        results = await self.serpapi_client.search_yelp(find_desc=query, find_loc=location)

        organic = results.get("organic_results", [])
        if not organic:
            return f"No Yelp results found for '{query}' in {location}"

        lines = [f"Yelp Results: {query} in {location}\n"]
        for biz in organic:
            lines.extend(self._format_business(biz))

        return "\n".join(lines)

    def _format_business(self, biz: dict) -> list[str]:  # noqa: ANON002 — SerpAPI JSON response item, schema varies
        """Format a single Yelp business entry into display lines."""
        lines = [f"- **{biz.get('title')}**"]
        if biz.get("rating"):
            lines.append(f"  - Rating: {biz['rating']} ({biz.get('reviews', 0)} reviews)")
        if biz.get("price"):
            lines.append(f"  - Price: {biz['price']}")
        if biz.get("categories"):
            cats = ", ".join(c.get("title", "") for c in biz["categories"] if isinstance(c, dict))
            if cats:
                lines.append(f"  - Categories: {cats}")
        if biz.get("neighborhoods"):
            lines.append(f"  - Neighborhoods: {', '.join(biz['neighborhoods'])}")
        if biz.get("link"):
            lines.append(f"  - Link: {biz['link']}")
        lines.append("")
        return lines
