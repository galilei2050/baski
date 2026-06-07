"""Tool for searching the web via Google SerpApi."""

from typing import Any, ClassVar

from baski.clients.serpapi_client import SerpApiClient

from ..tool import Tool


class GoogleSearchTool(Tool):
    """Tool for searching the web using Google via SerpApi."""

    name = "google_search"
    one_line = "Search Google for current information on any topic"
    description = (
        "Search Google for information. Use this when you need current information, facts, or data from the web. "
        "Examples: Scope to a site with 'topic site:example.com', "
        "use exact phrases with '\"exact phrase\"', "
        "or combine terms with 'keyword1 keyword2 -exclude'"
    )
    input_schema: ClassVar[Any] = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The search query to execute on Google. Supports operators like site:, intitle:, inurl:",
            }
        },
        "required": ["query"],
    }

    def __init__(self, serpapi_client: SerpApiClient) -> None:
        """Store the SerpApi client."""
        self.serpapi_client = serpapi_client

    async def execute(self, query: str) -> str:  # type: ignore[override]
        """Execute Google search and return formatted results."""
        results = await self.serpapi_client.search_google(q=query)

        lines = []

        organic_results = results.get("organic_results", [])
        if organic_results:
            lines.append("Google Search Results:")
            lines.append(self._format_organic_results(organic_results))

        knowledge_graph = results.get("knowledge_graph")
        if knowledge_graph:
            lines.append("Google Knowledge Graph:")
            lines.append(self._format_knowledge_graph(knowledge_graph))

        answer_box = results.get("answer_box")
        if answer_box:
            lines.append("Google Direct Answer:")
            lines.append(self._format_answer_box(answer_box))

        return "\n\n".join(lines) if lines else f"No results found for query: {query}"

    def _format_organic_results(self, organic_results: list[Any]) -> str:
        """Format organic search results."""
        lines = []
        for i, result in enumerate(organic_results[:5], 1):
            title = result.get("title", "No title")
            snippet = result.get("snippet", "No description")
            link = result.get("link", "")
            lines.append(f"{i}. {title}\n   {snippet}\n   {link}")
        return "\n".join(lines)

    def _format_knowledge_graph(self, knowledge_graph: dict) -> str:  # noqa: ANON002 — SerpAPI JSON response item, schema varies
        """Format knowledge graph information."""
        lines = []
        title = knowledge_graph.get("title", "")
        description = knowledge_graph.get("description", "")
        if title:
            lines.append(f"  {title}")
        if description:
            lines.append(f"  {description}")
        return "\n".join(lines)

    def _format_answer_box(self, answer_box: dict) -> str:  # noqa: ANON002 — SerpAPI JSON response item, schema varies
        """Format answer box information."""
        lines = []
        answer = answer_box.get("answer") or answer_box.get("snippet")
        if answer:
            lines.append(f"  {answer}")
        return "\n".join(lines)
