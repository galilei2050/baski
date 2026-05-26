from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx

from .. import get_env
from ..server.logger import Logger

__all__ = ["SerpApiClient"]


class SerpApiClient:
    BASE_URL = "https://serpapi.com"

    def __init__(
        self,
        logger: Logger,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._api_key = str(get_env("SERPAPI_API_KEY")).strip()
        self._http_client = http_client
        self._logger = logger

    async def request(self, method: str, engine: str, **kwargs: Any) -> dict:
        url = f"{self.BASE_URL}/search"
        params = kwargs.get("params", {})
        params["api_key"] = self._api_key
        params["engine"] = engine
        kwargs["params"] = params

        self._logger.info(
            "SerpApi request",
            labels={"serpapi_engine": engine, "params": {k: v for k, v in params.items() if k != "api_key"}},
        )

        response = await self._http_client.request(method=method, url=url, **kwargs)
        response.raise_for_status()
        return response.json()

    async def search_google(self, q: str) -> dict:
        return await self.request("GET", "google", params={"q": q, "gl": "us", "hl": "en"})

    async def search_yelp(self, find_desc: str, find_loc: str) -> dict:
        return await self.request(
            "GET",
            "yelp",
            params={
                "find_desc": find_desc,
                "find_loc": find_loc,
            },
        )

    async def search_google_play(self, q: str) -> dict:
        return await self.request("GET", "google_play", params={"q": q, "gl": "us", "hl": "en"})

    async def search_apple_app_store(self, term: str, **kwargs: Any) -> dict:
        params = {"term": term, "country": "us", "device": "mobile", "num": "10"}
        params.update(kwargs)

        return await self.request("GET", "apple_app_store", params=params)

    async def get_apple_product(self, product_id: str, **kwargs: Any) -> dict:
        params = {"product_id": product_id, "country": "us", "type": "app"}
        params.update(kwargs)

        return await self.request("GET", "apple_product", params=params)

    async def get_google_maps_reviews(self, data_id: str, amount: float | None = None, **kwargs: Any) -> dict:
        if amount is None:
            amount = float("inf")

        params = {"data_id": data_id, "hl": "en", "sort_by": "newestFirst"}
        params.update(kwargs)

        # Get first page
        data = await self.request("GET", "google_maps_reviews", params=params)
        last_response = data
        # Fetch additional pages if needed
        while len(data.get("reviews", [])) < amount:
            serpapi_pagination = last_response.get("serpapi_pagination") or {}
            next_page_token = serpapi_pagination.get("next_page_token")
            if not next_page_token:
                break

            params["next_page_token"] = next_page_token
            last_response = await self.request("GET", "google_maps_reviews", params=params)

            if "reviews" in last_response:
                data["reviews"].extend(last_response["reviews"])

        return data

    async def get_yelp_place(self, place_id: str, **kwargs: Any) -> dict:
        params = {"place_id": place_id}

        params.update(kwargs)

        return await self.request("GET", "yelp_place", params=params)

    async def get_yelp_reviews(self, place_id: str, amount: float | None = None, **kwargs: Any) -> dict:
        if amount is None:
            amount = float("inf")

        params: dict[str, Any] = {"place_id": place_id}

        # Set defaults
        params.setdefault("num", 49)
        params.setdefault("start", 0)

        params.update(kwargs)

        # Get first page
        data = await self.request("GET", "yelp_reviews", params=params)
        last_response = data

        # Fetch additional pages if needed
        while len(data.get("reviews", [])) < amount:
            serpapi_pagination = last_response.get("serpapi_pagination") or {}
            next_url = serpapi_pagination.get("next")
            if not next_url:
                break

            # Parse start parameter from next_url
            parsed_url = urlparse(next_url)
            query_params = parse_qs(parsed_url.query)
            if "start" in query_params:
                params["start"] = query_params["start"][0]

            last_response = await self.request("GET", "yelp_reviews", params=params)

            if "reviews" in last_response:
                data["reviews"].extend(last_response["reviews"])

        return data

    async def get_google_maps_place(self, place_id: str, **kwargs: Any) -> dict:
        params = {"place_id": place_id, "type": "place", "hl": "en"}
        params.update(kwargs)

        return await self.request("GET", "google_maps", params=params)
