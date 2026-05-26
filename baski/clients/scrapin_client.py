from typing import Any

import httpx

from .. import get_env
from ..server.logger import Logger

__all__ = ["ScrapinClient"]


class ScrapinClient:
    BASE_URL = "https://api.scrapin.io"

    def __init__(
        self,
        logger: Logger,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._api_key = str(get_env("SCRAPIN_API_KEY")).strip()
        self._http_client = http_client
        self._logger = logger

    async def request(self, method: str, endpoint: str, **kwargs: Any) -> dict:
        url = f"{self.BASE_URL}{endpoint}"
        params = kwargs.get("params", {})
        params["apikey"] = self._api_key
        kwargs["params"] = params

        log_data = {"scrapin_endpoint": endpoint, "params": {k: v for k, v in params.items() if k != "apikey"}}
        if "json" in kwargs:
            log_data["body"] = kwargs["json"]

        self._logger.info("Scrapin request", labels=log_data)

        response = await self._http_client.request(method=method, url=url, **kwargs)
        if response.status_code == httpx.codes.NOT_FOUND:
            return {}
        response.raise_for_status()
        return response.json()

    async def extract_company_data(self, linkedin_url: str) -> dict:
        data = await self.request("GET", "/v1/enrichment/company", params={"linkedInUrl": linkedin_url})
        return data.get("company")

    async def extract_person_data(self, linkedin_url: str) -> dict:
        data = await self.request(
            "POST",
            "/v1/enrichment/profile",
            json={
                "linkedInUrl": linkedin_url,
                "includes": {
                    "includeCompany": True,
                    "includeSummary": True,
                    "includeFollowersCount": True,
                    "includeSkills": True,
                    "includeLanguages": True,
                    "includeExperience": True,
                    "includeEducation": True,
                    "includeRecommendations": True,
                    "includeCertifications": True,
                },
            },
        )
        return data.get("person")
