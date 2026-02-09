"""Amplitude API client for data export and import."""

import httpx
import base64
from typing import Literal, Optional
from app.config.settings import settings

class AmplitudeClient:
    """
    Amplitude API client supporting both Web and Mobile credentials.
    """
    BASE_URL = "https://amplitude.com/api/2/export"

    def __init__(self, source: Literal["web", "mobile"] = "web"):
        self.source = source
        if source == "web":
            self.secret_key = settings.amplitude.web_secret_key
            self.client_id = settings.amplitude.web_client_id
        else:
            self.secret_key = settings.amplitude.mobile_secret_key
            self.client_id = settings.amplitude.mobile_client_id
        if not self.secret_key or not self.client_id:
            raise ValueError(f"Amplitude credentials for {source} not set in settings")

    def _get_auth_header(self) -> str:
        token = f"{self.client_id}:{self.secret_key}"
        return "Basic " + base64.b64encode(token.encode()).decode()

    async def export(self, start: str, end: str) -> bytes:
        """
        Export data from Amplitude API for the given date range.
        Args:
            start: Start date in format YYYYMMDDTHH (e.g. 20200201T00)
            end: End date in format YYYYMMDDTHH (e.g. 20200207T23)
        Returns:
            Raw zip file bytes
        """
        headers = {"Authorization": self._get_auth_header()}
        params = {"start": start, "end": end}
        async with httpx.AsyncClient(timeout=600) as client:
            response = await client.get(self.BASE_URL, headers=headers, params=params)
            response.raise_for_status()
            return response.content
