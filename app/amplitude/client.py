import httpx
import base64
import gzip
import zipfile

from io import BytesIO
from typing import Literal, AsyncGenerator
from datetime import datetime, timedelta

from app.config.settings import settings
from app.config.logger import get_logger

logger = get_logger(__name__)


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
        async with httpx.AsyncClient(timeout=2000) as client:
            response = await client.get(self.BASE_URL, headers=headers, params=params)
            response.raise_for_status()
            return response.content

    async def export_day(self, date_str: str) -> bytes:
        """Export a single day (00:00 – 23:59)."""
        return await self.export(f"{date_str}T00", f"{date_str}T23")

    async def iter_lines(
        self, start: datetime, end: datetime
    ) -> AsyncGenerator[str, None]:
        """
        Yield one JSON line per event for the inclusive date range [start, end].
        Each line is a string containing a complete JSON object.
        """
        current = start
        while current <= end:
            day_str = current.strftime("%Y%m%d")
            try:
                zip_bytes = await self.export_day(day_str)
            except Exception as e:
                logger.error(f"Failed to export {day_str}: {e}")
                raise

            with zipfile.ZipFile(BytesIO(zip_bytes), "r") as daily_zip:
                for gz_name in daily_zip.namelist():
                    if not gz_name.endswith(".gz"):
                        continue
                    with daily_zip.open(gz_name) as gz_file:
                        # Decompress .gz → JSON lines
                        json_bytes = gzip.decompress(gz_file.read())
                        text = json_bytes.decode("utf-8")
                        for line in text.splitlines():
                            if line.strip():
                                yield line
            current += timedelta(days=1)
