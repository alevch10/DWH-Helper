import httpx

from typing import Optional, Dict, Any

from ..config.settings import settings


class MetrikaClient:
    def __init__(self, token: str, base_url: str = settings.yandexmetrica.base_url):
        self.token = token
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            base_url=base_url, headers={"Authorization": f"OAuth {token}"}, timeout=30.0
        )

    async def close(self):
        await self.client.aclose()

    async def _get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        resp = await self.client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        resp = await self.client.post(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_counters(self) -> Dict[str, Any]:
        return await self._get("counters")

    async def create_logrequest(
        self, counter_id: int, date1: str, date2: str, fields: str, source: str = "hits"
    ) -> Dict[str, Any]:
        params = {"date1": date1, "date2": date2, "fields": fields, "source": source}
        return await self._post(f"counter/{counter_id}/logrequests", params=params)

    async def get_logrequests(self, counter_id: int) -> Dict[str, Any]:
        return await self._get(f"counter/{counter_id}/logrequests")

    async def get_logrequest_info(
        self, counter_id: int, request_id: int
    ) -> Dict[str, Any]:
        return await self._get(f"counter/{counter_id}/logrequest/{request_id}")

    async def clean_logrequest(
        self, counter_id: int, request_id: int
    ) -> Dict[str, Any]:
        return await self._post(f"counter/{counter_id}/logrequest/{request_id}/clean")

    async def cancel_logrequest(
        self, counter_id: int, request_id: int
    ) -> Dict[str, Any]:
        return await self._post(f"counter/{counter_id}/logrequest/{request_id}/cancel")

    async def evaluate_logrequest(
        self, counter_id: int, date1: str, date2: str, fields: str, source: str = "hits"
    ) -> Dict[str, Any]:
        params = {"date1": date1, "date2": date2, "fields": fields, "source": source}
        return await self._get(
            f"counter/{counter_id}/logrequests/evaluate", params=params
        )

    async def download_part(
        self, counter_id: int, request_id: int, part_number: int
    ) -> bytes:
        path = (
            f"counter/{counter_id}/logrequest/{request_id}/part/{part_number}/download"
        )
        resp = await self.client.get(path)
        resp.raise_for_status()
        return resp.content

    async def download_part_stream(
        self, counter_id: int, request_id: int, part_number: int
    ):
        """Возвращает асинхронный итератор байтов для скачивания части."""
        path = (
            f"counter/{counter_id}/logrequest/{request_id}/part/{part_number}/download"
        )
        async with self.client.stream("GET", path) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                yield chunk
