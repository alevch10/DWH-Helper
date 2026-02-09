import httpx
import asyncio
from typing import Any, Optional
from app.config.settings import settings


class AppMetricaClient:
    def __init__(self):
        self.base_url = settings.appmetrica.base_url
        self.api_key = settings.appmetrica.api_key
        self.application_id = settings.appmetrica.application_id
        self.poll_interval = settings.appmetrica.poll_interval_seconds
        self.poll_timeout = settings.appmetrica.poll_timeout_seconds

    async def fetch_metrics(self, counter_id: str, limit: int = 10) -> Any:
        """Fetch metrics from AppMetrica. If `appmetrica_base_url` not set, returns a simulated response."""
        if not self.base_url:
            return {"counter_id": counter_id, "metrics": [f"metric_{i+1}" for i in range(limit)]}

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        url = f"{self.base_url.rstrip('/')}/counters/{counter_id}/metrics"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers, params={"limit": limit})
            resp.raise_for_status()
            return resp.json()

    async def fetch_export(
        self,
        application_id: Optional[str] = None,
        skip_unavailable_shards: bool = False,
        date_since: Optional[str] = None,
        date_until: Optional[str] = None,
        date_dimension: str = "default",
        use_utf8_bom: bool = True,
        fields: Optional[str] = None,
        poll_timeout: Optional[int] = None,
        poll_interval: Optional[int] = None,
    ) -> dict:
        """Start an export request and poll until ready."""
        # Use config defaults if not provided
        poll_timeout = poll_timeout or self.poll_timeout
        poll_interval = poll_interval or self.poll_interval
        if not self.base_url:
            # return simulated export data
            sample = {
                "data": [
                    {"event_name": "Banner Shown", "event_datetime": "2026-02-09 16:20:55"},
                    {"event_name": "Create Appointment", "event_datetime": "2026-02-09 16:21:03"},
                ]
            }
            return {"status": "ready", "result": sample}

        app_id = application_id or self.application_id
        if not app_id:
            raise RuntimeError("application_id is required")

        params = {
            "application_id": app_id,
            "skip_unavailable_shards": str(skip_unavailable_shards).lower(),
            "date_since": date_since,
            "date_until": date_until,
            "date_dimension": date_dimension,
            "use_utf8_bom": str(use_utf8_bom).lower(),
            "fields": fields,
        }
        # remove None values
        params = {k: v for k, v in params.items() if v is not None}

        url = f"{self.base_url.rstrip('/')}/logs/v1/export/events.json"
        headers = {"Authorization": f"OAuth {self.api_key}"} if self.api_key else {}

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(url, headers=headers, params=params)
            # if queued
            if resp.status_code == 202:
                # poll until 200 or timeout
                import time

                start = time.time()
                while time.time() - start < poll_timeout:
                    await asyncio.sleep(poll_interval)
                    r2 = await client.get(url, headers=headers, params=params)
                    if r2.status_code == 200:
                        return {"status": "ready", "result": r2.json()}
                    # continue polling
                return {"status": "pending", "detail": "Timeout while waiting for export"}
            elif resp.status_code == 200:
                return {"status": "ready", "result": resp.json()}
            else:
                resp.raise_for_status()


client = AppMetricaClient()
