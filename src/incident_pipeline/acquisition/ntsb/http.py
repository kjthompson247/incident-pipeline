from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence

import httpx

from incident_pipeline.acquisition.ntsb.config import AppConfig

QueryParamScalar = str | int | float | bool | None
QueryParamValue = QueryParamScalar | Sequence[QueryParamScalar]
QueryParams = Mapping[str, QueryParamValue]


class HttpClient:
    def __init__(
        self,
        client: httpx.Client,
        *,
        rate_limit_per_second: float,
        max_retries: int,
        backoff_seconds: float,
        sleeper: Callable[[float], object] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._rate_limit_per_second = rate_limit_per_second
        self._max_retries = max_retries
        self._backoff_seconds = backoff_seconds
        self._sleeper = sleeper
        self._monotonic = monotonic
        self._last_request_at: float | None = None

    def close(self) -> None:
        self._client.close()

    def _respect_rate_limit(self) -> None:
        if self._rate_limit_per_second <= 0:
            return
        minimum_interval = 1.0 / self._rate_limit_per_second
        now = self._monotonic()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            if elapsed < minimum_interval:
                self._sleeper(minimum_interval - elapsed)
        self._last_request_at = self._monotonic()

    def request(
        self,
        method: str,
        url: str,
        *,
        params: QueryParams | None = None,
        data: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            self._respect_rate_limit()
            try:
                response = self._client.request(method, url, params=params, data=data)
                if response.status_code == 429 or response.status_code >= 500:
                    response.raise_for_status()
                return response
            except httpx.HTTPStatusError as error:
                last_error = error
                if attempt >= self._max_retries:
                    raise
                self._sleeper(self._backoff_seconds * (attempt + 1))
            except httpx.HTTPError as error:
                last_error = error
                if attempt >= self._max_retries:
                    raise
                self._sleeper(self._backoff_seconds * (attempt + 1))
        assert last_error is not None
        raise last_error
    def request_json(
        self,
        method: str,
        url: str,
        *,
        json_body: object | None = None,
        headers: Mapping[str, str] | None = None,
        cookies: dict[str, str] | None = None,
    ) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            self._respect_rate_limit()
            try:
                response = self._client.request(
                    method,
                    url,
                    json=json_body,
                    headers=headers,
                    cookies=cookies,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    response.raise_for_status()
                return response
            except httpx.HTTPStatusError as error:
                last_error = error
                if attempt >= self._max_retries:
                    raise
                self._sleeper(self._backoff_seconds * (attempt + 1))
            except httpx.HTTPError as error:
                last_error = error
                if attempt >= self._max_retries:
                    raise
                self._sleeper(self._backoff_seconds * (attempt + 1))
        assert last_error is not None
        raise last_error

    def get_json(
        self,
        url: str,
        *,
        params: QueryParams | None = None,
    ) -> object:
        response = self.request("GET", url, params=params)
        return response.json()

    def get_bytes(
        self,
        url: str,
        *,
        params: QueryParams | None = None,
    ) -> bytes:
        response = self.request("GET", url, params=params)
        return response.content

    def get_text(
        self,
        url: str,
        *,
        params: QueryParams | None = None,
    ) -> str:
        response = self.request("GET", url, params=params)
        return response.text


def build_http_client(
    config: AppConfig,
    *,
    transport: httpx.BaseTransport | None = None,
) -> HttpClient:
    client = httpx.Client(
        headers={"User-Agent": config.http_user_agent},
        timeout=30.0,
        follow_redirects=True,
        transport=transport,
    )
    return HttpClient(
        client,
        rate_limit_per_second=config.http_rate_limit_per_second,
        max_retries=config.http_max_retries,
        backoff_seconds=config.http_backoff_seconds,
    )
