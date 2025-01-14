import httpx
from typing import Dict, Any
import pendulum


class PowerBIAuth(httpx.Auth):
    def __init__(
            self,
            access_token_url: str,
            client_id: str,
            client_secret: str,
            access_token_request_data: Dict[str, Any],
            default_token_expiration: int = 3600
    ) -> None:
        self.access_token = None
        self.access_token_url = access_token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token_request_data = access_token_request_data
        self.default_token_expiration = default_token_expiration
        self.token_expiry: pendulum.DateTime = pendulum.now()

    def is_token_expired(self) -> bool:
        return pendulum.now() >= self.token_expiry

    async def obtain_token(self) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.access_token_url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                    **(self.access_token_request_data or {}),
                },
            )
            response.raise_for_status()
            response_json = response.json()
            self.access_token = response_json["access_token"]
            self.token_expiry = pendulum.now().add(
                seconds=response_json.get("expires_in", self.default_token_expiration)
            )

    async def async_auth_flow(self, request: httpx.Request) -> httpx.Request:
        if self.access_token is None or self.is_token_expired():
            await self.obtain_token()
        request.headers["Authorization"] = f"Bearer {self.access_token}"
        yield request
