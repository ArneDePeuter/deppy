import pytest
from unittest.mock import AsyncMock, patch
import httpx
from deppy.helpers.asyncclient import AsyncClient, IgnoreResult


@pytest.fixture
def async_client():
    return AsyncClient()


async def test_request_success(async_client):
    url = "https://example.com/api"
    mock_response = httpx.Response(
        status_code=200,
        content=b'{"key": "value"}',
        request=httpx.Request(method="GET", url=url),
    )

    with patch("httpx.AsyncClient.send", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response

        response = await async_client.request("GET", url)

        assert response == {"key": "value"}


async def test_request_http_error(async_client):
    url = "https://example.com/api"
    mock_response = httpx.Response(
        status_code=400, content=b"Error", request=httpx.Request(method="GET", url=url)
    )

    with patch("httpx.AsyncClient.send", new_callable=AsyncMock) as mock_request:
        mock_request.return_value = mock_response

        with pytest.raises(httpx.HTTPStatusError):
            await async_client.request("GET", url)


async def test_ignore_on_status_codes():
    async def mock_function():
        raise httpx.HTTPStatusError(
            "Error",
            request=httpx.Request(method="GET", url="https://example.com/api"),
            response=httpx.Response(status_code=404, content=b"Not Found"),
        )

    ignored_function = AsyncClient.ignore_on_status_codes(
        mock_function, status_codes=[404]
    )

    result = await ignored_function()
    assert isinstance(result, IgnoreResult)

    async def mock_function_no_error():
        return "Success"

    ignored_function_no_error = AsyncClient.ignore_on_status_codes(
        mock_function_no_error, status_codes=[404]
    )
    result = await ignored_function_no_error()
    assert result == "Success"


async def test_ignore_on_status_codes_non_ignored_error():
    async def mock_function():
        raise httpx.HTTPStatusError(
            "Error",
            request=httpx.Request(method="GET", url="https://example.com/api"),
            response=httpx.Response(status_code=500, content=b"Internal Server Error"),
        )

    ignored_function = AsyncClient.ignore_on_status_codes(
        mock_function, status_codes=[404]
    )

    with pytest.raises(httpx.HTTPStatusError):
        await ignored_function()
