from typing import Any, Awaitable, Callable, Union, Iterable, TypeVar, ParamSpec
import httpx
from httpx import URL, USE_CLIENT_DEFAULT
from httpx._client import UseClientDefault
from httpx._types import (
    RequestContent,
    RequestData,
    RequestFiles,
    QueryParamTypes,
    HeaderTypes,
    CookieTypes,
    AuthTypes,
    TimeoutTypes,
    RequestExtensions,
)
from functools import wraps
from deppy import IgnoreResult

P = ParamSpec("P")
T = TypeVar("T")


class AsyncClient(httpx.AsyncClient):
    """
    A subclass of `httpx.AsyncClient` with additional features for request handling.
    """

    async def request(
        self,
        method: str,
        url: URL | str,
        *,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        files: RequestFiles | None = None,
        json: Any | None = None,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        cookies: CookieTypes | None = None,
        auth: AuthTypes | UseClientDefault | None = USE_CLIENT_DEFAULT,
        follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
        timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        extensions: RequestExtensions | None = None,
    ) -> Any:
        """
        httpx.request method with automatic JSON parsing.

        Raises
        ------
        httpx.HTTPStatusError
            If the response status code is not successful.
        """
        response = await super().request(
            method,
            url,
            content=content,
            data=data,
            files=files,
            json=json,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def ignore_on_status_codes(
        function: Callable[P, Awaitable[httpx.Response]], status_codes: Iterable[int]
    ) -> Callable[P, Union[IgnoreResult, Any]]:
        """
        Wraps a coroutine function to handle specific HTTP status codes.

        Parameters
        ----------
        function : Callable[P, Awaitable[httpx.Response]]
            The coroutine function to wrap.
        status_codes : Iterable[int]
            A list of HTTP status codes to ignore.

        Returns
        -------
        Callable[P, Union[IgnoreResult, Any]]
            The wrapped function, returning `IgnoreResult` for ignored status codes.
        """

        @wraps(function)
        async def wrapper(
            *args: P.args, **kwargs: P.kwargs
        ) -> Union[IgnoreResult, Any]:
            try:
                result = await function(*args, **kwargs)
                return result
            except httpx.HTTPStatusError as e:
                if e.response.status_code in status_codes:
                    return IgnoreResult()
                raise

        return wrapper
