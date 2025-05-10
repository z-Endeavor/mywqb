import logging
import time
from collections.abc import Callable
from requests import Response, Session

__all__ = ['AutoAuthSession']


class AutoAuthSession(Session):

    def __init__(
        self,
        method: str,
        url: str,
        *,
        auth_expected: Callable[[Response], bool] = lambda _: True,
        auth_max_tries: int = 3,
        auth_delay_unexpected: float = 2.0,
        expected: Callable[[Response], bool] = lambda _: True,
        max_tries: int = 3,
        delay_unexpected: float = 2.0,
        logger: logging.Logger = logging.root,
        **kwargs,
    ) -> None:
        super().__init__()
        self.method = method
        self.url = url
        self.kwargs = kwargs
        self.auth_expected = auth_expected
        self.auth_max_tries = auth_max_tries
        self.auth_delay_unexpected = auth_delay_unexpected
        self.expected = expected
        self.max_tries = max(1, max_tries)
        self.delay_unexpected = max(0.0, delay_unexpected)
        self.logger = logger

    def __repr__(
        self,
    ) -> str:
        """
        Returns a string representation of the `AutoAuthSession` object.

        Returns
        -------
        str
            A string representation of the `AutoAuthSession` object.
        """
        return f"<AutoAuthSession []>"

    def auth_request(
        self,
        method: str | None = None,
        url: str | None = None,
        *args,
        expected: Callable[[Response], bool] | None = None,
        max_tries: int | None = None,
        delay_unexpected: float | None = None,
        log: str | None = None,
        **kwargs,
    ) -> Response:
        if method is None:
            method = self.method
        if url is None:
            url = self.url
        kwargs = self.kwargs | kwargs
        if expected is None:
            expected = self.auth_expected
        if max_tries is None:
            max_tries = self.auth_max_tries
        if delay_unexpected is None:
            delay_unexpected = self.auth_delay_unexpected
        max_tries = max(1, max_tries)
        delay_unexpected = max(0.0, delay_unexpected)
        for tries in range(1, 1 + max_tries):
            resp = super().request(method, url, *args, **kwargs)
            if expected(resp):
                break
            time.sleep(delay_unexpected)
        else:
            self.logger.warning(
                '\n'.join(
                    (
                        f"{self}.auth_request(...) [max {tries} tries ran out]",
                        f"super().request(method, url, *args, **kwargs):",
                        f"    method: {method}",
                        f"    url: {url}",
                        f"    args: {args}",
                        f"    kwargs: {kwargs}",
                        f"{resp}:",
                        f"    status_code: {resp.status_code}",
                        f"    reason: {resp.reason}",
                        f"    url: {resp.url}",
                        f"    elapsed: {resp.elapsed}",
                        f"    headers: {resp.headers}",
                        f"    text: {resp.text}",
                    )
                )
            )
        if log is not None:
            self.logger.info(f"{self}.auth_request(...) [{tries} tries]: {log}")
        return resp

    def request(
        self,
        method: str,
        url: str,
        *args,
        expected: Callable[[Response], bool] | None = None,
        max_tries: int | None = None,
        delay_unexpected: float | None = None,
        log: str | None = None,
        **kwargs,
    ) -> Response:
        if expected is None:
            expected = self.expected
        if max_tries is None:
            max_tries = self.max_tries
        if delay_unexpected is None:
            delay_unexpected = self.delay_unexpected
        max_tries = max(1, max_tries)
        delay_unexpected = max(0.0, delay_unexpected)
        for tries in range(1, 1 + max_tries):
            resp = super().request(method, url, *args, **kwargs)
            if expected(resp):
                break
            time.sleep(delay_unexpected)
            auth_resp = self.auth_request()
        else:
            self.logger.warning(
                '\n'.join(
                    (
                        f"{self}.request(...) [max {tries} tries ran out]",
                        f"super().request(method, url, *args, **kwargs):",
                        f"    method: {method}",
                        f"    url: {url}",
                        f"    args: {args}",
                        f"    kwargs: {kwargs}",
                        f"{resp}:",
                        f"    status_code: {resp.status_code}",
                        f"    reason: {resp.reason}",
                        f"    url: {resp.url}",
                        f"    elapsed: {resp.elapsed}",
                        f"    headers: {resp.headers}",
                        f"    text: {resp.text}",
                    )
                )
            )
        if log is not None:
            self.logger.info(f"{self}.request(...) [{tries} tries]: {log}")
        return resp