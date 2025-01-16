from typing import Any, Optional


class IgnoreResult:
    def __init__(
        self, reason: Optional[Any] = None, data: Optional[Any] = None
    ) -> None:
        self.reason = reason
        self.data = data

    def __str__(self):
        return f"IgnoreResult({self.reason}, {self.data})"

    def __repr__(self):
        return str(self)
