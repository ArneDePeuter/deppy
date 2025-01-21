from typing import Any, Optional


class IgnoreResult:
    """
    A class to represent an ignored result with optional metadata.

    Attributes
    ----------
    reason : Optional[Any]
        The reason why the result is being ignored.
    data : Optional[Any]
        Additional data or context about the ignored result.
    """

    def __init__(
        self, reason: Optional[Any] = None, data: Optional[Any] = None
    ) -> None:
        """
        Constructs an IgnoreResult instance.

        Parameters
        ----------
        reason : Optional[Any], optional
            The reason for ignoring the result (default is None).
        data : Optional[Any], optional
            Additional data or context about the ignored result (default is None).
        """
        self.reason = reason
        self.data = data

    def __str__(self):
        """
        Returns a string representation of the IgnoreResult.

        Returns
        -------
        str
            A string in the format "IgnoreResult(reason, data)".
        """
        return f"IgnoreResult({self.reason}, {self.data})"

    def __repr__(self):
        """
        Returns the official string representation of the IgnoreResult.

        Returns
        -------
        str
            The same as the string representation.
        """
        return str(self)
