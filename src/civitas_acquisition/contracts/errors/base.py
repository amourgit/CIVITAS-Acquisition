"""
Base exception for the entire Acquisition Platform.
All errors in this platform inherit from AcquisitionError.
This guarantees callers can catch the full domain with a single except clause.
"""


class AcquisitionError(Exception):
    """Root exception for the CIVITAS Acquisition Platform."""

    def __init__(self, message: str, context: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict = context or {}

    def __str__(self) -> str:
        if self.context:
            ctx = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
            return f"{self.message} [{ctx}]"
        return self.message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.message!r})"
