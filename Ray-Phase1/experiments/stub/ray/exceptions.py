"""The errors the double of Ray raises, with the same names as the real ones."""

from __future__ import annotations


class RayError(Exception):
    """Base of every emulated Ray error."""


class GetTimeoutError(RayError):
    """Raised when a call does not answer within its deadline."""


class RayActorError(RayError):
    """Raised when the actor is not alive."""
