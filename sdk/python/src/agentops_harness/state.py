from __future__ import annotations

from .gen.harnessapi.v1.harnessapi_pb import SessionState

__all__ = ["is_live"]

_LIVE = frozenset(
    {
        SessionState.PENDING,
        SessionState.LAUNCHING,
        SessionState.AWAITING_APPROVAL,
        SessionState.RUNNING,
        SessionState.SUSPENDED,
    }
)


def is_live(state: SessionState) -> bool:
    return state in _LIVE
