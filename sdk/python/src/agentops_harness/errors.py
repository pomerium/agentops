from __future__ import annotations

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf import enum_is_unknown

from .gen.harnessapi.v1.harnessapi_pb import ErrorInfo, Sentinel

__all__ = ["Code", "ConnectError", "sentinel_of"]

_ERROR_INFO_TYPE = ErrorInfo.desc().type_name


def sentinel_of(exc: BaseException) -> Sentinel | None:
    if not isinstance(exc, ConnectError):
        return None
    for item in exc.details:
        if item.type_name != _ERROR_INFO_TYPE:
            continue
        info = item.value(ErrorInfo)
        if info is None or info.sentinel == Sentinel.UNSPECIFIED or enum_is_unknown(info.sentinel):
            return None
        return info.sentinel
    return None
