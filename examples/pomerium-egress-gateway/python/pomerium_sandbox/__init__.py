"""The agent-sandbox Python SDK, pointed at a sandbox-router behind Pomerium.

The SDK's Direct connection mode takes a URL and headers to send with every
request. Pomerium accepts a Kubernetes ServiceAccount token as a Bearer, so the
only thing this module adds is where that token comes from:

- in a pod, the projected token at ``POMERIUM_TOKEN_FILE`` (kubelet rotates it);
- on a laptop, ``POMERIUM_TOKEN``: a token minted with ``kubectl create token``
  for the same ServiceAccount, or a Pomerium service account token.
"""

from __future__ import annotations

import os
from pathlib import Path

from k8s_agent_sandbox import AsyncSandboxClient, SandboxClient
from k8s_agent_sandbox.models import SandboxDirectConnectionConfig

__all__ = [
    "SANDBOX_NAMESPACE",
    "SANDBOX_WARMPOOL",
    "async_sandbox_client",
    "connection_config",
    "pomerium_bearer",
    "sandbox_client",
]

DEFAULT_TOKEN_FILE = "/var/run/secrets/pomerium/token"

SANDBOX_NAMESPACE = os.environ.get("SANDBOX_NAMESPACE", "sandbox-egress")
SANDBOX_WARMPOOL = os.environ.get("SANDBOX_WARMPOOL", "python-egress")


def pomerium_bearer() -> str:
    """The Bearer token for Pomerium's sandbox route.

    ``POMERIUM_TOKEN`` wins. Otherwise the token is read from
    ``POMERIUM_TOKEN_FILE`` (default ``/var/run/secrets/pomerium/token``, where
    the example's Job projects it). Read it when you build a client, not once
    at import: a projected token is replaced before it expires.
    """
    token = os.environ.get("POMERIUM_TOKEN")
    if token:
        return token.strip()
    path = Path(os.environ.get("POMERIUM_TOKEN_FILE", DEFAULT_TOKEN_FILE))
    if path.is_file():
        return path.read_text().strip()
    raise RuntimeError(
        "no Pomerium token: set POMERIUM_TOKEN, or mount a projected "
        f"ServiceAccount token at {path} (POMERIUM_TOKEN_FILE)"
    )


def connection_config(
    api_url: str | None = None, *, ca_cert: str | None = None, server_port: int = 8888
) -> SandboxDirectConnectionConfig:
    """Direct mode through Pomerium: the route URL, the Bearer, and the CA.

    ``SANDBOX_API_URL`` is the Pomerium route in front of the sandbox-router.
    ``POMERIUM_CA_CERT`` is only needed when Pomerium presents a private CA.
    """
    api_url = api_url or os.environ.get("SANDBOX_API_URL")
    if not api_url:
        raise RuntimeError("SANDBOX_API_URL is not set")
    ca_cert = ca_cert or os.environ.get("POMERIUM_CA_CERT") or None
    return SandboxDirectConnectionConfig(
        api_url=api_url,
        server_port=server_port,
        extra_headers={"Authorization": f"Bearer {pomerium_bearer()}"},
        ca_cert=ca_cert,
    )


def sandbox_client(api_url: str | None = None, **kwargs) -> SandboxClient:
    """A synchronous SDK client whose sandbox traffic goes through Pomerium."""
    return SandboxClient(connection_config=connection_config(api_url, **kwargs))


def async_sandbox_client(api_url: str | None = None, **kwargs) -> AsyncSandboxClient:
    """The asynchronous twin of :func:`sandbox_client`."""
    return AsyncSandboxClient(connection_config=connection_config(api_url, **kwargs))
