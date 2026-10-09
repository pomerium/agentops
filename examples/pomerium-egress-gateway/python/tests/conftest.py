import json

import pytest

from pomerium_sandbox import SANDBOX_NAMESPACE, SANDBOX_WARMPOOL, sandbox_client

# What the sandboxed code runs: a GET through the sidecar's loopback listener.
# The sidecar adds the pod's token and forwards to the Pomerium route, and
# pomerium/verify answers with the identity Pomerium attached.
PROBE = (
    "import json, urllib.request; "
    "print(urllib.request.urlopen('http://127.0.0.1:9000/json', timeout=15).read().decode())"
)
PROBE_COMMAND = f"python3 -c \"{PROBE}\""

# Pomerium prefixes the subject with the identity provider name.
EXPECTED_SUBJECT = f"cluster/system:serviceaccount:{SANDBOX_NAMESPACE}:sandbox-egress"


def assert_reached_verify_as_sandbox(output: str) -> dict:
    """The JSON that verify returned names the sandbox's ServiceAccount."""
    body = json.loads(output.strip().splitlines()[-1])
    identity = body["identity"]
    assert identity["sub"] == EXPECTED_SUBJECT, body
    assert "error" not in body, body
    return body


@pytest.fixture(scope="session")
def client():
    return sandbox_client()


@pytest.fixture
def sandbox(client):
    sb = client.create_sandbox(warmpool=SANDBOX_WARMPOOL, namespace=SANDBOX_NAMESPACE)
    try:
        yield sb
    finally:
        sb.terminate()
