"""The SDK alone: no framework. This is what the in-cluster Job runs."""

from conftest import PROBE_COMMAND, assert_reached_verify_as_sandbox


def test_sandboxed_code_reaches_verify_through_pomerium(sandbox):
    result = sandbox.commands.run(PROBE_COMMAND, timeout=60)
    assert result.exit_code == 0, result.stderr
    body = assert_reached_verify_as_sandbox(result.stdout)
    # The route stripped the pod's own token before forwarding.
    assert "Authorization" not in body["headers"]


def test_direct_egress_is_blocked(sandbox):
    """Without the sidecar the pod reaches nothing: DNS and Pomerium only."""
    result = sandbox.commands.run(
        "python3 -c \"import urllib.request; urllib.request.urlopen('https://example.com', timeout=5)\"",
        timeout=60,
    )
    assert result.exit_code != 0
