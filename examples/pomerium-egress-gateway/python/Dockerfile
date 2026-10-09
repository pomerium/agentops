# The agent as it runs in the cluster: the SDK and the example's tests, no
# framework. The Job in ../agent/job.yaml runs `pytest tests/test_sdk.py` in
# it with a projected ServiceAccount token as its only credential.
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md* ./
COPY pomerium_sandbox ./pomerium_sandbox
RUN uv sync --frozen --no-dev --extra test
COPY tests ./tests
ENV PATH=/app/.venv/bin:$PATH
CMD ["pytest", "-q", "-rs", "tests/test_sdk.py"]
