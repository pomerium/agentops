# The sandbox image: agent-sandbox's python-runtime-sandbox (the in-pod server
# the SDK talks to) plus pillow. smolagents ships its final_answer tool into
# the sandbox as source that imports PIL and numpy at module level; numpy is
# already in the base image. The other four frameworks need nothing extra.
FROM us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/python-runtime-sandbox:latest-main
USER root
RUN pip install --no-cache-dir pillow
USER 1000
