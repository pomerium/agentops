# syntax=docker/dockerfile:1

# ---- build stage --------------------------------------------------------
# modernc.org/sqlite is pure Go, so we build with CGO disabled and can ship
# on a minimal static base image.
#
# Pin the build stage to the builder's native platform ($BUILDPLATFORM) and
# cross-compile to the requested target via GOOS/GOARCH. This keeps multi-arch
# (`docker buildx --platform linux/amd64,linux/arm64`) builds fast — the Go
# toolchain runs natively instead of under QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build

WORKDIR /src

# Prime the module cache first so dependency downloads are cached
# independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# TARGETOS/TARGETARCH are supplied automatically by BuildKit per requested
# --platform (and default to the build host when single-platform).
ARG TARGETOS TARGETARCH

# Copy the rest of the source and build the binary.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/agentops ./cmd/agentops

# Create the /data mount point owned by the non-root user (uid/gid 65532)
# here, because the distroless final stage has no shell to mkdir with.
RUN install -d -o 65532 -g 65532 -m 0755 /out/data

# ---- final stage --------------------------------------------------------
# Distroless static: no shell, runs as a non-root user by default (65532).
FROM gcr.io/distroless/static-debian12:nonroot

# The SQLite database lives on a PVC mounted at /data; ship the directory so
# it exists (and is writable) even before the volume is attached.
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build /out/agentops /usr/local/bin/agentops

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/agentops"]
