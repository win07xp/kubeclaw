# The load harness binary as an in-cluster image: the orchestrator runs it as
# a Job in `loadgen` mode so request traffic reaches the gateway from inside
# the cluster. Never shipped; built by `make load-images` and imported into the
# load cluster. Its own Dockerfile keeps the production one production-only.
FROM docker.io/golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY api/ api/
COPY test/load/ test/load/

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -tags loadtest -a -o /out/app ./test/load

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /out/app .
USER 65532:65532

ENTRYPOINT ["/app"]
