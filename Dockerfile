# Pharos as one static binary on distroless (no shell, no package manager; tzdata for usage.timezone).
#
#   docker build -t pharos .
#   docker run -p 8090:8090 -v ./pharos.yaml:/etc/pharos/pharos.yaml:ro pharos
#
# It runs as root so Docker label discovery can open the socket (mount /var/run/docker.sock
# read-only); with static backends only, run it as any user (--user 65532).
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /pharos ./cmd/pharos

FROM gcr.io/distroless/static-debian12
COPY --from=build /pharos /pharos
EXPOSE 8090
ENTRYPOINT ["/pharos"]
CMD ["serve", "-config", "/etc/pharos/pharos.yaml"]
