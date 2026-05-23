FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server && \
    CGO_ENABLED=0 go build -o /out/kvctl ./cmd/kvctl

FROM alpine:3.20
COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/kvctl /usr/local/bin/kvctl
ENTRYPOINT ["/usr/local/bin/server"]
