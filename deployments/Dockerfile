# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder

ENV GOTOOLCHAIN=auto
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/control-plane ./cmd/control-plane && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/data-plane ./cmd/data-plane && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/demo-app ./cmd/demo-app

FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /out/control-plane /usr/local/bin/control-plane
COPY --from=builder /out/data-plane /usr/local/bin/data-plane
COPY --from=builder /out/demo-app /usr/local/bin/demo-app
ENTRYPOINT ["/usr/local/bin/control-plane"]

