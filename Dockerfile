# syntax=docker/dockerfile:1.6

# --- build stage ---
FROM golang:1.25-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/notification-svc ./

# --- runtime stage ---
FROM alpine:3.20
WORKDIR /app

RUN apk add --no-cache ca-certificates wget

COPY --from=build /out/notification-svc /app/notification-svc

EXPOSE 8080
ENTRYPOINT ["/app/notification-svc"]
