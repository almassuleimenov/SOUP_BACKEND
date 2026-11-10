# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/ /app/
USER app
WORKDIR /work
EXPOSE 8080
ENTRYPOINT []
CMD ["/app/server"]
