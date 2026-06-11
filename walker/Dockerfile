# walker/Dockerfile
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /walker ./cmd/walker

FROM gcr.io/distroless/static-debian12
COPY --from=build /walker /walker
ENTRYPOINT ["/walker"]
