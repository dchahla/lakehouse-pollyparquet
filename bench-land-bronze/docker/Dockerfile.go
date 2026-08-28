# 1.25 to match the go directive in go.mod (iceberg-go requires go 1.25).
FROM golang:1.25 AS build

WORKDIR /build
COPY go/ .

# Static build. iceberg-go pulls arrow-go; both link cleanly with cgo off.
RUN go mod tidy && CGO_ENABLED=0 go build -o land-bronze ./cmd/land-bronze

FROM gcr.io/distroless/static-debian12

COPY --from=build /build/land-bronze /app/land-bronze

ENTRYPOINT ["/app/land-bronze"]
