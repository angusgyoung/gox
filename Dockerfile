FROM golang:1.20-alpine AS build
RUN apk add --no-progress --no-cache gcc musl-dev
WORKDIR /build
COPY . .
RUN go mod download
RUN go build -tags musl -ldflags '-extldflags "-static"' \
    -o /build/gox ./cmd/gox/main.go

FROM scratch
WORKDIR /app
COPY --from=build /build/gox .
CMD [ "/app/gox" ]