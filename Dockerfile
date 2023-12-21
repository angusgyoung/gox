FROM golang:1.21-alpine AS build
RUN apk add --no-progress --no-cache gcc musl-dev
WORKDIR /build
COPY . .
RUN go mod download
RUN go build -tags musl -ldflags '-extldflags "-static"' \
    -o /build/gox ./main.go

FROM scratch
WORKDIR /app
COPY --from=build /build/gox .
ENTRYPOINT [ "/app/gox" ]