FROM golang:1.20-bullseye AS build
WORKDIR /build
COPY . .
RUN make build

FROM scratch
COPY --from=build build/gox gox
CMD [ "gox" ]