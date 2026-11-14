.PHONY: build

build:
	go build -o gox -v main.go																								

test: unit integration

unit:
	go test -v ./...

integration:
	go test -v ./... --tags=integration

run:
	go run main.go
