build:
	go build -o gox -v ./cmd/gox/main.go																								

.PHONY: test
test:
	go test -v ./...
run:
	go run cmd/gox/main.go
