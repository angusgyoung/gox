build:
	go build -o gox -v main.go																								

.PHONY: test
test:
	go test -v ./...
run:
	go run main.go
