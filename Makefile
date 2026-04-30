.PHONY: fmt test vet check help

help:
	@echo "Targets: fmt test vet check"

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

vet:
	go vet ./...

check: fmt test vet
	git diff --check
