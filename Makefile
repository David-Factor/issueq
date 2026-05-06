.PHONY: fmt test vet check smoke-handoff-gates live-smoke-preflight help

help:
	@echo "Targets: fmt test vet check smoke-handoff-gates live-smoke-preflight"

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

vet:
	go vet ./...

check: fmt test vet
	git diff --check

smoke-handoff-gates:
	go test -count=1 ./internal/daemon -run 'TestLocal(HandoffGatesSmoke|WorkStartedFallbackSmoke)'

live-smoke-preflight:
	@if [ -z "$(CONFIG)" ]; then echo "CONFIG=/path/to/issueq.yaml is required"; exit 2; fi
	./scripts/handoff-gates-live-preflight.sh --config "$(CONFIG)" $(if $(DB),--db "$(DB)",) $(if $(ISSUE),--issue "$(ISSUE)",) $(if $(BIN),--bin "$(BIN)",)
