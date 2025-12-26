NAME=kafetrain
BUILD_DIR ?= dist

GO ?= go

NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m

# Run only unit tests (fast, no Docker required)
test-unit:
	@echo "$(OK_COLOR)==> Running unit tests... $(NO_COLOR)"
	@$(GO) test -race -v ./resilience

# Run only integration tests (slower, requires Docker)
test-integration:
	@echo "$(OK_COLOR)==> Running integration tests... $(NO_COLOR)"
	@$(GO) test -race -v -tags=integration ./tests/integration

# Run all tests (unit + integration)
test: test-unit test-integration
	@echo "$(OK_COLOR)==> All tests passed! $(NO_COLOR)"

# Alias for backwards compatibility
test-all: test

up:
	docker-compose -f ./example/docker-compose.yml up -d

down:
	docker-compose -f ./example/docker-compose.yml down -v

build-example:
	@echo "$(OK_COLOR)==> Building... $(NO_COLOR)"
	@CGO_ENABLED=0 $(GO) build -o $(BUILD_DIR)/$(BINARY) $(GO_FLAGS) $(BINARY_SRC)