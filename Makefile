NAME=kafetrain
BUILD_DIR ?= dist

GO ?= go

NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m

test:
	@$(GO) test -race