CGO_CXXFLAGS ?= $(shell go env CGO_CXXFLAGS 2>/dev/null)
ifeq ($(CGO_CXXFLAGS),)
	CGO_CXXFLAGS += -g
	CGO_CXXFLAGS += -O2
endif

GOTEST = CGO_CXXFLAGS="$(CGO_CXXFLAGS)" GOEXPERIMENT=synctest go test -trimpath

lintci-deps:
	@./tools/golangci_lint.sh --install-deps
lintci:
	@CGO_CXXFLAGS="$(CGO_CXXFLAGS)" ./tools/golangci_lint.sh

lint-mod-tidy:
	@./tools/mod_tidy_check.sh

lint-deps: lintci-deps
lint: lintci lint-mod-tidy

test-short:
	$(GOTEST) -short ./...

test-all:
	$(GOTEST) -coverprofile=coverage-test-all.out ./...

test-all-race:
	$(GOTEST) -race ./...
