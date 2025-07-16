CGO_CXXFLAGS ?= $(shell go env CGO_CXXFLAGS 2>/dev/null)
ifeq ($(CGO_CXXFLAGS),)
	CGO_CXXFLAGS += -g
	CGO_CXXFLAGS += -O2
endif

GOTEST = CGO_CXXFLAGS="$(CGO_CXXFLAGS)" go test -trimpath

test-short:
	$(GOTEST) -short -coverprofile=coverage-test.out ./...

test-all:
	$(GOTEST) -coverprofile=coverage-test-all.out ./...

test-all-race:
	$(GOTEST) -coverprofile=coverage-test-all.out -race ./...
