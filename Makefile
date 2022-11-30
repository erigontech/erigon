GOBINREL = build/bin
GOBIN = $(CURDIR)/$(GOBINREL)
BUILD_TAGS = nosqlite,noboltdb,disable_libutp
GOBUILD = env GO111MODULE=on go build -trimpath -tags $(BUILD_TAGS)
GOTEST = go test -trimpath -tags $(BUILD_TAGS)
GOTEST_NOFUZZ = go test -trimpath --tags=$(BUILD_TAGS),nofuzz
OS = $(shell uname -s)
ARCH = $(shell uname -m)

ifeq ($(OS),Darwin)
PROTOC_OS := osx
ifeq ($(ARCH),arm64)
ARCH = aarch_64
endif
endif
ifeq ($(OS),Linux)
PROTOC_OS = linux
endif

PROTOC_INCLUDE = build/include/google


default: gen

gen: grpc mocks

$(GOBINREL):
	mkdir -p "$(GOBIN)"

$(GOBINREL)/protoc: | $(GOBINREL)
	$(eval PROTOC_TMP := $(shell mktemp -d))
	curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v21.9/protoc-21.9-$(PROTOC_OS)-$(ARCH).zip -o "$(PROTOC_TMP)/protoc.zip"
	cd "$(PROTOC_TMP)" && unzip protoc.zip
	cp "$(PROTOC_TMP)/bin/protoc" "$(GOBIN)"
	mkdir -p "$(PROTOC_INCLUDE)"
	cp -R "$(PROTOC_TMP)/include/google/" "$(PROTOC_INCLUDE)"
	rm -rf "$(PROTOC_TMP)"

# 'protoc-gen-go' tool generates proto messages
$(GOBINREL)/protoc-gen-go: | $(GOBINREL)
	$(GOBUILD) -o "$(GOBIN)/protoc-gen-go" google.golang.org/protobuf/cmd/protoc-gen-go

# 'protoc-gen-go-grpc' tool generates grpc services
$(GOBINREL)/protoc-gen-go-grpc: | $(GOBINREL)
	$(GOBUILD) -o "$(GOBIN)/protoc-gen-go-grpc" google.golang.org/grpc/cmd/protoc-gen-go-grpc

protoc-all: $(GOBINREL)/protoc $(PROTOC_INCLUDE) $(GOBINREL)/protoc-gen-go $(GOBINREL)/protoc-gen-go-grpc

protoc-clean:
	rm -f "$(GOBIN)/protoc"*
	rm -rf "$(PROTOC_INCLUDE)"

grpc: protoc-all
	go mod vendor
	PATH="$(GOBIN):$(PATH)" protoc --proto_path=vendor/github.com/ledgerwatch/interfaces --go_out=gointerfaces -I=$(PROTOC_INCLUDE) \
		types/types.proto
	PATH="$(GOBIN):$(PATH)" protoc --proto_path=vendor/github.com/ledgerwatch/interfaces --go_out=gointerfaces --go-grpc_out=gointerfaces -I=$(PROTOC_INCLUDE) \
		--go_opt=Mtypes/types.proto=github.com/ledgerwatch/erigon-lib/gointerfaces/types \
		--go-grpc_opt=Mtypes/types.proto=github.com/ledgerwatch/erigon-lib/gointerfaces/types \
		p2psentry/sentry.proto p2psentinel/sentinel.proto \
		remote/kv.proto remote/ethbackend.proto \
		downloader/downloader.proto execution/execution.proto \
		txpool/txpool.proto txpool/mining.proto
	rm -rf vendor

$(GOBINREL)/moq: | $(GOBINREL)
	$(GOBUILD) -o "$(GOBIN)/moq" github.com/matryer/moq

mocks: $(GOBINREL)/moq
	PATH="$(GOBIN):$(PATH)" go generate ./...

lint: $(GOBINREL)/golangci-lint
	@"$(GOBIN)/golangci-lint" run --config ./.golangci.yml

# force re-make golangci-lint
lintci-deps: lintci-deps-clean $(GOBINREL)/golangci-lint
lintci-deps-clean: golangci-lint-clean

# download and build golangci-lint (https://golangci-lint.run)
$(GOBINREL)/golangci-lint: | $(GOBINREL)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$(GOBIN)" v1.50.1

golangci-lint-clean:
	rm -f "$(GOBIN)/golangci-lint"

test:
	$(GOTEST) --count 1 -p 2 ./...

test-no-fuzz:
	$(GOTEST_NOFUZZ) --count 1 -p 2 ./...
