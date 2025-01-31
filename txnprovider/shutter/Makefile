GOBINREL = build/bin
GOBIN = $(CURDIR)/$(GOBINREL)
OPENZEPPELINDIR = $(CURDIR)/../../contracts/openzeppelin

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
PROTO_DIR = $(CURDIR)/internal/proto

$(GOBINREL):
	mkdir -p "$(GOBIN)"

$(GOBINREL)/protoc: $(GOBINREL)
	$(eval PROTOC_TMP := $(shell mktemp -d))
	curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v27.1/protoc-27.1-$(PROTOC_OS)-$(ARCH).zip -o "$(PROTOC_TMP)/protoc.zip"
	cd "$(PROTOC_TMP)" && unzip protoc.zip
	cp "$(PROTOC_TMP)/bin/protoc" "$(GOBIN)"
	mkdir -p "$(PROTOC_INCLUDE)"
	cp -R "$(PROTOC_TMP)/include/google/" "$(PROTOC_INCLUDE)"
	rm -rf "$(PROTOC_TMP)"

$(GOBINREL)/protoc-gen-go: $(GOBINREL)
	go build -o "$(GOBIN)/protoc-gen-go" google.golang.org/protobuf/cmd/protoc-gen-go

$(GOBINREL)/abigen: $(GOBINREL)
	go build -tags abigen -o "$(GOBIN)/abigen" ../../cmd/abigen

.PHONY: devtools
devtools: $(GOBINREL)/protoc $(GOBINREL)/protoc-gen-go $(GOBINREL)/abigen

.PHONY: proto
proto:
	@test -x $(GOBIN)/protoc || (echo 'Please build protoc by running `make devtools`'; exit 1)
	@test -x $(GOBIN)/protoc-gen-go || (echo 'Please build protoc-gen-go by running `make devtools`'; exit 1)
	PATH="$(GOBIN):$(PATH)" protoc -I=$(PROTO_DIR) --go_out=$(PROTO_DIR) $(PROTO_DIR)/shutter.proto

.PHONY: contracts
contracts: solc abigen

.PHONY: solc
solc:
	@type "solc" 2> /dev/null || (echo 'Please install solc'; exit 1)
	@if [ ! -d "$(OPENZEPPELINDIR)" ] || [ -z "$$(ls $(OPENZEPPELINDIR) 2>/dev/null)" ]; then echo 'Please pull openzeppelin contracts by running `git submodule update --init --recursive --force`'; exit 1; fi
	PATH="$(GOBIN):$(PATH)" go generate -run "solc" ./...

.PHONY: abigen
abigen:
	@test -x $(GOBIN)/abigen || (echo 'Please build abigen by running `make devtools`'; exit 1)
	PATH="$(GOBIN):$(PATH)" go generate -run "abigen" ./...
