BUILD = build
GOBINREL = $(BUILD)/bin
GOBIN = $(CURDIR)/$(GOBINREL)
OPENZEPPELIN = $(BUILD)/openzeppelin

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
	curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-$(PROTOC_OS)-$(ARCH).zip -o "$(PROTOC_TMP)/protoc.zip"
	cd "$(PROTOC_TMP)" && unzip protoc.zip
	cp "$(PROTOC_TMP)/bin/protoc" "$(GOBIN)"
	mkdir -p "$(PROTOC_INCLUDE)"
	cp -R "$(PROTOC_TMP)/include/google/" "$(PROTOC_INCLUDE)"
	rm -rf "$(PROTOC_TMP)"

$(GOBINREL)/protoc-gen-go: $(GOBINREL)
	go build -o "$(GOBIN)/protoc-gen-go" google.golang.org/protobuf/cmd/protoc-gen-go

$(GOBINREL)/abigen: $(GOBINREL)
	go build -tags abigen -o "$(GOBIN)/abigen" ../../cmd/abigen

$(OPENZEPPELIN):
	mkdir -p $(OPENZEPPELIN)
	$(eval OPENZEPPELIN_TMP_DIR := $(shell mktemp -d))
	$(eval LATEST_VERSION := $(shell curl -s https://api.github.com/repos/OpenZeppelin/openzeppelin-contracts/tags | jq -r '.[].name' | head -n1))
	$(eval LATEST_VERSION_NUM := $(shell echo $(LATEST_VERSION) | cut -c 2-))
	curl -sSL https://github.com/OpenZeppelin/openzeppelin-contracts/archive/refs/tags/$(LATEST_VERSION).zip -o $(OPENZEPPELIN_TMP_DIR)/tmp.zip
	cd $(OPENZEPPELIN_TMP_DIR) && unzip tmp.zip
	cp -R $(OPENZEPPELIN_TMP_DIR)/openzeppelin-contracts-$(LATEST_VERSION_NUM)/* $(OPENZEPPELIN)
	rm -rf "$(OPENZEPPELIN_TMP_DIR)"

.PHONY: protoc-clean
protoc-clean:
	rm -f "$(GOBIN)/protoc"*
	rm -rf "$(PROTOC_INCLUDE)"

.PHONY: proto
proto: protoc-clean $(GOBINREL)/protoc $(GOBINREL)/protoc-gen-go
	PATH="$(GOBIN):$(PATH)" protoc -I=$(PROTO_DIR) --go_out=$(PROTO_DIR) $(PROTO_DIR)/shutter.proto

.PHONY: contracts
contracts: $(OPENZEPPELIN) solc abigen

.PHONY: solc
solc:
	@type "solc" 2> /dev/null || (echo 'Please install solc'; exit 1)
	PATH="$(GOBIN):$(PATH)" go generate -run "solc" ./...

.PHONY: abigen
abigen: $(GOBINREL)/abigen
	@test -x $(GOBIN)/abigen || (echo 'Please build abigen by running `make devtools`'; exit 1)
	PATH="$(GOBIN):$(PATH)" go generate -run "abigen" ./...
