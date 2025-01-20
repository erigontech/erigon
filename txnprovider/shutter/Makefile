GOBINREL = build/bin
GOBIN = $(CURDIR)/$(GOBINREL)

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
PROTO_DIR = $(CURDIR)/proto

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

.PHONY: proto
proto: $(GOBINREL)/protoc $(GOBINREL)/protoc-gen-go
	PATH="$(GOBIN):$(PATH)" protoc -I=$(PROTO_DIR) --go_out=$(PROTO_DIR) $(PROTO_DIR)/shutter.proto
