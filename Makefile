GOBIN = $(CURDIR)/build/bin
GOBUILD = env GO111MODULE=on go build -trimpath
OS = $(shell uname -s)
ARCH = $(shell uname -m)

ifeq ($(OS),Darwin)
PROTOC_OS := osx
endif
ifeq ($(OS),Linux)
PROTOC_OS = linux
endif


gen: grpc mocks

grpc:
	mkdir -p ./build/bin/
	rm -f ./build/bin/protoc*
	rm -rf ./build/include*

	$(eval PROTOC_TMP := $(shell mktemp -d))
	cd $(PROTOC_TMP); curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v3.18.0/protoc-3.18.0-$(PROTOC_OS)-$(ARCH).zip -o protoc.zip
	cd $(PROTOC_TMP); unzip protoc.zip && mv bin/protoc "$(GOBIN)" && mv include "$(GOBIN)"/..

	$(GOBUILD) -o "$(GOBIN)"/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go # generates proto messages
	$(GOBUILD) -o "$(GOBIN)"/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc # generates grpc services

	PATH="$(GOBIN)":"$(PATH)" protoc --proto_path=interfaces --go_out=gointerfaces -I=build/include/google \
		types/types.proto
	PATH="$(GOBIN)":"$(PATH)" protoc --proto_path=interfaces --go_out=gointerfaces --go-grpc_out=gointerfaces -I=build/include/google \
		--go_opt=Mtypes/types.proto=github.com/ledgerwatch/erigon-lib/gointerfaces/types \
		--go-grpc_opt=Mtypes/types.proto=github.com/ledgerwatch/erigon-lib/gointerfaces/types \
		p2psentry/sentry.proto \
		remote/kv.proto remote/ethbackend.proto \
		snapshot_downloader/external_downloader.proto \
		consensus_engine/consensus.proto \
		testing/testing.proto \
		txpool/txpool.proto txpool/mining.proto


mocks:
	$(GOBUILD) -o "$(GOBIN)"/moq	  github.com/matryer/moq
	PATH="$(GOBIN)":"$(PATH)" go generate ./...

lint:
	@./build/bin/golangci-lint run --config ./.golangci.yml

lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./build/bin v1.42.1
