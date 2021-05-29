GOBIN = $(CURDIR)/build/bin
GOTEST = go test ./... -p 1 --tags 'mdbx'

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
GOBUILD = env GO111MODULE=on go build -trimpath -tags=mdbx -ldflags "-X main.gitCommit=${GIT_COMMIT} -X main.gitBranch=${GIT_BRANCH}"
GO_DBG_BUILD = env CGO_CFLAGS='-O0 -g -DMDBX_BUILD_FLAGS_CONFIG="config.h"' go build -trimpath -tags=mdbx,debug -ldflags "-X main.gitCommit=${GIT_COMMIT} -X main.gitBranch=${GIT_BRANCH}" -gcflags=all="-N -l"  # see delve docs

GO_MAJOR_VERSION = $(shell go version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f1)
GO_MINOR_VERSION = $(shell go version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f2)

OS = $(shell uname -s)
ARCH = $(shell uname -m)

ifeq ($(OS),Darwin)
PROTOC_OS := osx
endif
ifeq ($(OS),Linux)
PROTOC_OS = linux
endif

all: erigon hack rpctest state pics rpcdaemon integration db-tools

go-version:
	@if [ $(GO_MINOR_VERSION) -lt 16 ]; then \
		echo "minimum required Golang version is 1.16"; \
		exit 1 ;\
	fi

docker:
	docker build -t turbo-geth:latest --build-arg git_commit='${GIT_COMMIT}' --build-arg git_branch='${GIT_BRANCH}' .

docker-compose:
	docker-compose up

# debug build allows see C stack traces, run it with GOTRACEBACK=crash. You don't need debug build for C pit for profiling. To profile C code use SETCGOTRCKEBACK=1
dbg: mdbx-dbg
	$(GO_DBG_BUILD) -o $(GOBIN)/ ./cmd/...

geth: mdbx
	$(GOBUILD) -o $(GOBIN)/erigon ./cmd/erigon
	@echo "Done building."
	@echo "Run \"$(GOBIN)/erigon\" to launch Erigon."

erigon: go-version mdbx
	@echo "Building Erigon"
	rm -f $(GOBIN)/tg # Remove old binary to prevent confusion where users still use it because of the scripts
	$(GOBUILD) -o $(GOBIN)/erigon ./cmd/erigon
	@echo "Done building."
	@echo "Run \"$(GOBIN)/erigon\" to launch Erigon."

hack:
	$(GOBUILD) -o $(GOBIN)/hack ./cmd/hack
	@echo "Done building."
	@echo "Run \"$(GOBIN)/hack\" to launch hack."

rpctest:
	$(GOBUILD) -o $(GOBIN)/rpctest ./cmd/rpctest
	@echo "Done building."
	@echo "Run \"$(GOBIN)/rpctest\" to launch rpctest."

state:
	$(GOBUILD) -o $(GOBIN)/state ./cmd/state
	@echo "Done building."
	@echo "Run \"$(GOBIN)/state\" to launch state."


pics:
	$(GOBUILD) -o $(GOBIN)/pics ./cmd/pics
	@echo "Done building."
	@echo "Run \"$(GOBIN)/pics\" to launch pics."

rpcdaemon:
	$(GOBUILD) -o $(GOBIN)/rpcdaemon ./cmd/rpcdaemon
	@echo "Done building."
	@echo "Run \"$(GOBIN)/rpcdaemon\" to launch rpcdaemon."

integration:
	$(GOBUILD) -o $(GOBIN)/integration ./cmd/integration
	@echo "Done building."
	@echo "Run \"$(GOBIN)/integration\" to launch integration tests."

headers:
	$(GOBUILD) -o $(GOBIN)/headers ./cmd/headers
	@echo "Done building."
	@echo "Run \"$(GOBIN)/headers\" to run headers download PoC."

cons:
	$(GOBUILD) -o $(GOBIN)/cons ./cmd/cons
	@echo "Done building."
	@echo "Run \"$(GOBIN)/cons\" to run consensus engine PoC."

evm:
	$(GOBUILD) -o $(GOBIN)/evm ./cmd/evm
	@echo "Done building."
	@echo "Run \"$(GOBIN)/evm\" to run EVM"

seeder:
	$(GOBUILD) -o $(GOBIN)/seeder ./cmd/snapshots/seeder
	@echo "Done building."
	@echo "Run \"$(GOBIN)/seeder\" to seed snapshots."

sndownloader:
	$(GOBUILD) -o $(GOBIN)/sndownloader ./cmd/snapshots/downloader
	@echo "Done building."
	@echo "Run \"$(GOBIN)/sndownloader\" to seed snapshots."

tracker:
	$(GOBUILD) -o $(GOBIN)/tracker ./cmd/snapshots/tracker
	@echo "Done building."
	@echo "Run \"$(GOBIN)/tracker\" to run snapshots tracker."

db-tools: mdbx
	@echo "Building bb-tools"
	go mod vendor; cd vendor/github.com/ledgerwatch/lmdb-go/dist; make clean mdb_stat mdb_copy mdb_dump mdb_drop mdb_load; cp mdb_stat $(GOBIN); cp mdb_copy $(GOBIN); cp mdb_dump $(GOBIN); cp mdb_drop $(GOBIN); cp mdb_load $(GOBIN); cd ../../../../..; rm -rf vendor

	cd ethdb/mdbx/dist/ && make tools
	cp ethdb/mdbx/dist/mdbx_chk $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_copy $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_dump $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_drop $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_load $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_stat $(GOBIN)
	@echo "Run \"$(GOBIN)/lmdb_stat -h\" to get info about lmdb file."

mdbx:
	@echo "Building mdbx"
	@cd ethdb/mdbx/dist/ \
		&& make clean && make config.h \
		&& echo '#define MDBX_DEBUG 0' >> config.h \
		&& echo '#define MDBX_FORCE_ASSERTIONS 0' >> config.h \
        && CFLAGS_EXTRA="-Wno-deprecated-declarations" make mdbx-static.o

mdbx-dbg:
	@echo "Building mdbx"
	@cd ethdb/mdbx/dist/ \
		&& make clean && make config.h \
		&& echo '#define MDBX_DEBUG 1' >> config.h \
		&& echo '#define MDBX_FORCE_ASSERTIONS 1' >> config.h \
        && CFLAGS_EXTRA="-Wno-deprecated-declarations" CFLAGS='-O0 -g -Wall -Werror -Wextra -Wpedantic -ffunction-sections -fPIC -fvisibility=hidden -std=gnu11 -pthread -Wno-error=attributes' make mdbx-static.o

test: mdbx
	TEST_DB=mdbx $(GOTEST) --timeout 15m

test-lmdb:
	TEST_DB=lmdb $(GOTEST)


test-mdbx:
	TEST_DB=mdbx $(GOTEST) --timeout 20m

lint:
	@./build/bin/golangci-lint run --build-tags="mdbx" --config ./.golangci.yml

lintci: mdbx
	@echo "--> Running linter for code"
	@./build/bin/golangci-lint run --build-tags="mdbx" --config ./.golangci.yml

lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b ./build/bin v1.40.1

clean:
	env GO111MODULE=on go clean -cache
	rm -fr build/*
	cd ethdb/mdbx/dist/ && make clean

# The devtools target installs tools required for 'go generate'.
# You need to put $GOBIN (or $GOPATH/bin) in your PATH to use 'go generate'.

devtools:
	# Notice! If you adding new binary - add it also to cmd/hack/binary-deps/main.go file
	$(GOBUILD) -o $(GOBIN)/stringer golang.org/x/tools/cmd/stringer
	$(GOBUILD) -o $(GOBIN)/go-bindata github.com/kevinburke/go-bindata/go-bindata
	$(GOBUILD) -o $(GOBIN)/gencodec github.com/fjl/gencodec
	$(GOBUILD) -o $(GOBIN)/codecgen github.com/ugorji/go/codec/codecgen
	$(GOBUILD) -o $(GOBIN)/abigen ./cmd/abigen
	PATH=$(GOBIN):$(PATH) go generate ./common
	PATH=$(GOBIN):$(PATH) go generate ./core/types
	@type "npm" 2> /dev/null || echo 'Please install node.js and npm'
	@type "solc" 2> /dev/null || echo 'Please install solc'
	@type "protoc" 2> /dev/null || echo 'Please install protoc'

bindings:
	PATH=$(GOBIN):$(PATH) go generate ./tests/contracts/
	PATH=$(GOBIN):$(PATH) go generate ./core/state/contracts/

grpc:
	# See also: ./cmd/hack/binary-deps/main.go
	mkdir -p ./build/bin/
	rm -f ./build/bin/protoc*
	rm -rf ./build/include*

	$(eval PROTOC_TMP := $(shell mktemp -d))
	cd $(PROTOC_TMP); curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v3.17.0/protoc-3.17.0-$(PROTOC_OS)-$(ARCH).zip -o protoc.zip
	cd $(PROTOC_TMP); unzip protoc.zip && mv bin/protoc $(GOBIN) && mv include $(GOBIN)/..

	$(GOBUILD) -o $(GOBIN)/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go # generates proto messages
	$(GOBUILD) -o $(GOBIN)/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc # generates grpc services
	PATH=$(GOBIN):$(PATH) protoc --proto_path=interfaces --go_out=gointerfaces -I=build/include/google \
		types/types.proto
	PATH=$(GOBIN):$(PATH) protoc --proto_path=interfaces --go_out=gointerfaces --go-grpc_out=gointerfaces -I=build/include/google \
		--go_opt=Mtypes/types.proto=github.com/ledgerwatch/erigon/gointerfaces/types \
		--go-grpc_opt=Mtypes/types.proto=github.com/ledgerwatch/erigon/gointerfaces/types \
		p2psentry/sentry.proto \
		remote/kv.proto remote/ethbackend.proto \
		snapshot_downloader/external_downloader.proto \
		consensus_engine/consensus.proto \
		testing/testing.proto \
		txpool/txpool.proto txpool/mining.proto

prometheus:
	docker-compose up prometheus grafana


escape:
	cd $(path) && go test -gcflags "-m -m" -run none -bench=BenchmarkJumpdest* -benchmem -memprofile mem.out
