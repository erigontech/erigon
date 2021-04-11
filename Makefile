GOBIN = $(CURDIR)/build/bin
GOTEST = go test ./... -p 1 --tags 'mdbx'

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git branch --show-current)
GOBUILD = env GO111MODULE=on go build -trimpath -tags "mdbx" -ldflags "-X main.gitCommit=${GIT_COMMIT} -X main.gitBranch=${GIT_BRANCH}"

OS = $(shell uname -s)
ARCH = $(shell uname -m)

ifeq ($(OS),Darwin)
PROTOC_OS := osx
endif
ifeq ($(OS),Linux)
PROTOC_OS = linux
endif

all: tg hack rpctest state pics rpcdaemon integration db-tools

docker:
	docker build -t turbo-geth:latest --build-arg git_commit='${GIT_COMMIT}' --build-arg git_branch='${GIT_BRANCH}' .

docker-compose:
	docker-compose up

geth:
	$(GOBUILD) -o $(GOBIN)/tg ./cmd/tg
	@echo "Done building."
	@echo "Run \"$(GOBIN)/tg\" to launch turbo-geth."

tg: mdbx
	@echo "Building tg"
	$(GOBUILD) -o $(GOBIN)/tg ./cmd/tg
	@echo "Done building."
	@echo "Run \"$(GOBIN)/tg\" to launch turbo-geth."

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
	cp ethdb/mdbx/dist/mdbx_drop $(GOBIN)
	@echo "Run \"$(GOBIN)/lmdb_stat -h\" to get info about lmdb file."

mdbx:
	@echo "Building mdbx"
	@cd ethdb/mdbx/dist/ \
		&& make clean && make config.h \
		&& echo '#define MDBX_DEBUG 0' >> config.h \
		&& echo '#define MDBX_FORCE_ASSERTIONS 0' >> config.h \
		&& echo '#define MDBX_ENABLE_MADVISE 0' >> config.h \
        && echo '#define MDBX_TXN_CHECKOWNER 1' >> config.h \
        && echo '#define MDBX_ENV_CHECKPID 1' >> config.h \
        && echo '#define MDBX_DISABLE_PAGECHECKS 0' >> config.h \
        && CFLAGS_EXTRA="-Wno-deprecated-declarations" make mdbx-static.o

test: mdbx
	$(GOTEST) --timeout 15m

test-lmdb:
	TEST_DB=lmdb $(GOTEST)


test-mdbx: mdbx
	TEST_DB=mdbx $(GOTEST)

lint: lintci

lintci: mdbx
	@echo "--> Running linter for code"
	@./build/bin/golangci-lint run --build-tags="mdbx" --config ./.golangci.yml

lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b ./build/bin v1.38.0

clean:
	env GO111MODULE=on go clean -cache
	rm -fr build/*
	rm -f semantics/z3/build/libz3.a
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
	cd $(PROTOC_TMP); curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v3.15.6/protoc-3.15.6-$(PROTOC_OS)-$(ARCH).zip -o protoc.zip
	cd $(PROTOC_TMP); unzip protoc.zip && mv bin/protoc $(GOBIN) && mv include $(GOBIN)/..

	$(GOBUILD) -o $(GOBIN)/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go # generates proto messages
	$(GOBUILD) -o $(GOBIN)/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc # generates grpc services
	PATH=$(GOBIN):$(PATH) protoc --proto_path=interfaces --go_out=gointerfaces --go-grpc_out=gointerfaces -I=build/include/google \
		--go_opt=Mtypes/types.proto=github.com/ledgerwatch/turbo-geth/gointerfaces/types \
		types/types.proto \
		p2psentry/sentry.proto \
		remote/kv.proto remote/ethbackend.proto \
		snapshot_downloader/external_downloader.proto

prometheus:
	docker-compose up prometheus grafana


escape:
	cd $(path) && go test -gcflags "-m -m" -run none -bench=BenchmarkJumpdest* -benchmem -memprofile mem.out
