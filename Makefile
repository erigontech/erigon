GOBIN = $(CURDIR)/build/bin
GOBUILD = env GO111MODULE=on go build -trimpath
GOTEST = go test ./... -p 1 --tags 'mdbx'

LATEST_COMMIT ?= $(shell git log -n 1 origin/master --pretty=format:"%H")
ifeq ($(LATEST_COMMIT),)
LATEST_COMMIT := $(shell git log -n 1 HEAD~1 --pretty=format:"%H")
endif

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)

OS = $(shell uname -s)
ARCH = $(shell uname -m)

ifeq ($(OS),Darwin)
PROTOC_OS := osx
endif
ifeq ($(OS),Linux)
PROTOC_OS = linux
endif

all: tg hack tester rpctest state pics rpcdaemon integration db-tools

docker:
	docker build -t turbo-geth:latest  --build-arg git_commit='${GIT_COMMIT}' .

docker-compose:
	docker-compose up

geth:
	$(GOBUILD) -o $(GOBIN)/tg -tags "mdbx" -ldflags "-X main.gitCommit=${GIT_COMMIT}" ./cmd/tg
	@echo "Done building."
	@echo "Run \"$(GOBIN)/tg\" to launch turbo-geth."

tg:
	@echo "Building mdbx"
	cd ethdb/mdbx/dist/ && make clean && make mdbx-static.o && cat config.h
	$(GOBUILD) -o $(GOBIN)/tg -tags "mdbx" -ldflags "-X main.gitCommit=${GIT_COMMIT}" ./cmd/tg
	@echo "Done building."
	@echo "Run \"$(GOBIN)/tg\" to launch turbo-geth."

hack:
	$(GOBUILD) -o $(GOBIN)/hack  -tags "mdbx" ./cmd/hack
	@echo "Done building."
	@echo "Run \"$(GOBIN)/hack\" to launch hack."

tester:
	$(GOBUILD) -o $(GOBIN)/tester -tags 'mdbx' ./cmd/tester
	@echo "Done building."
	@echo "Run \"$(GOBIN)/tester\" to launch tester."

rpctest:
	$(GOBUILD) -o $(GOBIN)/rpctest -tags 'mdbx' ./cmd/rpctest
	@echo "Done building."
	@echo "Run \"$(GOBIN)/rpctest\" to launch rpctest."

state:
	$(GOBUILD) -o $(GOBIN)/state -tags 'mdbx' ./cmd/state
	@echo "Done building."
	@echo "Run \"$(GOBIN)/state\" to launch state."

restapi:
	$(GOBUILD) -o $(GOBIN)/restapi -tags 'mdbx' ./cmd/restapi
	@echo "Done building."
	@echo "Run \"$(GOBIN)/restapi\" to launch restapi."

run-web-ui:
	@echo 'Web: Turbo-Geth Debug Utility is launching...'
	@cd debug-web-ui && yarn start

pics:
	$(GOBUILD) -o $(GOBIN)/pics -tags 'mdbx' ./cmd/pics
	@echo "Done building."
	@echo "Run \"$(GOBIN)/pics\" to launch pics."

rpcdaemon:
	$(GOBUILD) -o $(GOBIN)/rpcdaemon -ldflags "-X commands.gitCommit=${GIT_COMMIT}" -tags 'mdbx' ./cmd/rpcdaemon
	@echo "Done building."
	@echo "Run \"$(GOBIN)/rpcdaemon\" to launch rpcdaemon."

integration:
	$(GOBUILD) -o $(GOBIN)/integration -tags 'mdbx' ./cmd/integration
	@echo "Done building."
	@echo "Run \"$(GOBIN)/integration\" to launch integration tests."

headers:
	$(GOBUILD) -o $(GOBIN)/headers ./cmd/headers
	@echo "Done building."
	@echo "Run \"$(GOBIN)/integration\" to run headers download PoC."

db-tools:
	go mod vendor; cd vendor/github.com/ledgerwatch/lmdb-go/dist; make clean mdb_stat mdb_copy mdb_dump mdb_load; cp mdb_stat $(GOBIN); cp mdb_copy $(GOBIN); cp mdb_dump $(GOBIN); cp mdb_load $(GOBIN); cd ../../../../..; rm -rf vendor
	$(GOBUILD) -o $(GOBIN)/lmdbgo_copy github.com/ledgerwatch/lmdb-go/cmd/lmdb_copy
	$(GOBUILD) -o $(GOBIN)/lmdbgo_stat github.com/ledgerwatch/lmdb-go/cmd/lmdb_stat

	cd ethdb/mdbx/dist/ && make tools
	cp ethdb/mdbx/dist/mdbx_chk $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_copy $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_dump $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_load $(GOBIN)
	cp ethdb/mdbx/dist/mdbx_stat $(GOBIN)
	@echo "Run \"$(GOBIN)/lmdb_stat -h\" to get info about lmdb file."

ethdb/mdbx/dist/mdbx-static.o:
	echo "Building mdbx"
	cd ethdb/mdbx/dist/ \
		&& make clean && make config.h \
		&& echo '#define MDBX_DEBUG 0' >> config.h \
		&& echo '#define MDBX_FORCE_ASSERTIONS 0' >> config.h \
        && echo '#define MDBX_TXN_CHECKOWNER 1' >> config.h \
        && echo '#define MDBX_DISABLE_PAGECHECKS 1' >> config.h \
        && echo '#define MDBX_ENV_CHECKPID 0' >> config.h \
        && CFLAGS_EXTRA="-Wno-deprecated-declarations" make mdbx-static.o

test: ethdb/mdbx/dist/mdbx-static.o
	$(GOTEST)

test-lmdb:
	TEST_DB=lmdb $(GOTEST)


test-mdbx: ethdb/mdbx/dist/mdbx-static.o
	TEST_DB=mdbx $(GOTEST)

lint: lintci

lintci: ethdb/mdbx/dist/mdbx-static.o
	@echo "--> Running linter for code diff versus commit $(LATEST_COMMIT)"
	@./build/bin/golangci-lint run \
	    --new-from-rev=$(LATEST_COMMIT) \
		--build-tags="mdbx" \
	    --config ./.golangci/step1.yml \
	    --exclude "which can be annoying to use"

	@./build/bin/golangci-lint run \
	    --new-from-rev=$(LATEST_COMMIT) \
		--build-tags="mdbx" \
	    --config ./.golangci/step2.yml

	@./build/bin/golangci-lint run \
	    --new-from-rev=$(LATEST_COMMIT) \
		--build-tags="mdbx" \
	    --config ./.golangci/step3.yml

	@./build/bin/golangci-lint run \
	    --new-from-rev=$(LATEST_COMMIT) \
		--build-tags="mdbx" \
	    --config ./.golangci/step4.yml

lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b ./build/bin v1.29.0

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
	PATH=$(GOBIN):$(PATH) go generate ./ethdb/typedbucket
	@type "npm" 2> /dev/null || echo 'Please install node.js and npm'
	@type "solc" 2> /dev/null || echo 'Please install solc'
	@type "protoc" 2> /dev/null || echo 'Please install protoc'

bindings:
	PATH=$(GOBIN):$(PATH) go generate ./tests/contracts/
	PATH=$(GOBIN):$(PATH) go generate ./cmd/tester/contracts/
	PATH=$(GOBIN):$(PATH) go generate ./core/state/contracts/

grpc:
	# See also: ./cmd/hack/binary-deps/main.go
	mkdir -p ./build/bin/
	rm -f ./build/bin/protoc*
	rm -rf ./build/include*

	$(eval PROTOC_TMP := $(shell mktemp -d))
	cd $(PROTOC_TMP); curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v3.14.0/protoc-3.14.0-$(PROTOC_OS)-$(ARCH).zip -o protoc.zip
	cd $(PROTOC_TMP); unzip protoc.zip && mv bin/protoc $(GOBIN) && mv include $(GOBIN)/..

	$(GOBUILD) -o $(GOBIN)/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go # generates proto messages
	$(GOBUILD) -o $(GOBIN)/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc # generates grpc services
	PATH=$(GOBIN):$(PATH) go generate ./ethdb
	PATH=$(GOBIN):$(PATH) go generate ./cmd/headers
	PATH=$(GOBIN):$(PATH) go generate ./turbo/shards
	PATH=$(GOBIN):$(PATH) go generate ./turbo/snapshotsync


simulator-genesis:
	go run ./cmd/tester genesis > ./cmd/tester/simulator_genesis.json

prometheus:
	docker-compose up prometheus grafana

escape:
	cd $(path) && go test -gcflags "-m -m" -run none -bench=BenchmarkJumpdest* -benchmem -memprofile mem.out
