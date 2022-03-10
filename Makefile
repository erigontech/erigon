GO = go
GOBIN = $(CURDIR)/build/bin
GOTEST = GODEBUG=cgocheck=0 $(GO) test -tags nosqlite -trimpath ./... -p 2

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
GIT_TAG    ?= $(shell git describe --tags `git rev-list --tags="v*" --max-count=1`)

CGO_CFLAGS := $(shell $(GO) env CGO_CFLAGS) # don't loose default
CGO_CFLAGS += -DMDBX_FORCE_ASSERTIONS=1 # Enable MDBX's asserts by default in 'devel' branch and disable in 'stable'
CGO_CFLAGS := CGO_CFLAGS="$(CGO_CFLAGS)"
DBG_CGO_CFLAGS += -DMDBX_DEBUG=1

GOBUILD = $(CGO_CFLAGS) $(GO) build -tags nosqlite -trimpath -ldflags "-X github.com/ledgerwatch/erigon/params.GitCommit=${GIT_COMMIT} -X github.com/ledgerwatch/erigon/params.GitBranch=${GIT_BRANCH} -X github.com/ledgerwatch/erigon/params.GitTag=${GIT_TAG}"
GO_DBG_BUILD = $(DBG_CGO_CFLAGS) $(GO) build -tags nosqlite -trimpath -tags=debug -ldflags "-X github.com/ledgerwatch/erigon/params.GitCommit=${GIT_COMMIT} -X github.com/ledgerwatch/erigon/params.GitBranch=${GIT_BRANCH} -X github.com/ledgerwatch/erigon/params.GitTag=${GIT_TAG}" -gcflags=all="-N -l"  # see delve docs

GO_MAJOR_VERSION = $(shell $(GO) version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f1)
GO_MINOR_VERSION = $(shell $(GO) version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f2)

default: all

go-version:
	@if [ $(GO_MINOR_VERSION) -lt 16 ]; then \
		echo "minimum required Golang version is 1.16"; \
		exit 1 ;\
	fi

docker:
	DOCKER_BUILDKIT=1 docker build -t erigon:latest --build-arg git_commit='${GIT_COMMIT}' --build-arg git_branch='${GIT_BRANCH}' --build-arg git_tag='${GIT_TAG}' .

xdg_data_home :=  ~/.local/share
ifdef XDG_DATA_HOME
	xdg_data_home = $(XDG_DATA_HOME)
endif
docker-compose:
	mkdir -p $(xdg_data_home)/erigon $(xdg_data_home)/erigon-grafana $(xdg_data_home)/erigon-prometheus; \
	docker-compose up

# debug build allows see C stack traces, run it with GOTRACEBACK=crash. You don't need debug build for C pit for profiling. To profile C code use SETCGOTRCKEBACK=1
dbg:
	$(GO_DBG_BUILD) -o $(GOBIN)/ ./cmd/...

%.cmd: git-submodules
	@# Note: $* is replaced by the command name
	@echo "Building $*"
	@cd ./cmd/$* && $(GOBUILD) -o $(GOBIN)/$*
	@echo "Run \"$(GOBIN)/$*\" to launch $*."

geth: erigon

erigon: go-version erigon.cmd
	@rm -f $(GOBIN)/tg # Remove old binary to prevent confusion where users still use it because of the scripts

COMMANDS += cons
COMMANDS += devnettest
COMMANDS += downloader
COMMANDS += evm
COMMANDS += hack
COMMANDS += integration
COMMANDS += pics
COMMANDS += rpcdaemon
COMMANDS += rpctest
COMMANDS += sentry
COMMANDS += state
COMMANDS += txpool

# build each command using %.cmd rule
$(COMMANDS): %: %.cmd

all: erigon $(COMMANDS)

db-tools:
	@echo "Building db-tools"

	# hub.docker.com setup incorrect gitpath for git modules. Just remove it and re-init submodule.
	rm -rf libmdbx
	git submodule update --init --recursive --force libmdbx

	cd libmdbx && MDBX_BUILD_TIMESTAMP=unknown make tools
	cp libmdbx/mdbx_chk $(GOBIN)
	cp libmdbx/mdbx_copy $(GOBIN)
	cp libmdbx/mdbx_dump $(GOBIN)
	cp libmdbx/mdbx_drop $(GOBIN)
	cp libmdbx/mdbx_load $(GOBIN)
	cp libmdbx/mdbx_stat $(GOBIN)
	@echo "Run \"$(GOBIN)/mdbx_stat -h\" to get info about mdbx db file."

test:
	$(GOTEST) --timeout 30m

lint:
	@./build/bin/golangci-lint run --config ./.golangci.yml

lintci:
	@echo "--> Running linter for code"
	@./build/bin/golangci-lint run --config ./.golangci.yml

lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./build/bin v1.44.2

clean:
	go clean -cache
	rm -fr build/*
	cd libmdbx/ && make clean

# The devtools target installs tools required for 'go generate'.
# You need to put $GOBIN (or $GOPATH/bin) in your PATH to use 'go generate'.

devtools:
	# Notice! If you adding new binary - add it also to cmd/hack/binary-deps/main.go file
	$(GOBUILD) -o $(GOBIN)/go-bindata github.com/kevinburke/go-bindata/go-bindata
	$(GOBUILD) -o $(GOBIN)/gencodec github.com/fjl/gencodec
	$(GOBUILD) -o $(GOBIN)/codecgen github.com/ugorji/go/codec/codecgen
	$(GOBUILD) -o $(GOBIN)/abigen ./cmd/abigen
	PATH=$(GOBIN):$(PATH) go generate ./common
	PATH=$(GOBIN):$(PATH) go generate ./core/types
	PATH=$(GOBIN):$(PATH) go generate ./consensus/aura/...
	@type "npm" 2> /dev/null || echo 'Please install node.js and npm'
	@type "solc" 2> /dev/null || echo 'Please install solc'
	@type "protoc" 2> /dev/null || echo 'Please install protoc'

bindings:
	PATH=$(GOBIN):$(PATH) go generate ./tests/contracts/
	PATH=$(GOBIN):$(PATH) go generate ./core/state/contracts/

prometheus:
	docker-compose up prometheus grafana


escape:
	cd $(path) && go test -gcflags "-m -m" -run none -bench=BenchmarkJumpdest* -benchmem -memprofile mem.out

git-submodules:
	@echo "Updating git submodules"
	@# Dockerhub using ./hooks/post-checkout to set submodules, so this line will fail on Dockerhub
	@git submodule update --quiet --init --recursive --force || true
