GO = go # if using docker, should not need to be installed/linked
GOBIN = $(CURDIR)/build/bin
UNAME = $(shell uname) # Supported: Darwin, Linux
DOCKER := $(shell command -v docker 2> /dev/null)

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
GIT_TAG    ?= $(shell git describe --tags '--match=v*' --dirty)
ERIGON_USER ?= erigon
# if using volume-mounting data dir, then must exist on host OS
DOCKER_UID ?= $(shell id -u)
DOCKER_GID ?= $(shell id -g)
DOCKER_TAG ?= thorax/erigon:latest

# Variables below for building on host OS, and are ignored for docker
#
# Pipe error below to /dev/null since Makefile structure kind of expects
# Go to be available, but with docker it's not strictly necessary
CGO_CFLAGS := $(shell $(GO) env CGO_CFLAGS 2>/dev/null) # don't lose default
CGO_CFLAGS += -DMDBX_FORCE_ASSERTIONS=1 # Enable MDBX's asserts by default in 'devel' branch and disable in 'stable'
CGO_CFLAGS := CGO_CFLAGS="$(CGO_CFLAGS)"
DBG_CGO_CFLAGS += -DMDBX_DEBUG=1

BUILD_TAGS = nosqlite,noboltdb
PACKAGE = github.com/ledgerwatch/erigon

GO_FLAGS += -trimpath -tags $(BUILD_TAGS) -buildvcs=false
GO_FLAGS += -ldflags "-X ${PACKAGE}/params.GitCommit=${GIT_COMMIT} -X ${PACKAGE}/params.GitBranch=${GIT_BRANCH} -X ${PACKAGE}/params.GitTag=${GIT_TAG}"

GOBUILD = $(CGO_CFLAGS) $(GO) build $(GO_FLAGS)
GO_DBG_BUILD = $(DBG_CGO_CFLAGS) $(GO) build $(GO_FLAGS) -tags $(BUILD_TAGS),debug -gcflags=all="-N -l"  # see delve docs
GOTEST = GODEBUG=cgocheck=0 $(GO) test $(GO_FLAGS) ./... -p 2

default: all

go-version:
	@if [ $(shell $(GO) version | cut -c 16-17) -lt 18 ]; then \
		echo "minimum required Golang version is 1.18"; \
		exit 1 ;\
	fi

validate_docker_build_args:
	@echo "Docker build args:"
	@echo "    DOCKER_UID: $(DOCKER_UID)"
	@echo "    DOCKER_GID: $(DOCKER_GID)\n"
	@echo "Ensuring host OS user exists with specified UID/GID..."
	@if [ "$(UNAME)" = "Darwin" ]; then \
		dscl . list /Users UniqueID | grep "$(DOCKER_UID)"; \
	elif [ "$(UNAME)" = "Linux" ]; then \
		cat /etc/passwd | grep "$(DOCKER_UID):$(DOCKER_GID)"; \
	fi
	@echo "✔️ host OS user exists: $(shell id -nu $(DOCKER_UID))"

docker: validate_docker_build_args git-submodules
	DOCKER_BUILDKIT=1 $(DOCKER) build -t ${DOCKER_TAG} \
		--build-arg "BUILD_DATE=$(shell date -Iseconds)" \
		--build-arg VCS_REF=${GIT_COMMIT} \
		--build-arg VERSION=${GIT_TAG} \
		--build-arg UID=${DOCKER_UID} \
		--build-arg GID=${DOCKER_GID} \
		${DOCKER_FLAGS} \
		.

xdg_data_home :=  ~/.local/share
ifdef XDG_DATA_HOME
	xdg_data_home = $(XDG_DATA_HOME)
endif
xdg_data_home_subdirs = $(xdg_data_home)/erigon $(xdg_data_home)/erigon-grafana $(xdg_data_home)/erigon-prometheus

setup_xdg_data_home:
	mkdir -p $(xdg_data_home_subdirs)
	ls -aln $(xdg_data_home) | grep -E "472.*0.*erigon-grafana" || sudo chown -R 472:0 $(xdg_data_home)/erigon-grafana
	@echo "✔️ xdg_data_home setup"
	@ls -al $(xdg_data_home)

docker-compose: validate_docker_build_args setup_xdg_data_home
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
COMMANDS += hack
COMMANDS += integration
COMMANDS += observer
COMMANDS += pics
COMMANDS += rpcdaemon
COMMANDS += rpcdaemon22
COMMANDS += rpctest
COMMANDS += sentry
COMMANDS += state
COMMANDS += txpool

# build each command using %.cmd rule
$(COMMANDS): %: %.cmd

all: erigon $(COMMANDS)

db-tools: git-submodules
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
	$(GOTEST) --timeout 30s

test-integration:
	$(GOTEST) --timeout 30m -tags $(BUILD_TAGS),integration

lint:
	@./build/bin/golangci-lint run --config ./.golangci.yml

lintci:
	@echo "--> Running linter for code"
	@./build/bin/golangci-lint run --config ./.golangci.yml

lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./build/bin v1.46.2

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
	#PATH=$(GOBIN):$(PATH) go generate ./eth/ethconfig/...
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
	@[ -d ".git" ] || (echo "Not a git repository" && exit 1)
	@echo "Updating git submodules"
	@# Dockerhub using ./hooks/post-checkout to set submodules, so this line will fail on Dockerhub
	@# these lines will also fail if ran as root in a non-root user's checked out repository
	@git submodule sync --quiet --recursive || true
	@git submodule update --quiet --init --recursive --force || true

# since DOCKER_UID, DOCKER_GID are default initialized to the current user uid/gid,
# we need separate envvars to facilitate creation of the erigon user on the host OS.
ERIGON_USER_UID ?= 3473
ERIGON_USER_GID ?= 3473
ERIGON_USER_XDG_DATA_HOME ?= ~$(ERIGON_USER)/.local/share

# create "erigon" user
user_linux:
ifdef DOCKER
	sudo groupadd -f docker
endif
	sudo addgroup --gid $(ERIGON_USER_GID) $(ERIGON_USER) 2> /dev/null || true
	sudo adduser --disabled-password --gecos '' --uid $(ERIGON_USER_UID) --gid $(ERIGON_USER_GID) $(ERIGON_USER) 2> /dev/null || true
	sudo mkhomedir_helper $(ERIGON_USER)
	echo 'export PATH=$$PATH:/usr/local/go/bin' | sudo -u $(ERIGON_USER) tee /home/$(ERIGON_USER)/.bash_aliases >/dev/null
ifdef DOCKER
	sudo usermod -aG docker $(ERIGON_USER)
endif
	sudo -u $(ERIGON_USER) mkdir -p $(ERIGON_USER_XDG_DATA_HOME)

# create "erigon" user
user_macos:
	sudo dscl . -create /Users/$(ERIGON_USER)
	sudo dscl . -create /Users/$(ERIGON_USER) UserShell /bin/bash
	sudo dscl . -list /Users UniqueID | grep $(ERIGON_USER) | grep $(ERIGON_USER_UID) || sudo dscl . -create /Users/$(ERIGON_USER) UniqueID $(ERIGON_USER_UID)
	sudo dscl . -create /Users/$(ERIGON_USER) PrimaryGroupID $(ERIGON_USER_GID)
	sudo dscl . -create /Users/$(ERIGON_USER) NFSHomeDirectory /Users/$(ERIGON_USER)
	sudo dscl . -append /Groups/admin GroupMembership $(ERIGON_USER)
	sudo -u $(ERIGON_USER) mkdir -p $(ERIGON_USER_XDG_DATA_HOME)
