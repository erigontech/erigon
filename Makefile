GO ?= go # if using docker, should not need to be installed/linked
GOAMD64_VERSION ?= v2 # See https://go.dev/wiki/MinimumRequirements#microarchitecture-support
GOBINREL = build/bin
GOBIN = $(CURDIR)/$(GOBINREL)
UNAME = $(shell uname) # Supported: Darwin, Linux
DOCKER := $(shell command -v docker 2> /dev/null)

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
GIT_TAG    ?= $(shell git describe --tags '--match=*.*.*' --abbrev=7 --dirty)
ERIGON_USER ?= erigon
# if using volume-mounting data dir, then must exist on host OS
DOCKER_UID ?= $(shell id -u)
DOCKER_GID ?= $(shell id -g)
DOCKER_TAG ?= erigontech/erigon:latest

# Variables below for building on host OS, and are ignored for docker
#
# Pipe error below to /dev/null since Makefile structure kind of expects
# Go to be available, but with docker it's not strictly necessary
CGO_CFLAGS := $(shell $(GO) env CGO_CFLAGS 2>/dev/null) # don't lose default
CGO_CFLAGS += -DMDBX_FORCE_ASSERTIONS=0 # Enable MDBX's asserts by default in 'main' branch and disable in releases
CGO_CFLAGS += -DMDBX_DISABLE_VALIDATION=0 # Can disable it on CI by separated PR which will measure perf impact.
#CGO_CFLAGS += -DMDBX_ENABLE_PROFGC=0 # Disabled by default, but may be useful for performance debugging
#CGO_CFLAGS += -DMDBX_ENABLE_PGOP_STAT=0 # Disabled by default, but may be useful for performance debugging
CGO_CFLAGS += -DMDBX_ENV_CHECKPID=0 # Erigon doesn't do fork() syscall

# If it is arm64 or aarch64, then we need to use portable version of blst. use or with stringw "arm64" and "aarch64" to support both
ifeq ($(shell uname -m), arm64)
	CGO_CFLAGS += -D__BLST_PORTABLE__
endif
ifeq ($(shell uname -m), aarch64)
	CGO_CFLAGS += -D__BLST_PORTABLE__
endif

# Configure GOAMD64 env.variable for AMD64 architecture:
ifeq ($(shell uname -m),x86_64)
	CPU_ARCH= GOAMD64=${GOAMD64_VERSION}
endif

CGO_CFLAGS += -Wno-unknown-warning-option -Wno-enum-int-mismatch -Wno-strict-prototypes -Wno-unused-but-set-variable -O3

CGO_LDFLAGS := $(shell $(GO) env CGO_LDFLAGS 2> /dev/null)
CGO_LDFLAGS += -O3 -g

ifeq ($(shell uname -s), Darwin)
	ifeq ($(filter-out 13.%,$(shell sw_vers --productVersion)),)
		CGO_LDFLAGS += -mmacosx-version-min=13.3
	endif
endif

# about netgo see: https://github.com/golang/go/issues/30310#issuecomment-471669125 and https://github.com/golang/go/issues/57757
BUILD_TAGS = nosqlite,noboltdb,netgo

ifneq ($(shell "$(CURDIR)/turbo/silkworm/silkworm_compat_check.sh"),)
	BUILD_TAGS := $(BUILD_TAGS),nosilkworm
endif

GOPRIVATE = github.com/erigontech/silkworm-go

PACKAGE = github.com/erigontech/erigon

GO_FLAGS += -trimpath -tags $(BUILD_TAGS) -buildvcs=false 
GO_FLAGS += -ldflags "-X ${PACKAGE}/params.GitCommit=${GIT_COMMIT} -X ${PACKAGE}/params.GitBranch=${GIT_BRANCH} -X ${PACKAGE}/params.GitTag=${GIT_TAG}"

GOBUILD = ${CPU_ARCH} CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" GOPRIVATE="$(GOPRIVATE)" $(GO) build $(GO_FLAGS)
GO_DBG_BUILD = ${CPU_ARCH} CGO_CFLAGS="$(CGO_CFLAGS) -DMDBX_DEBUG=1" CGO_LDFLAGS="$(CGO_LDFLAGS)" GOPRIVATE="$(GOPRIVATE)" $(GO) build -tags $(BUILD_TAGS),debug -gcflags=all="-N -l"  # see delve docs
GOTEST = ${CPU_ARCH} CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" GOPRIVATE="$(GOPRIVATE)" GODEBUG=cgocheck=0 GOTRACEBACK=1 $(GO) test $(GO_FLAGS) ./... -p 2

default: all

## go-version:                        print and verify go version
go-version:
	@if [ $(shell $(GO) version | cut -c 16-17) -lt 20 ]; then \
		echo "minimum required Golang version is 1.20"; \
		exit 1 ;\
	fi

## validate_docker_build_args:        ensure docker build args are valid
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

## docker:                            validate, update submodules and build with docker
docker: validate_docker_build_args git-submodules
	DOCKER_BUILDKIT=1 $(DOCKER) build -t ${DOCKER_TAG} \
		--build-arg "BUILD_DATE=$(shell date +"%Y-%m-%dT%H:%M:%S:%z")" \
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

## setup_xdg_data_home:               TODO
setup_xdg_data_home:
	mkdir -p $(xdg_data_home_subdirs)
	ls -aln $(xdg_data_home) | grep -E "472.*0.*erigon-grafana" || chown -R 472:0 $(xdg_data_home)/erigon-grafana
	@echo "✔️ xdg_data_home setup"
	@ls -al $(xdg_data_home)

## docker-compose:                    validate build args, setup xdg data home, and run docker-compose up
docker-compose: validate_docker_build_args setup_xdg_data_home
	docker compose up

## dbg                                debug build allows see C stack traces, run it with GOTRACEBACK=crash. You don't need debug build for C pit for profiling. To profile C code use SETCGOTRCKEBACK=1
dbg:
	$(GO_DBG_BUILD) -o $(GOBIN)/ ./cmd/...

%.cmd:
	@# Note: $* is replaced by the command name
	@echo "Building $*"
	@cd ./cmd/$* && $(GOBUILD) -o $(GOBIN)/$*
	@echo "Run \"$(GOBIN)/$*\" to launch $*."

## geth:                              run erigon (TODO: remove?)
geth: erigon

## erigon:                            build erigon
erigon: go-version erigon.cmd
	@rm -f $(GOBIN)/tg # Remove old binary to prevent confusion where users still use it because of the scripts

COMMANDS += devnet
COMMANDS += capcli
COMMANDS += downloader
COMMANDS += hack
COMMANDS += integration
COMMANDS += observer
COMMANDS += pics
COMMANDS += rpcdaemon
COMMANDS += rpctest
COMMANDS += sentry
COMMANDS += state
COMMANDS += txpool
COMMANDS += verkle
COMMANDS += evm
COMMANDS += sentinel
COMMANDS += caplin
COMMANDS += snapshots
COMMANDS += diag

# build each command using %.cmd rule
$(COMMANDS): %: %.cmd

## all:                               run erigon with all commands
all: erigon $(COMMANDS)

## db-tools:                          build db tools
db-tools:
	@echo "Building db-tools"

	go mod vendor
	cd vendor/github.com/erigontech/mdbx-go && MDBX_BUILD_TIMESTAMP=unknown make tools
	mkdir -p $(GOBIN)
	cd vendor/github.com/erigontech/mdbx-go/mdbxdist && cp mdbx_chk $(GOBIN) && cp mdbx_copy $(GOBIN) && cp mdbx_dump $(GOBIN) && cp mdbx_drop $(GOBIN) && cp mdbx_load $(GOBIN) && cp mdbx_stat $(GOBIN)
	rm -rf vendor
	@echo "Run \"$(GOBIN)/mdbx_stat -h\" to get info about mdbx db file."

test-erigon-lib:
	@cd erigon-lib && $(MAKE) test

test-erigon-ext:
	@cd tests/erigon-ext-test && ./test.sh $(GIT_COMMIT)

## test:                              run unit tests with a 100s timeout
test: test-erigon-lib
	$(GOTEST) --timeout 10m -coverprofile=coverage.out

## test-integration:                  run integration tests with a 30m timeout
test-integration: test-erigon-lib
	$(GOTEST) --timeout 240m -tags $(BUILD_TAGS),integration

## test-hive						run the hive tests locally off nektos/act workflows simulator
test-hive:
	@if ! command -v act >/dev/null 2>&1; then \
		echo "act command not found in PATH, please source it in PATH. If nektosact is not installed, install it by visiting https://nektosact.com/installation/index.html"; \
	elif [ -z "$(GITHUB_TOKEN)"]; then \
		echo "Please export GITHUB_TOKEN var in the environment"; \
	else \
		act -j test-hive -s GITHUB_TOKEN=$(GITHUB_TOKEN) ; \
	fi

## lint-deps:                         install lint dependencies
lint-deps:
	@cd erigon-lib && $(MAKE) lint-deps

## lintci:                            run golangci-lint linters
lintci:
	@cd erigon-lib && $(MAKE) lintci
	@./erigon-lib/tools/golangci_lint.sh

## lint:                              run all linters
lint:
	@cd erigon-lib && $(MAKE) lint
	@./erigon-lib/tools/golangci_lint.sh
	@./erigon-lib/tools/mod_tidy_check.sh

## clean:                             cleans the go cache, build dir, libmdbx db dir
clean:
	go clean -cache
	rm -fr build/*

# The devtools target installs tools required for 'go generate'.
# You need to put $GOBIN (or $GOPATH/bin) in your PATH to use 'go generate'.

## devtools:                          installs dev tools (and checks for npm installation etc.)
devtools:
	# Notice! If you adding new binary - add it also to tools.go file
	$(GOBUILD) -o $(GOBIN)/gencodec github.com/fjl/gencodec
	$(GOBUILD) -o $(GOBIN)/abigen ./cmd/abigen
	@type "npm" 2> /dev/null || echo 'Please install node.js and npm'
	@type "solc" 2> /dev/null || echo 'Please install solc'
	@type "protoc" 2> /dev/null || echo 'Please install protoc'

## mocks:                             generate test mocks
mocks:
	rm -f $(GOBIN)/mockgen
	$(GOBUILD) -o "$(GOBIN)/mockgen" go.uber.org/mock/mockgen
	grep -r -l --exclude-dir="erigon-lib" --exclude-dir="tests" --exclude-dir="*$(GOBINREL)*" "^// Code generated by MockGen. DO NOT EDIT.$$" . | xargs rm -r
	PATH="$(GOBIN):$(PATH)" go generate -run "mockgen" ./...

## solc:                              generate all solidity contracts
solc:
	PATH="$(GOBIN):$(PATH)" go generate -run "solc" ./...

## abigen:                            generate abis using abigen
abigen:
	$(GOBUILD) -o $(GOBIN)/abigen ./cmd/abigen
	PATH="$(GOBIN):$(PATH)" go generate -run "abigen" ./...

## gencodec:                          generate marshalling code using gencodec
gencodec:
	$(GOBUILD) -o $(GOBIN)/gencodec github.com/fjl/gencodec
	PATH="$(GOBIN):$(PATH)" go generate -run "gencodec" ./...

## graphql:                           generate graphql code
graphql:
	PATH=$(GOBIN):$(PATH) cd ./cmd/rpcdaemon/graphql && go run github.com/99designs/gqlgen .

## gen:                               generate all auto-generated code in the codebase
gen: mocks solc abigen gencodec graphql
	@cd erigon-lib && $(MAKE) gen

## bindings:                          generate test contracts and core contracts
bindings:
	PATH=$(GOBIN):$(PATH) go generate ./tests/contracts/
	PATH=$(GOBIN):$(PATH) go generate ./core/state/contracts/

## prometheus:                        run prometheus and grafana with docker-compose
prometheus:
	docker compose up prometheus grafana

## escape:                            run escape path={path} to check for memory leaks e.g. run escape path=cmd/erigon
escape:
	cd $(path) && go test -gcflags "-m -m" -run none -bench=BenchmarkJumpdest* -benchmem -memprofile mem.out

## git-submodules:                    update git submodules
git-submodules:
	@[ -d ".git" ] || (echo "Not a git repository" && exit 1)
	@echo "Updating git submodules"
	@# Dockerhub using ./hooks/post-checkout to set submodules, so this line will fail on Dockerhub
	@# these lines will also fail if ran as root in a non-root user's checked out repository
	@git submodule sync --quiet --recursive || true
	@git submodule update --quiet --init --recursive --force || true

## install:                            copies binaries and libraries to DIST
DIST ?= $(CURDIR)/build/dist
.PHONY: install
install:
	mkdir -p "$(DIST)"
	cp -f "$$($(CURDIR)/turbo/silkworm/silkworm_lib_path.sh)" "$(DIST)"
	cp -f "$(GOBIN)/"* "$(DIST)"
	@echo "Copied files to $(DIST):"
	@ls -al "$(DIST)"

PACKAGE_NAME          := github.com/erigontech/erigon
GOLANG_CROSS_VERSION  ?= v1.21.5


.PHONY: release-dry-run
release-dry-run: git-submodules
	@docker run \
		--rm \
		--privileged \
		-e CGO_ENABLED=1 \
		-e GITHUB_TOKEN \
		-e DOCKER_USERNAME \
		-e DOCKER_PASSWORD \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/$(PACKAGE_NAME) \
		-w /go/src/$(PACKAGE_NAME) \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip=validate --skip=publish
# since DOCKER_UID, DOCKER_GID are default initialized to the current user uid/gid,
# we need separate envvars to facilitate creation of the erigon user on the host OS.
ERIGON_USER_UID ?= 3473
ERIGON_USER_GID ?= 3473

## user_linux:                        create "erigon" user (Linux)
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
	sudo -u $(ERIGON_USER) mkdir -p /home/$(ERIGON_USER)/.local/share

## user_macos:                        create "erigon" user (MacOS)
user_macos:
	sudo dscl . -create /Users/$(ERIGON_USER)
	sudo dscl . -create /Users/$(ERIGON_USER) UserShell /bin/bash
	sudo dscl . -list /Users UniqueID | grep $(ERIGON_USER) | grep $(ERIGON_USER_UID) || sudo dscl . -create /Users/$(ERIGON_USER) UniqueID $(ERIGON_USER_UID)
	sudo dscl . -create /Users/$(ERIGON_USER) PrimaryGroupID $(ERIGON_USER_GID)
	sudo dscl . -create /Users/$(ERIGON_USER) NFSHomeDirectory /Users/$(ERIGON_USER)
	sudo dscl . -append /Groups/admin GroupMembership $(ERIGON_USER)
	sudo -u $(ERIGON_USER) mkdir -p /Users/$(ERIGON_USER)/.local/share

## automated-tests                    run automated tests (BUILD_ERIGON=0 to prevent erigon build with local image tag)
.PHONY: automated-tests
automated-tests:
	./tests/automated-testing/run.sh

## help:                              print commands help
help	:	Makefile
	@sed -n 's/^##//p' $<
