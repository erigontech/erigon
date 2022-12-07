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
CGO_CFLAGS += -DMDBX_FORCE_ASSERTIONS=0 # Enable MDBX's asserts by default in 'devel' branch and disable in releases
CGO_CFLAGS += -O
CGO_CFLAGS += -D__BLST_PORTABLE__
CGO_CFLAGS := CGO_CFLAGS="$(CGO_CFLAGS)"
DBG_CGO_CFLAGS += -DMDBX_DEBUG=1

BUILD_TAGS = nosqlite,noboltdb,disable_libutp
PACKAGE = github.com/ledgerwatch/erigon

GO_FLAGS += -trimpath -tags $(BUILD_TAGS) -buildvcs=false
GO_FLAGS += -ldflags "-X ${PACKAGE}/params.GitCommit=${GIT_COMMIT} -X ${PACKAGE}/params.GitBranch=${GIT_BRANCH} -X ${PACKAGE}/params.GitTag=${GIT_TAG}"

GOBUILD = $(CGO_CFLAGS) $(GO) build $(GO_FLAGS)
GO_DBG_BUILD = $(GO) build $(GO_FLAGS) -tags $(BUILD_TAGS),debug -gcflags=all="-N -l"  # see delve docs
GOTEST = $(CGO_CFLAGS) GODEBUG=cgocheck=0 $(GO) test $(GO_FLAGS) ./... -p 2

default: all

## go-version:                        print and verify go version
go-version:
	@if [ $(shell $(GO) version | cut -c 16-17) -lt 18 ]; then \
		echo "minimum required Golang version is 1.18"; \
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
	docker-compose up

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
COMMANDS += downloader
COMMANDS += erigon-cl
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
COMMANDS += lightclient
COMMANDS += sentinel

# build each command using %.cmd rule
$(COMMANDS): %: %.cmd

## all:                               run erigon with all commands
all: erigon $(COMMANDS)

## db-tools:                          build db tools
db-tools:
	@echo "Building db-tools"

	go mod vendor
	cd vendor/github.com/torquem-ch/mdbx-go && MDBX_BUILD_TIMESTAMP=unknown make tools
	cd vendor/github.com/torquem-ch/mdbx-go/mdbxdist && cp mdbx_chk $(GOBIN) && cp mdbx_copy $(GOBIN) && cp mdbx_dump $(GOBIN) && cp mdbx_drop $(GOBIN) && cp mdbx_load $(GOBIN) && cp mdbx_stat $(GOBIN)
	rm -rf vendor
	@echo "Run \"$(GOBIN)/mdbx_stat -h\" to get info about mdbx db file."

## test:                              run unit tests with a 50s timeout
test:
	$(GOTEST) --timeout 50s

test3:
	$(GOTEST) --timeout 50s -tags $(BUILD_TAGS),erigon3

## test-integration:                  run integration tests with a 30m timeout
test-integration:
	$(GOTEST) --timeout 30m -tags $(BUILD_TAGS),integration

test3-integration:
	$(GOTEST) --timeout 30m -tags $(BUILD_TAGS),integration,erigon3

## lint:                              run golangci-lint with .golangci.yml config file
lint:
	@./build/bin/golangci-lint run --config ./.golangci.yml

## lintci:                            run golangci-lint (additionally outputs message before run)
lintci:
	@echo "--> Running linter for code"
	@./build/bin/golangci-lint run --config ./.golangci.yml

## lintci-deps:                       (re)installs golangci-lint to build/bin/golangci-lint
lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./build/bin v1.50.1

## clean:                             cleans the go cache, build dir, libmdbx db dir
clean:
	go clean -cache
	rm -fr build/*

# The devtools target installs tools required for 'go generate'.
# You need to put $GOBIN (or $GOPATH/bin) in your PATH to use 'go generate'.

## devtools:                          installs dev tools (and checks for npm installation etc.)
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

## bindings:                          generate test contracts and core contracts
bindings:
	PATH=$(GOBIN):$(PATH) go generate ./tests/contracts/
	PATH=$(GOBIN):$(PATH) go generate ./core/state/contracts/

## prometheus:                        run prometheus and grafana with docker-compose
prometheus:
	docker-compose up prometheus grafana

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

PACKAGE_NAME          := github.com/ledgerwatch/erigon
GOLANG_CROSS_VERSION  ?= v1.18.1

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
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--rm-dist --skip-validate --skip-publish

.PHONY: release
release: git-submodules
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
		goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--rm-dist --skip-validate

# since DOCKER_UID, DOCKER_GID are default initialized to the current user uid/gid,
# we need separate envvars to facilitate creation of the erigon user on the host OS.
ERIGON_USER_UID ?= 3473
ERIGON_USER_GID ?= 3473
ERIGON_USER_XDG_DATA_HOME ?= ~$(ERIGON_USER)/.local/share

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
	sudo -u $(ERIGON_USER) mkdir -p $(ERIGON_USER_XDG_DATA_HOME)

## user_macos:                        create "erigon" user (MacOS)
user_macos:
	sudo dscl . -create /Users/$(ERIGON_USER)
	sudo dscl . -create /Users/$(ERIGON_USER) UserShell /bin/bash
	sudo dscl . -list /Users UniqueID | grep $(ERIGON_USER) | grep $(ERIGON_USER_UID) || sudo dscl . -create /Users/$(ERIGON_USER) UniqueID $(ERIGON_USER_UID)
	sudo dscl . -create /Users/$(ERIGON_USER) PrimaryGroupID $(ERIGON_USER_GID)
	sudo dscl . -create /Users/$(ERIGON_USER) NFSHomeDirectory /Users/$(ERIGON_USER)
	sudo dscl . -append /Groups/admin GroupMembership $(ERIGON_USER)
	sudo -u $(ERIGON_USER) mkdir -p $(ERIGON_USER_XDG_DATA_HOME)

## coverage:                          run code coverage report and output total coverage %
.PHONY: coverage
coverage:
	@go test -coverprofile=coverage.out ./... > /dev/null 2>&1 && go tool cover -func coverage.out | grep total | awk '{print substr($$3, 1, length($$3)-1)}'

## hive:                              run hive test suite locally using docker e.g. OUTPUT_DIR=~/results/hive SIM=ethereum/engine make hive
.PHONY: hive
hive:
	DOCKER_TAG=thorax/erigon:ci-local make docker
	docker pull thorax/hive:latest
	docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v $(OUTPUT_DIR):/work thorax/hive:latest --sim $(SIM) --results-root=/work/results --client erigon_ci-local # run erigon

## help:                              print commands help
help	:	Makefile
	@sed -n 's/^##//p' $<
