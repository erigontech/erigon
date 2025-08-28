GO ?= go # if using docker, should not need to be installed/linked
GOAMD64_VERSION ?= v2 # See https://go.dev/wiki/MinimumRequirements#microarchitecture-support
GOBINREL := build/bin
GOBIN := $(CURDIR)/$(GOBINREL)
GOARCH ?= $(shell go env GOHOSTARCH)
UNAME := $(shell uname) # Supported: Darwin, Linux
DOCKER := $(shell command -v docker 2> /dev/null)

GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
SHORT_COMMIT := $(shell echo $(GIT_COMMIT) | cut -c 1-8)
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


CGO_CFLAGS += -D__BLST_PORTABLE__

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

BUILD_TAGS =

ifneq ($(shell "$(CURDIR)/turbo/silkworm/silkworm_compat_check.sh"),)
	BUILD_TAGS := $(BUILD_TAGS),nosilkworm
endif

override BUILD_TAGS := $(BUILD_TAGS),$(EXTRA_BUILD_TAGS)

GOPRIVATE = github.com/erigontech/silkworm-go

PACKAGE = github.com/erigontech/erigon

# Add to user provided GO_FLAGS. Insert it after a bunch of other stuff to allow overrides, and before tags to maintain BUILD_TAGS (set that instead if you want to modify it).

GO_RELEASE_FLAGS := -trimpath -buildvcs=false \
	-ldflags "-X ${PACKAGE}/db/version.GitCommit=${GIT_COMMIT} -X ${PACKAGE}/db/version.GitBranch=${GIT_BRANCH} -X ${PACKAGE}/db/version.GitTag=${GIT_TAG}"
GO_BUILD_ENV = GOARCH=${GOARCH} ${CPU_ARCH} CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" GOPRIVATE="$(GOPRIVATE)"

# Basic release build. Pass EXTRA_BUILD_TAGS if you want to modify the tags set.
GOBUILD = $(GO_BUILD_ENV) $(GO) build $(GO_RELEASE_FLAGS) $(GO_FLAGS) -tags $(BUILD_TAGS)
DLV_GO_FLAGS := -gcflags='all="-N -l" -trimpath=false'
GO_BUILD_DEBUG = $(GO_BUILD_ENV) CGO_CFLAGS="$(CGO_CFLAGS) -DMDBX_DEBUG=1" $(GO) build $(DLV_GO_FLAGS) $(GO_FLAGS) -tags $(BUILD_TAGS),debug
GOTEST = $(GO_BUILD_ENV) GODEBUG=cgocheck=0 GOTRACEBACK=1 GOEXPERIMENT=synctest $(GO) test $(GO_FLAGS) ./...

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
docker: 
	DOCKER_BUILDKIT=1 $(DOCKER) build -t ${DOCKER_TAG} \
		--build-arg "BUILD_DATE=$(shell date +"%Y-%m-%dT%H:%M:%S:%z")" \
		--build-arg VCS_REF=${GIT_COMMIT} \
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
	$(GO_BUILD_DEBUG) -o $(GOBIN)/ ./cmd/...

.PHONY: %.cmd
# Deferred (=) because $* isn't defined until the rule is executed.
%.cmd: override OUTPUT = $(GOBIN)/$*$(CMD_BUILD_SUFFIX)
%.cmd:
	@echo Building '$(OUTPUT)'
	cd ./cmd/$* && $(GOBUILD) -o $(OUTPUT)
	@echo "Run \"$(GOBIN)/$*\" to launch $*."

## geth:                              run erigon (TODO: remove?)
geth: erigon

## erigon:                            build erigon
erigon: go-version erigon.cmd
	@rm -f $(GOBIN)/tg # Remove old binary to prevent confusion where users still use it because of the scripts

COMMANDS += capcli
COMMANDS += downloader
COMMANDS += hack
COMMANDS += integration
COMMANDS += pics
COMMANDS += rpcdaemon
COMMANDS += rpctest
COMMANDS += sentry
COMMANDS += state
COMMANDS += txpool
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
	cd vendor/github.com/erigontech/mdbx-go/libmdbx && cp mdbx_chk $(GOBIN) && cp mdbx_copy $(GOBIN) && cp mdbx_dump $(GOBIN) && cp mdbx_drop $(GOBIN) && cp mdbx_load $(GOBIN) && cp mdbx_stat $(GOBIN)
	rm -rf vendor
	@echo "Run \"$(GOBIN)/mdbx_stat -h\" to get info about mdbx db file."

test-erigon-lib-short:
	@cd erigon-lib && $(MAKE) test-short

test-erigon-lib-all:
	@cd erigon-lib && $(MAKE) test-all

test-erigon-lib-all-race:
	@cd erigon-lib && $(MAKE) test-all-race

test-erigon-ext:
	@cd tests/erigon-ext-test && ./test.sh $(GIT_COMMIT)

## test-short:                run short tests with a 10m timeout
test-short: test-erigon-lib-short
	@{ \
		$(GOTEST) -short > run.log 2>&1; \
		STATUS=$$?; \
		grep -v -e ' CONT ' -e 'RUN' -e 'PAUSE' -e 'PASS' run.log; \
		exit $$STATUS; \
	}

## test-all:                  run all tests with a 1h timeout
test-all: test-erigon-lib-all
	@{ \
		$(GOTEST) --timeout 60m -coverprofile=coverage-test-all.out > run.log 2>&1; \
		STATUS=$$?; \
		grep -v -e ' CONT ' -e 'RUN' -e 'PAUSE' -e 'PASS' run.log; \
		exit $$STATUS; \
	}

## test-all-race:             run all tests with the race flag
test-all-race: test-erigon-lib-all-race
	@{ \
		$(GOTEST) --timeout 60m -race > run.log 2>&1; \
		STATUS=$$?; \
		grep -v -e ' CONT ' -e 'RUN' -e 'PAUSE' -e 'PASS' run.log; \
		exit $$STATUS; \
	}

## test-hive						run the hive tests locally off nektos/act workflows simulator
test-hive:
	@if ! command -v act >/dev/null 2>&1; then \
		echo "act command not found in PATH, please source it in PATH. If nektosact is not installed, install it by visiting https://nektosact.com/installation/index.html"; \
	elif [ -z "$(GITHUB_TOKEN)"]; then \
		echo "Please export GITHUB_TOKEN var in the environment" ; \
	elif [ "$(SUITE)" = "eest" ]; then \
		act -j test-hive-eest -s GITHUB_TOKEN=$(GITHUB_TOKEN) ; \
	else \
		act -j test-hive -s GITHUB_TOKEN=$(GITHUB_TOKEN) ; \
	fi


# Define the run_suite function
define run_suite
    printf "\n\n============================================================"; \
    echo "Running test: $1-$2"; \
    printf "\n"; \
    ./hive --sim ethereum/$1 --sim.limit=$2 --sim.parallelism=8 --docker.nocache=true --client erigon $3 2>&1 | tee output.log; \
    if [ $$? -gt 0 ]; then \
        echo "Exitcode gt 0"; \
    fi; \
    status_line=$$(tail -2 output.log | head -1 | sed -r "s/\x1B\[[0-9;]*[a-zA-Z]//g"); \
    suites=$$(echo "$$status_line" | sed -n 's/.*suites=\([0-9]*\).*/\1/p'); \
    if [ -z "$$suites" ]; then \
        status_line=$$(tail -1 output.log | sed -r "s/\x1B\[[0-9;]*[a-zA-Z]//g"); \
        suites=$$(echo "$$status_line" | sed -n 's/.*suites=\([0-9]*\).*/\1/p'); \
    fi; \
    tests=$$(echo "$$status_line" | sed -n 's/.*tests=\([0-9]*\).*/\1/p'); \
    failed=$$(echo "$$status_line" | sed -n 's/.*failed=\([0-9]*\).*/\1/p'); \
    printf "\n"; \
    echo "-----------   Results for $1-$2    -----------"; \
    echo "Tests: $$tests, Failed: $$failed"; \
    printf "\n\n============================================================\n"
endef

hive-local:
	@if [ ! -d "temp" ]; then mkdir temp; fi
	docker build -t "test/erigon:$(SHORT_COMMIT)" . 
	rm -rf "temp/hive-local-$(SHORT_COMMIT)" && mkdir "temp/hive-local-$(SHORT_COMMIT)"
	cd "temp/hive-local-$(SHORT_COMMIT)" && git clone https://github.com/erigontech/hive

	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && \
	$(if $(filter Darwin,$(UNAME)), \
		sed -i '' "s/^ARG baseimage=erigontech\/erigon$$/ARG baseimage=test\/erigon/" clients/erigon/Dockerfile && \
		sed -i '' "s/^ARG tag=main-latest$$/ARG tag=$(SHORT_COMMIT)/" clients/erigon/Dockerfile, \
		sed -i "s/^ARG baseimage=erigontech\/erigon$$/ARG baseimage=test\/erigon/" clients/erigon/Dockerfile && \
		sed -i "s/^ARG tag=main-latest$$/ARG tag=$(SHORT_COMMIT)/" clients/erigon/Dockerfile \
	)
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && go build . 2>&1 | tee buildlogs.log 
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && go build ./cmd/hiveview && ./hiveview --serve --logdir ./workspace/logs &
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && $(call run_suite,engine,exchange-capabilities)
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && $(call run_suite,engine,withdrawals)
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && $(call run_suite,engine,cancun)
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && $(call run_suite,engine,api)
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && $(call run_suite,engine,auth)
	cd "temp/hive-local-$(SHORT_COMMIT)/hive" && $(call run_suite,rpc-compat,)

eest-hive:
	@if [ ! -d "temp" ]; then mkdir temp; fi
	docker build -t "test/erigon:$(SHORT_COMMIT)" .
	rm -rf "temp/eest-hive-$(SHORT_COMMIT)" && mkdir "temp/eest-hive-$(SHORT_COMMIT)"
	cd "temp/eest-hive-$(SHORT_COMMIT)" && git clone https://github.com/erigontech/hive
	cd "temp/eest-hive-$(SHORT_COMMIT)/hive" && \
	$(if $(filter Darwin,$(UNAME)), \
		sed -i '' "s/^ARG baseimage=erigontech\/erigon$$/ARG baseimage=test\/erigon/" clients/erigon/Dockerfile && \
		sed -i '' "s/^ARG tag=main-latest$$/ARG tag=$(SHORT_COMMIT)/" clients/erigon/Dockerfile, \
		sed -i "s/^ARG baseimage=erigontech\/erigon$$/ARG baseimage=test\/erigon/" clients/erigon/Dockerfile && \
		sed -i "s/^ARG tag=main-latest$$/ARG tag=$(SHORT_COMMIT)/" clients/erigon/Dockerfile \
	)
	cd "temp/eest-hive-$(SHORT_COMMIT)/hive" && go build . 2>&1 | tee buildlogs.log 
	cd "temp/eest-hive-$(SHORT_COMMIT)/hive" && go build ./cmd/hiveview && ./hiveview --serve --logdir ./workspace/logs &
	cd "temp/eest-hive-$(SHORT_COMMIT)/hive" && $(call run_suite,eest/consume-engine,"",--sim.buildarg branch=hive --sim.buildarg fixtures=https://github.com/ethereum/execution-spec-tests/releases/download/v4.5.0/fixtures_develop.tar.gz)

# define kurtosis assertoor runner
define run-kurtosis-assertoor
	docker build -t test/erigon:current . ; \
	kurtosis enclave rm -f makefile-kurtosis-testnet ; \
	kurtosis run --enclave makefile-kurtosis-testnet github.com/ethpandaops/ethereum-package --args-file $(1) ; \
	printf "\nTo view logs: \nkurtosis service logs makefile-kurtosis-testnet el-1-erigon-lighthouse\n"
endef

check-kurtosis:
	@if ! command -v kurtosis >/dev/null 2>&1; then \
		echo "kurtosis command not found in PATH, please source it in PATH. If Kurtosis is not installed, install it by visiting https://docs.kurtosis.com/install/"; \
		exit 1; \
	fi; \

kurtosis-pectra-assertoor:	check-kurtosis
	@$(call run-kurtosis-assertoor,".github/workflows/kurtosis/pectra.io")

kurtosis-regular-assertoor:	check-kurtosis
	@$(call run-kurtosis-assertoor,".github/workflows/kurtosis/regular-assertoor.io")

kurtosis-fusaka-assertoor: check-kurtosis
	@$(call run-kurtosis-assertoor,".github/workflows/kurtosis/fusaka.io")

kurtosis-cleanup:
	@echo "Currently Running Enclaves: "
	@kurtosis enclave ls
	@echo "-----------------------------------\n"
	kurtosis enclave rm -f makefile-kurtosis-testnet

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

## tidy:                              `go mod tidy`
tidy:
	cd erigon-lib && go mod tidy
	go mod tidy

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
	@cd erigon-lib && $(MAKE) mocks
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

## grpc:                              generate grpc and protobuf code
grpc:
	@cd erigon-lib && $(MAKE) grpc
	@cd txnprovider/shutter && $(MAKE) proto

## stringer:                          generate stringer code
stringer:
	$(GOBUILD) -o $(GOBIN)/stringer golang.org/x/tools/cmd/stringer
	PATH="$(GOBIN):$(PATH)" go generate -run "stringer" ./...

## gen:                               generate all auto-generated code in the codebase
gen: mocks solc abigen gencodec graphql grpc stringer
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
