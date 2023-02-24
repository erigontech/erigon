# import .env - create from .env.example if it doesn't exist
ifeq ($(wildcard .env),)
    $(shell cp .env.example .env)
endif

include .env
export

default: all

## go-version:                        print and verify go version
.PHONY: go-version
go-version:
	@if [ $(shell $(GO) version | cut -c 16-17) -lt 18 ]; then \
		echo "minimum required Golang version is 1.18"; \
		exit 1 ;\
	fi

## validate_docker_build_args:        ensure docker build args are valid
.PHONY: validate_docker_build_args
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

## setup_xdg_data_home:              sets up the xdg data home directory for storing data
.PHONY: setup_xdg_data_home
setup_xdg_data_home:
	mkdir -p $(xdg_data_home_subdirs)
	ls -aln $(xdg_data_home) | grep -E "472.*0.*erigon-grafana" || chown -R 472:0 $(xdg_data_home)/erigon-grafana
	@echo "✔️ xdg_data_home setup"
	@ls -al $(xdg_data_home)

## docker-compose:                    validate build args, setup xdg data home, and run docker-compose up
.PHONY: docker-compose
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

## erigon:                            build erigon
erigon: go-version erigon.cmd
	@rm -f $(GOBIN)/tg # Remove old binary to prevent confusion where users still use it because of the scripts

COMMANDS += devnet
COMMANDS += erigon-el-mock
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
COMMANDS += erigon-el

# build each command using %.cmd rule
$(COMMANDS): %: %.cmd

## all:                               run erigon with all commands
all: erigon $(COMMANDS)

## db-tools:                          build db tools
.PHONY: db-tools
db-tools:
	@echo "Building db-tools"

	go mod vendor
	cd vendor/github.com/torquem-ch/mdbx-go && MDBX_BUILD_TIMESTAMP=unknown make tools
	cd vendor/github.com/torquem-ch/mdbx-go/mdbxdist && cp mdbx_chk $(GOBIN) && cp mdbx_copy $(GOBIN) && cp mdbx_dump $(GOBIN) && cp mdbx_drop $(GOBIN) && cp mdbx_load $(GOBIN) && cp mdbx_stat $(GOBIN)
	rm -rf vendor
	@echo "Run \"$(GOBIN)/mdbx_stat -h\" to get info about mdbx db file."

## test:                              run unit tests with a 50s timeout
.PHONY: test
test:
	$(GOTEST) --timeout 50s

## test3:                             run erigon 3 unit tests with a 50s timeout
.PHONY: test3
test3:
	$(GOTEST) --timeout 50s -tags $(BUILD_TAGS),erigon3

## test-integration:                  run integration tests with a 30m timeout
.PHONY: test-integration
test-integration:
	$(GOTEST) --timeout 30m -tags $(BUILD_TAGS),integration

## test3-integration:                 run erigon 3 integration tests with a 30m timeout
.PHONY: test3-integration
test3-integration:
	$(GOTEST) --timeout 30m -tags $(BUILD_TAGS),integration,erigon3

## lint:                              run golangci-lint with .golangci.yml config file
.PHONY: lint
lint:
	@./build/bin/golangci-lint run --config ./.golangci.yml

## lintci:                            run golangci-lint (additionally outputs message before run)
.PHONY: lintci
lintci:
	@echo "--> Running linter for code"
	@./build/bin/golangci-lint run --config ./.golangci.yml

## lintci-deps:                       (re)installs golangci-lint to build/bin/golangci-lint
.PHONY: lintci-deps
lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./build/bin v1.51.1

## clean:                             cleans the go cache, build dir, libmdbx db dir
.PHONY: clean
clean:
	go clean -cache
	rm -fr build/*

# The devtools target installs tools required for 'go generate'.
# You need to put $GOBIN (or $GOPATH/bin) in your PATH to use 'go generate'.

## devtools:                          installs dev tools (and checks for npm installation etc.)
.PHONY: devtools
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
.PHONY: bindings
bindings:
	PATH=$(GOBIN):$(PATH) go generate ./tests/contracts/
	PATH=$(GOBIN):$(PATH) go generate ./core/state/contracts/

## prometheus:                        run prometheus and grafana with docker-composei
.PHONY: prometheus
prometheus:
	docker-compose up prometheus grafana

## escape:                            run escape path={path} to check for memory leaks e.g. run escape path=cmd/erigon
.PHONY: escape
escape:
	cd $(path) && go test -gcflags "-m -m" -run none -bench=BenchmarkJumpdest* -benchmem -memprofile mem.out

## git-submodules:                    update git submodules
.PHONY: git-submodules
git-submodules:
	@[ -d ".git" ] || (echo "Not a git repository" && exit 1)
	@echo "Updating git submodules"
	@# Dockerhub using ./hooks/post-checkout to set submodules, so this line will fail on Dockerhub
	@# these lines will also fail if ran as root in a non-root user's checked out repository
	@git submodule sync --quiet --recursive || true
	@git submodule update --quiet --init --recursive --force || true

## release-dry-run:                   run goreleaser in dry-run mode (does not release!)
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
		--clean --skip-validate --skip-publish

## release:                           run goreleaser (WARNING: will create a release)
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
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		--clean --skip-validate

	@docker image push --all-tags thorax/erigon
	@docker image push --all-tags ghcr.io/ledgerwatch/erigon

## user_linux:                        create "erigon" user (Linux)
.PHONY : user_linux
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
.PHONY: user_macos
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

## automated-tests                    run automated tests (BUILD_ERIGON=0 to prevent erigon build with local image tag)
.PHONY: automated-tests
automated-tests:
	./tests/automated-testing/run.sh

## help:                              print commands help
.PHONY: help
help	:	Makefile
	@sed -n 's/^##//p' $<
