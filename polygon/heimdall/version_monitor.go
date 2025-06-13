package heimdall

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
)

type HeimdallVersion int64

const (
	HeimdallV1 HeimdallVersion = iota
	HeimdallV2
)

type heimdallClient interface {
	FetchStatus(ctx context.Context) (*Status, error)
}

type versionMonitor struct {
	ctx            context.Context
	currentVersion HeimdallVersion

	heimdall heimdallClient
	logger   log.Logger

	frequency time.Duration
	m         sync.Mutex
	once      sync.Once
}

func NewVersionMonitor(ctx context.Context, heimdallClient heimdallClient, logger log.Logger, frequency time.Duration) *versionMonitor {
	return &versionMonitor{
		ctx:            ctx,
		heimdall:       heimdallClient,
		currentVersion: HeimdallV1,
		frequency:      frequency,
		logger:         logger,
	}
}

func (vm *versionMonitor) Run() {
	checkEvery := time.NewTicker(vm.frequency)
	defer checkEvery.Stop()

	for {
		select {
		case <-checkEvery.C:
			vm.resolveVersion()
			vm.once.Do(func() {}) // no need to resolve version in Version() function anymore
		case <-vm.ctx.Done():
			return
		}
	}
}

func (vm *versionMonitor) Version() HeimdallVersion {
	vm.once.Do(vm.resolveVersion)

	vm.m.Lock()
	defer vm.m.Unlock()

	return vm.currentVersion
}

func (vm *versionMonitor) resolveVersion() {
	status, err := vm.heimdall.FetchStatus(vm.ctx)
	if err != nil {
		vm.logger.Error("Failed attempt to resolve heimdall version", "err", err)
		return
	}

	// Current Heimdall API does not provide a good way to detect if it is HeimdallV1 or Heimdall V2. The next
	// code is what bor client does as well. Probably we d'like to raise a question with them about better endpoints.
	version := status.NodeInfo.Version
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		vm.logger.Error("Failed resolve version: unexpected version format", "version", version)
		return
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		vm.logger.Error("Failed resolve version: unexpected version format", "version", version)
		return
	}

	vm.m.Lock()
	defer vm.m.Unlock()

	// We should monitor upgrade and downgrade both, because it is a valid scenario
	if minor >= 38 {
		vm.currentVersion = HeimdallV2
	} else {
		vm.currentVersion = HeimdallV1
	}
}
