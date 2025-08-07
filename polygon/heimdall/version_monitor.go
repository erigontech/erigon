package heimdall

import (
	"context"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/poshttp"
)

type heimdallClient interface {
	FetchChainManagerStatus(ctx context.Context) (*poshttp.ChainManagerStatus, error)
}

type versionMonitor struct {
	ctx            context.Context
	currentVersion poshttp.HeimdallVersion

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
		currentVersion: poshttp.HeimdallV1,
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

func (vm *versionMonitor) Version() poshttp.HeimdallVersion {
	vm.once.Do(vm.resolveVersion)

	vm.m.Lock()
	defer vm.m.Unlock()

	return vm.currentVersion
}

func (vm *versionMonitor) resolveVersion() {
	status, err := vm.heimdall.FetchChainManagerStatus(vm.ctx)
	if err != nil {
		vm.logger.Error("Failed attempt to resolve heimdall version", "err", err)
		return
	}

	vm.m.Lock()
	defer vm.m.Unlock()

	// We should monitor upgrade and downgrade both, because it is a valid scenario
	if status.Params.ChainParams.PolTokenAddress != nil {
		vm.currentVersion = poshttp.HeimdallV2
	} else {
		vm.currentVersion = poshttp.HeimdallV1
	}
}
