package poshttp

import (
	"context"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
)

type HeimdallVersion int64

const (
	HeimdallV1 HeimdallVersion = iota
	HeimdallV2
)

type apiVersioner interface {
	Version() HeimdallVersion
}

type ChainManagerStatus struct {
	Params struct {
		ChainParams struct {
			PolTokenAddress *string `json:"pol_token_address,omitempty"`
		} `json:"chain_params"`
	} `json:"params"`
}

//go:generate mockgen -typed=true -source=./version_monitor.go -destination=./heimdall_client_mock.go -package=poshttp heimdallClient
type heimdallClient interface {
	FetchChainManagerStatus(ctx context.Context) (*ChainManagerStatus, error)
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
	status, err := vm.heimdall.FetchChainManagerStatus(vm.ctx)
	if err != nil {
		vm.logger.Error("Failed attempt to resolve heimdall version", "err", err)
		return
	}

	vm.m.Lock()
	defer vm.m.Unlock()

	// We should monitor upgrade and downgrade both, because it is a valid scenario
	if status.Params.ChainParams.PolTokenAddress != nil {
		vm.currentVersion = HeimdallV2
	} else {
		vm.currentVersion = HeimdallV1
	}
}
