package misc

import (
	"github.com/ledgerwatch/erigon-lib/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/params"
)

func ApplyBeaconRootEip4788(parentBeaconBlockRoot *libcommon.Hash, syscall consensus.SystemCall, tracer *tracing.Hooks) {
	if tracer != nil && tracer.OnSystemCallStart != nil {
		tracer.OnSystemCallStart()
	}

	if tracer != nil && tracer.OnSystemCallEnd != nil {
		defer tracer.OnSystemCallEnd()
	}

	_, err := syscall(params.BeaconRootsAddress, parentBeaconBlockRoot.Bytes())
	if err != nil {
		log.Warn("Failed to call beacon roots contract", "err", err)
	}
}
