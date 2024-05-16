package misc

import (
	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/params"
)

func ApplyBeaconRootEip4788(parentBeaconBlockRoot *libcommon.Hash, syscall consensus.SystemCall, eLogger *tracing.Hooks) {
	if eLogger != nil && eLogger.OnSystemCallStart != nil {
		eLogger.OnSystemCallStart()
	}

	if eLogger != nil && eLogger.OnSystemCallEnd != nil {
		defer eLogger.OnSystemCallEnd()
	}

	_, err := syscall(params.BeaconRootsAddress, parentBeaconBlockRoot.Bytes())
	if err != nil {
		log.Warn("Failed to call beacon roots contract", "err", err)
	}
}
