package types

import (
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
)

type Debug struct {
	Message string
}

func ProcessDebug(debug *datastream.Debug) Debug {
	result := Debug{}

	if debug != nil {
		result.Message = debug.Message
	}

	return result
}
