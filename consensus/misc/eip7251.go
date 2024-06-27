package misc

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func DequeueConsolidationRequests7251(syscall consensus.SystemCall) types.Requests {
	res, err := syscall(params.ConsolidationRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to ConsolidationRequestAddress", "err", err)
		return nil
	}
	// Parse out the consolidations - using the bytes array returned
	var reqs types.Requests
	lenPerReq := 20 + 48 + 48 // addr + sourcePubkey + targetPubkey
	for i := 0; i <= len(res)-lenPerReq; i += lenPerReq {
		var sourcePubKey [48]byte
		copy(sourcePubKey[:], res[i+20:i+68])
		var targetPubKey [48]byte
		copy(targetPubKey[:], res[i+68:i+116])
		wr := &types.ConsolidationRequest{
			SourceAddress: common.BytesToAddress(res[i : i+20]),
			SourcePubKey:  sourcePubKey,
			TargetPubKey:  targetPubKey,
		}
		reqs = append(reqs, wr)
	}
	return reqs
}
