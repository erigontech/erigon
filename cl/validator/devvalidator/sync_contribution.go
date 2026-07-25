package devvalidator

import (
	"encoding/binary"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// isSyncCommitteeAggregator reports whether the selection proof selects the
// validator as a sync committee contribution aggregator for its subcommittee.
func isSyncCommitteeAggregator(cfg *clparams.BeaconChainConfig, selectionProof common.Bytes96) bool {
	modulo := max(1, cfg.SyncCommitteeSize/cfg.SyncCommitteeSubnetCount/cfg.TargetAggregatorsPerSyncSubcommittee)
	hash := crypto.Sha256(selectionProof[:])
	return binary.LittleEndian.Uint64(hash[:8])%modulo == 0
}

// buildContribution constructs a single-participant sync committee contribution
// for the validator's position within a subcommittee.
func buildContribution(
	slot uint64,
	blockRoot common.Hash,
	subcommitteeIndex, bitIndex uint64,
	sig common.Bytes96,
	cfg *clparams.BeaconChainConfig,
) *cltypes.Contribution {
	aggBits := make([]byte, cfg.SyncCommitteeAggregationBitsSize())
	aggBits[bitIndex/8] |= 1 << (bitIndex % 8)
	return &cltypes.Contribution{
		Slot:              slot,
		BeaconBlockRoot:   blockRoot,
		SubcommitteeIndex: subcommitteeIndex,
		AggregationBits:   aggBits,
		Signature:         sig,
	}
}
