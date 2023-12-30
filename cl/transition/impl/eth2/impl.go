package eth2

import "github.com/ledgerwatch/erigon/cl/transition/machine"

type Impl = impl

var _ machine.Interface = (*impl)(nil)

type BlockRewardsCollector struct {
	Attestations      uint64
	AttesterSlashings uint64
	ProposerSlashings uint64
	SyncAggregate     uint64
}

type impl struct {
	FullValidation        bool
	BlockRewardsCollector *BlockRewardsCollector
}
