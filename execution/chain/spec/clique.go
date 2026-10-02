package chainspec

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
)

var (
	AllCliqueProtocolChanges = &chain.Config{
		ChainID:               uint256.NewInt(1337),
		Rules:                 chain.CliqueRules,
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		MuirGlacierBlock:      common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		LondonBlock:           common.NewUint64(0),
		Clique:                &chain.CliqueConfig{Period: 0, Epoch: 30000},
	}

	CliqueSnapshot = &ConsensusSnapshotConfig{
		CheckpointInterval: 10,
		InmemorySnapshots:  1024,
		InmemorySignatures: 16384,
	}
)

type ConsensusSnapshotConfig struct {
	CheckpointInterval uint64 // Number of blocks after which to save the vote snapshot to the database
	InmemorySnapshots  int    // Number of recent vote snapshots to keep in memory
	InmemorySignatures int    // Number of recent block signatures to keep in memory
}
