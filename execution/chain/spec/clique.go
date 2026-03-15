package chainspec

import (
	"math/big"
	"path"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/paths"
)

var (
	// AllCliqueProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the Clique consensus.
	AllCliqueProtocolChanges = &chain.Config{
		ChainID:               big.NewInt(1337),
		Rules:                 chain.CliqueRules,
		HomesteadBlock:        chain.NewUint64(0),
		TangerineWhistleBlock: chain.NewUint64(0),
		SpuriousDragonBlock:   chain.NewUint64(0),
		ByzantiumBlock:        chain.NewUint64(0),
		ConstantinopleBlock:   chain.NewUint64(0),
		PetersburgBlock:       chain.NewUint64(0),
		IstanbulBlock:         chain.NewUint64(0),
		MuirGlacierBlock:      chain.NewUint64(0),
		BerlinBlock:           chain.NewUint64(0),
		LondonBlock:           chain.NewUint64(0),
		Clique:                &chain.CliqueConfig{Period: 0, Epoch: 30000},
	}

	CliqueSnapshot = NewConsensusSnapshotConfig(10, 1024, 16384, true, "")
)

type ConsensusSnapshotConfig struct {
	CheckpointInterval uint64 // Number of blocks after which to save the vote snapshot to the database
	InmemorySnapshots  int    // Number of recent vote snapshots to keep in memory
	InmemorySignatures int    // Number of recent block signatures to keep in memory
	DBPath             string
	InMemory           bool
}

const cliquePath = "clique"

func NewConsensusSnapshotConfig(checkpointInterval uint64, inmemorySnapshots int, inmemorySignatures int, inmemory bool, dbPath string) *ConsensusSnapshotConfig {
	if len(dbPath) == 0 {
		dbPath = paths.DefaultDataDir()
	}

	return &ConsensusSnapshotConfig{
		checkpointInterval,
		inmemorySnapshots,
		inmemorySignatures,
		path.Join(dbPath, cliquePath),
		inmemory,
	}
}
