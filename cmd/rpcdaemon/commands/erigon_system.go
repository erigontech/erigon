package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/ethdb/prune"
)

// Forks is a data type to record a list of forks passed by this node
type Forks struct {
	GenesisHash common.Hash `json:"genesis"`
	Forks       []uint64    `json:"forks"`
}

// Forks implements erigon_forks. Returns the genesis block hash and a sorted list of all forks block numbers
func (api *ErigonImpl) Forks(ctx context.Context) (Forks, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return Forks{}, err
	}
	defer tx.Rollback()

	chainConfig, genesis, err := api.chainConfigWithGenesis(tx)
	if err != nil {
		return Forks{}, err
	}
	forksBlocks := forkid.GatherForks(chainConfig)

	return Forks{genesis.Hash(), forksBlocks}, nil
}

type PruneInfo struct {
	Name        string  `json:"name"`
	Enabled     bool    `json:"enabled"`
	OlderThan   *uint64 `json:"older_than"`
	BeforeBlock *uint64 `json:"before_block"` // https://github.com/ledgerwatch/erigon/issues/2421
}

func (api *ErigonImpl) PruneInfo(ctx context.Context) ([]PruneInfo, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	pm, err := prune.Get(tx)
	if err != nil {
		return nil, err
	}
	build := func(name string, distance prune.Distance) PruneInfo {
		p := PruneInfo{Name: name, Enabled: distance.Enabled()}
		if distance.Enabled() {
			x := uint64(distance)
			p.OlderThan = &x
		}
		return p
	}
	return []PruneInfo{
		build("history", pm.History),
		build("receipts", pm.Receipts),
		build("tx_index", pm.TxIndex),
		build("call_traces", pm.CallTraces),
	}, nil
}
