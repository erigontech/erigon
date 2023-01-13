package stagedsync

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

func TestIssuanceStage(t *testing.T) {
	ctx, assert := context.Background(), assert.New(t)
	db, tx := memdb.NewTestTx(t)

	header1 := &types.Header{
		BaseFee: big.NewInt(10),
		GasUsed: 1000,
		Number:  big.NewInt(1),
	}

	header2 := &types.Header{
		BaseFee: big.NewInt(30),
		GasUsed: 1000,
		Number:  big.NewInt(2),
	}

	header3 := &types.Header{
		BaseFee: big.NewInt(100),
		GasUsed: 1000,
		Number:  big.NewInt(3),
	}
	// Write Headers
	rawdb.WriteHeader(tx, header1)
	rawdb.WriteHeader(tx, header2)
	rawdb.WriteHeader(tx, header3)

	stages.SaveStageProgress(tx, stages.Bodies, 3)
	// Canonicals
	rawdb.WriteCanonicalHash(tx, header1.Hash(), header1.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header2.Hash(), header2.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header3.Hash(), header3.Number.Uint64())
	// Execute stage issuance
	err := SpawnStageIssuance(StageIssuanceCfg(db, &chain.Config{
		Consensus: chain.EtHashConsensus,
	}, snapshotsync.NewBlockReader(), true), &StageState{
		ID: stages.Issuance,
	}, tx, ctx)
	assert.NoError(err)

	tb, err := rawdb.ReadTotalBurnt(tx, 3)
	assert.NoError(err)
	assert.Equal(tb, big.NewInt(140000))

	ti, err := rawdb.ReadTotalIssued(tx, 3)
	assert.NoError(err)
	assert.Equal(ti, big.NewInt(900000000000000000))
}
