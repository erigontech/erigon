package migrations

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/stretchr/testify/assert"
)

func generateFakeBlocks(from, to int) (*types.Header, []*types.Header) {
	parentHash := types.EmptyRootHash
	origin := &types.Header{
		ParentHash: parentHash,
		UncleHash:  types.EmptyUncleHash,
		Root:       types.EmptyRootHash,
		Difficulty: big.NewInt(int64(0)),
		Number:     big.NewInt(int64(0)),
		GasLimit:   6000,
	}
	parentHash = origin.Hash()
	parent := origin
	result := make([]*types.Header, 0)
	for i := 1; i <= to; i++ {
		time := uint64(i)
		difficulty := ethash.CalcDifficulty(
			params.AllEthashProtocolChanges,
			time,
			parent,
		)

		header := &types.Header{
			ParentHash: parentHash,
			UncleHash:  types.EmptyUncleHash,
			Root:       types.EmptyRootHash,
			Difficulty: difficulty,
			Number:     big.NewInt(int64(i)),
			GasLimit:   6000,
			Time:       time,
		}
		parentHash = header.Hash()
		parent = header
		if i >= from {
			result = append(result, header)
		}
	}
	return origin, result
}
func TestStagedsyncToUseStageBlockhashes(t *testing.T) {
	origin, headers := generateFakeBlocks(1, 4)

	db := ethdb.NewMemDatabase()

	// prepare db so it works with our test
	rawdb.WriteHeaderNumber(db, origin.Hash(), 0)
	rawdb.WriteTd(db, origin.Hash(), 0, origin.Difficulty)
	rawdb.WriteHeader(context.TODO(), db, origin)
	rawdb.WriteHeadHeaderHash(db, origin.Hash())
	rawdb.WriteCanonicalHash(db, origin.Hash(), 0)

	_, _, err := stagedsync.InsertHeaderChain(db, headers, params.AllEthashProtocolChanges, ethash.NewFaker(), 0)
	assert.NoError(t, err)

	for _, h := range headers {
		fmt.Println(h.Hash())
		rawdb.WriteHeaderNumber(db, h.Hash(), h.Number.Uint64())
	}
	migrator := NewMigrator()
	migrator.Migrations = []Migration{stagedsyncToUseStageBlockhashes}
	err = migrator.Apply(db, "")

	_, err = db.Get(dbutils.SyncStageProgress, stages.DBKeys[stages.BlockHashes])
	assert.NoError(t, err)
}
