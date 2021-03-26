package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
)

func TestBlockHashStage(t *testing.T) {
	origin, headers := generateFakeBlocks(1, 4)

	db := ethdb.NewMemDatabase()

	// prepare db so it works with our test
	rawdb.WriteHeaderNumber(db, origin.Hash(), 0)
	if err := rawdb.WriteTd(db, origin.Hash(), 0, origin.Difficulty); err != nil {
		panic(err)
	}
	rawdb.WriteHeader(context.TODO(), db, origin)
	if err := rawdb.WriteHeadHeaderHash(db, origin.Hash()); err != nil {
		t.Fatalf("failed to write head header hash: %v", err)
	}
	if err := stages.SaveStageProgress(db, stages.Headers, origin.Number.Uint64()); err != nil {
		t.Fatalf("setting headers progress: %v", err)
	}
	if err := rawdb.WriteCanonicalHash(db, origin.Hash(), 0); err != nil {
		t.Fatalf("writing canonical hash: %v", err)
	}

	if _, _, _, err := InsertHeaderChain("logPrefix", db, headers); err != nil {
		t.Errorf("inserting header chain: %v", err)
	}
	if err := stages.SaveStageProgress(db, stages.Headers, headers[len(headers)-1].Number.Uint64()); err != nil {
		t.Fatalf("setting headers progress: %v", err)
	}
	err := SpawnBlockHashStage(&StageState{Stage: stages.BlockHashes}, db, "", nil)
	assert.NoError(t, err)
	for _, h := range headers {
		n := rawdb.ReadHeaderNumber(db, h.Hash())
		assert.Equal(t, *n, h.Number.Uint64())
	}

}
