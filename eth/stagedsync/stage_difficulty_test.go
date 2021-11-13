package stagedsync

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/stretchr/testify/assert"
)

func TestDifficultyComputation(t *testing.T) {
	// We need a Database with the following requirements:
	// 3 Headers
	// 3 Canonical Hashes
	ctx, assert := context.Background(), assert.New(t)
	db := memdb.New()
	tx, _ := db.BeginRw(ctx)
	// Create the 3 headers, body is irrelevant we just need to have difficulty
	var header1, header2, header3 types.Header
	// First header
	header1.Difficulty = big.NewInt(10)
	header1.Number = big.NewInt(1)
	// Second Header
	header2.Difficulty = big.NewInt(30)
	header2.Number = big.NewInt(2)
	// Third Header
	header3.Difficulty = big.NewInt(314)
	header3.Number = big.NewInt(3)
	// Insert the headers into the db
	rawdb.WriteHeader(tx, &header1)
	rawdb.WriteHeader(tx, &header2)
	rawdb.WriteHeader(tx, &header3)
	// Canonical hashes
	rawdb.WriteCanonicalHash(tx, header1.Hash(), header1.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header2.Hash(), header2.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header3.Hash(), header3.Number.Uint64())
	// save progress for headers
	_ = stages.SaveStageProgress(tx, stages.Headers, 3)
	// Code
	err := SpawnDifficultyStage(&StageState{BlockNumber: 0, ID: stages.TotalDifficulty}, tx, StageDifficultyCfg(db, "", common.Big0), ctx)
	assert.NoError(err)
	// Asserts
	actual_td, err := rawdb.ReadTd(tx, header1.Hash(), header1.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(10), actual_td.Uint64(), "Wrong total difficulty")

	actual_td, err = rawdb.ReadTd(tx, header2.Hash(), header2.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(40), actual_td.Uint64(), "Wrong total difficulty")

	actual_td, err = rawdb.ReadTd(tx, header3.Hash(), header3.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(354), actual_td.Uint64(), "Wrong total difficulty")
}

func TestDifficultyComputationNonCanonical(t *testing.T) {
	// We need a Database with the following requirements:
	// 3 Headers
	// 3 Canonical Hashes
	ctx, assert := context.Background(), assert.New(t)
	db := memdb.New()
	tx, _ := db.BeginRw(ctx)

	// Create the 3 headers, body is irrelevant we just need to have difficulty
	var header1, header2, noncanonicalHeader2, header3 types.Header
	// First header
	header1.Difficulty = big.NewInt(10)
	header1.Number = big.NewInt(1)
	// Second Header
	header2.Difficulty = big.NewInt(30)
	header2.Number = big.NewInt(2)
	noncanonicalHeader2.Difficulty = big.NewInt(50)
	noncanonicalHeader2.Number = big.NewInt(2)
	// Third Header
	header3.Difficulty = big.NewInt(314)
	header3.Number = big.NewInt(3)
	// Insert the headers into the db
	rawdb.WriteHeader(tx, &header1)
	rawdb.WriteHeader(tx, &header2)
	rawdb.WriteHeader(tx, &noncanonicalHeader2)
	rawdb.WriteHeader(tx, &header3)
	// Canonical hashes
	rawdb.WriteCanonicalHash(tx, header1.Hash(), header1.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header2.Hash(), header2.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header3.Hash(), header3.Number.Uint64())
	// save progress for headers
	_ = stages.SaveStageProgress(tx, stages.Headers, 3)
	// Code
	err := SpawnDifficultyStage(&StageState{BlockNumber: 0, ID: stages.TotalDifficulty}, tx, StageDifficultyCfg(db, "", common.Big0), ctx)
	assert.NoError(err)
	// Asserts
	actual_td, err := rawdb.ReadTd(tx, header1.Hash(), header1.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(10), actual_td.Uint64(), "Wrong total difficulty")

	actual_td, err = rawdb.ReadTd(tx, header2.Hash(), header2.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(40), actual_td.Uint64(), "Wrong total difficulty")

	actual_td, err = rawdb.ReadTd(tx, header3.Hash(), header3.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(354), actual_td.Uint64(), "Wrong total difficulty")
}

func TestDifficultyProgress(t *testing.T) {
	// We need a Database with the following requirements:
	// 3 Headers
	// 3 Canonical Hashes
	ctx, assert := context.Background(), assert.New(t)
	db := memdb.New()
	tx, _ := db.BeginRw(ctx)
	// Create the 3 headers, body is irrelevant we just need to have difficulty
	var header1, header2, noncanonicalHeader2, header3 types.Header
	// First header
	header1.Difficulty = big.NewInt(10)
	header1.Number = big.NewInt(1)
	// Second Header
	header2.Difficulty = big.NewInt(30)
	header2.Number = big.NewInt(2)
	noncanonicalHeader2.Difficulty = big.NewInt(50)
	noncanonicalHeader2.Number = big.NewInt(2)
	// Third Header
	header3.Difficulty = big.NewInt(314)
	header3.Number = big.NewInt(3)
	// Insert the headers into the db
	rawdb.WriteHeader(tx, &header1)
	_ = rawdb.WriteTd(tx, header1.Hash(), header1.Number.Uint64(), big.NewInt(10))
	rawdb.WriteHeader(tx, &header2)
	rawdb.WriteHeader(tx, &noncanonicalHeader2)
	rawdb.WriteHeader(tx, &header3)
	// Canonical hashes
	rawdb.WriteCanonicalHash(tx, header1.Hash(), header1.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header2.Hash(), header2.Number.Uint64())
	rawdb.WriteCanonicalHash(tx, header3.Hash(), header3.Number.Uint64())
	// save progress for headers
	_ = stages.SaveStageProgress(tx, stages.Headers, 3)
	_ = stages.SaveStageProgress(tx, stages.TotalDifficulty, 1)
	// Code
	err := SpawnDifficultyStage(&StageState{BlockNumber: 0, ID: stages.TotalDifficulty}, tx, StageDifficultyCfg(db, "", common.Big0), ctx)
	assert.NoError(err)
	// Asserts
	actual_td, err := rawdb.ReadTd(tx, header1.Hash(), header1.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(10), actual_td.Uint64(), "Wrong total difficulty")

	actual_td, err = rawdb.ReadTd(tx, header2.Hash(), header2.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(40), actual_td.Uint64(), "Wrong total difficulty")

	actual_td, err = rawdb.ReadTd(tx, header3.Hash(), header3.Number.Uint64())
	assert.NoError(err)
	assert.Equalf(uint64(354), actual_td.Uint64(), "Wrong total difficulty")
}
