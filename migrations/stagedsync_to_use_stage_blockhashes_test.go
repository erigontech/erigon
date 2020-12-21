package migrations

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStagedsyncToUseStageBlockhashes(t *testing.T) {

	require, db := require.New(t), ethdb.NewMemDatabase()
	var expected uint64 = 12

	err := stages.SaveStageProgress(db, stages.Headers, expected)
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{stagedsyncToUseStageBlockhashes}
	err = migrator.Apply(db, "")
	require.NoError(err)

	actual, err := stages.GetStageProgress(db, stages.BlockHashes)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestUnwindStagedsyncToUseStageBlockhashes(t *testing.T) {

	require, db := require.New(t), ethdb.NewMemDatabase()
	var expected uint64 = 12

	err := stages.SaveStageUnwind(db, stages.Headers, expected)
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{unwindStagedsyncToUseStageBlockhashes}
	err = migrator.Apply(db, "")
	require.NoError(err)

	actual, err := stages.GetStageUnwind(db, stages.BlockHashes)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
