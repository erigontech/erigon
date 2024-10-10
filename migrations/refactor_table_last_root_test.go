package migrations

import (
	"context"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	smtdb "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func TestRefactorTableLastRoot(t *testing.T) {
	require, tmpDir, db := require.New(t), t.TempDir(), memdb.NewTestDB(t)

	randomRootHash := "0xbb0ed7f7111626844cb6dfbc7c82877eb298cb7b3b3271cf0760bdbc93564531"
	oldBucketName := "HermezSmtLastRoot"
	lastRootKey := []byte("lastRoot")

	err := prepareDb(db, randomRootHash, oldBucketName, lastRootKey)
	require.NoError(err)

	migrator := NewMigrator(kv.ChainDB)
	migrator.Migrations = []Migration{refactorTableLastRoot}
	err = migrator.Apply(db, tmpDir)
	require.NoError(err)

	assertDb(t, db, randomRootHash, lastRootKey)
}

func prepareDb(db kv.RwDB, randomRootHash, oldBucketName string, lastRootKey []byte) error {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.CreateBucket(oldBucketName)
	if err != nil {
		return err
	}

	err = tx.Put(oldBucketName, lastRootKey, []byte(randomRootHash))
	if err != nil {
		return err
	}

	return tx.Commit()
}

func assertDb(t *testing.T, db kv.RwDB, randomRootHash string, lastRootKey []byte) error {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	data, err := tx.GetOne(smtdb.TableStats, lastRootKey)
	if err != nil {
		return err
	}

	rootHash := string(data)

	assert.Equal(t, rootHash, randomRootHash)

	return tx.Commit()
}
