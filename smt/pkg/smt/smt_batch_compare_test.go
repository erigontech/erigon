package smt_test

import (
	"context"
	"os"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"gotest.tools/v3/assert"
)

func TestCompareAllTreesInsertTimesAndFinalHashesUsingDiskDb(t *testing.T) {
	incrementalDbPath := "/tmp/smt-incremental"
	smtIncrementalDb, smtIncrementalTx, smtIncrementalSmtDb := initDb(t, incrementalDbPath)

	bulkDbPath := "/tmp/smt-bulk"
	smtBulkDb, smtBulkTx, smtBulkSmtDb := initDb(t, bulkDbPath)

	batchDbPath := "/tmp/smt-batch"
	smtBatchDb, smtBatchTx, smtBatchSmtDb := initDb(t, batchDbPath)

	smtIncremental := smt.NewSMT(smtIncrementalSmtDb, false)
	smtBulk := smt.NewSMT(smtBulkSmtDb, false)
	smtBatch := smt.NewSMT(smtBatchSmtDb, false)

	compareAllTreesInsertTimesAndFinalHashes(t, smtIncremental, smtBulk, smtBatch)

	smtIncrementalTx.Commit()
	smtBulkTx.Commit()
	smtBatchTx.Commit()
	t.Cleanup(func() {
		smtIncrementalDb.Close()
		smtBulkDb.Close()
		smtBatchDb.Close()
		os.RemoveAll(incrementalDbPath)
		os.RemoveAll(bulkDbPath)
		os.RemoveAll(batchDbPath)
	})
}

func TestCompareAllTreesInsertTimesAndFinalHashesUsingInMemoryDb(t *testing.T) {
	smtIncremental := smt.NewSMT(nil, false)
	smtBulk := smt.NewSMT(nil, false)
	smtBatch := smt.NewSMT(nil, false)

	compareAllTreesInsertTimesAndFinalHashes(t, smtIncremental, smtBulk, smtBatch)
}

func compareAllTreesInsertTimesAndFinalHashes(t *testing.T, smtIncremental, smtBulk, smtBatch *smt.SMT) {
	batchInsertDataHolders, totalInserts := prepareData()
	ctx := context.Background()
	var incrementalError error

	accChanges := make(map[libcommon.Address]*accounts.Account)
	codeChanges := make(map[libcommon.Address]string)
	storageChanges := make(map[libcommon.Address]map[string]string)

	for _, batchInsertDataHolder := range batchInsertDataHolders {
		accChanges[batchInsertDataHolder.AddressAccount] = &batchInsertDataHolder.acc
		codeChanges[batchInsertDataHolder.AddressContract] = batchInsertDataHolder.Bytecode
		storageChanges[batchInsertDataHolder.AddressContract] = batchInsertDataHolder.Storage
	}

	startTime := time.Now()
	for addr, acc := range accChanges {
		if err := smtIncremental.SetAccountStorage(addr, acc); err != nil {
			incrementalError = err
		}
	}

	for addr, code := range codeChanges {
		if err := smtIncremental.SetContractBytecode(addr.String(), code); err != nil {
			incrementalError = err
		}
	}

	for addr, storage := range storageChanges {
		if _, err := smtIncremental.SetContractStorage(addr.String(), storage, nil); err != nil {
			incrementalError = err
		}
	}

	assert.NilError(t, incrementalError)
	t.Logf("Incremental insert %d values in %v\n", totalInserts, time.Since(startTime))

	startTime = time.Now()
	keyPointers, valuePointers, err := smtBatch.SetStorage(ctx, "", accChanges, codeChanges, storageChanges)
	assert.NilError(t, err)
	t.Logf("Batch insert %d values in %v\n", totalInserts, time.Since(startTime))

	keys := []utils.NodeKey{}
	for i, key := range keyPointers {
		v := valuePointers[i]
		if !v.IsZero() {
			smtBulk.Db.InsertAccountValue(*key, *v)
			keys = append(keys, *key)
		}
	}
	startTime = time.Now()
	smtBulk.GenerateFromKVBulk(ctx, "", keys)
	t.Logf("Bulk insert %d values in %v\n", totalInserts, time.Since(startTime))

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	smtBulkRootHash, _ := smtBulk.Db.GetLastRoot()
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))
	assert.Equal(t, utils.ConvertBigIntToHex(smtBulkRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))

	assertSmtDbStructure(t, smtBatch, true)
}
