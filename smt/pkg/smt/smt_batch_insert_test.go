package smt_test

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"gotest.tools/v3/assert"
)

func TestBatchSimpleInsert(t *testing.T) {
	keysRaw := []*big.Int{
		big.NewInt(8),
		big.NewInt(8),
		big.NewInt(1),
		big.NewInt(31),
		big.NewInt(31),
		big.NewInt(0),
		big.NewInt(8),
	}
	valuesRaw := []*big.Int{
		big.NewInt(17),
		big.NewInt(18),
		big.NewInt(19),
		big.NewInt(20),
		big.NewInt(0),
		big.NewInt(0),
		big.NewInt(0),
	}

	keyPointers := []*utils.NodeKey{}
	valuePointers := []*utils.NodeValue8{}

	smtIncremental := smt.NewSMT(nil, false)
	smtBatch := smt.NewSMT(nil, false)
	smtBatchNoSave := smt.NewSMT(nil, true)

	for i := range keysRaw {
		k := utils.ScalarToNodeKey(keysRaw[i])
		vArray := utils.ScalarToArrayBig(valuesRaw[i])
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		keyPointers = append(keyPointers, &k)
		valuePointers = append(valuePointers, v)

		smtIncremental.InsertKA(k, valuesRaw[i])
	}

	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "", false)
	_, err := smtBatch.InsertBatch(insertBatchCfg, keyPointers, valuePointers, nil, nil)
	assert.NilError(t, err)

	_, err = smtBatchNoSave.InsertBatch(insertBatchCfg, keyPointers, valuePointers, nil, nil)
	assert.NilError(t, err)

	smtIncremental.DumpTree()
	fmt.Println()
	smtBatch.DumpTree()
	fmt.Println()
	fmt.Println()
	fmt.Println()

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	smtBatchNoSaveRootHash, _ := smtBatchNoSave.Db.GetLastRoot()
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtBatchNoSaveRootHash))

	assertSmtDbStructure(t, smtBatch, false)
}

func TestBatchRawInsert(t *testing.T) {
	keysForBatch := []*utils.NodeKey{}
	valuesForBatch := []*utils.NodeValue8{}

	keysForIncremental := []utils.NodeKey{}
	valuesForIncremental := []utils.NodeValue8{}

	smtIncremental := smt.NewSMT(nil, false)
	smtBatch := smt.NewSMT(nil, false)

	rand.Seed(1)
	size := 1 << 10
	for i := 0; i < size; i++ {
		rawKey := big.NewInt(rand.Int63())
		rawValue := big.NewInt(rand.Int63())

		k := utils.ScalarToNodeKey(rawKey)
		vArray := utils.ScalarToArrayBig(rawValue)
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		keysForBatch = append(keysForBatch, &k)
		valuesForBatch = append(valuesForBatch, v)

		keysForIncremental = append(keysForIncremental, k)
		valuesForIncremental = append(valuesForIncremental, *v)

	}

	startTime := time.Now()
	for i := range keysForIncremental {
		smtIncremental.Insert(keysForIncremental[i], valuesForIncremental[i])
	}
	t.Logf("Incremental insert %d values in %v\n", len(keysForIncremental), time.Since(startTime))

	startTime = time.Now()

	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "", true)
	_, err := smtBatch.InsertBatch(insertBatchCfg, keysForBatch, valuesForBatch, nil, nil)
	assert.NilError(t, err)
	t.Logf("Batch insert %d values in %v\n", len(keysForBatch), time.Since(startTime))

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))

	assertSmtDbStructure(t, smtBatch, false)

	// DELETE
	keysForBatchDelete := []*utils.NodeKey{}
	valuesForBatchDelete := []*utils.NodeValue8{}

	keysForIncrementalDelete := []utils.NodeKey{}
	valuesForIncrementalDelete := []utils.NodeValue8{}

	sizeToDelete := 1 << 14
	for i := 0; i < sizeToDelete; i++ {
		rawValue := big.NewInt(0)
		vArray := utils.ScalarToArrayBig(rawValue)
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		deleteIndex := rand.Intn(size)

		keyForBatchDelete := keysForBatch[deleteIndex]
		keyForIncrementalDelete := keysForIncremental[deleteIndex]

		keysForBatchDelete = append(keysForBatchDelete, keyForBatchDelete)
		valuesForBatchDelete = append(valuesForBatchDelete, v)

		keysForIncrementalDelete = append(keysForIncrementalDelete, keyForIncrementalDelete)
		valuesForIncrementalDelete = append(valuesForIncrementalDelete, *v)
	}

	startTime = time.Now()
	for i := range keysForIncrementalDelete {
		smtIncremental.Insert(keysForIncrementalDelete[i], valuesForIncrementalDelete[i])
	}
	t.Logf("Incremental delete %d values in %v\n", len(keysForIncrementalDelete), time.Since(startTime))

	startTime = time.Now()

	_, err = smtBatch.InsertBatch(insertBatchCfg, keysForBatchDelete, valuesForBatchDelete, nil, nil)
	assert.NilError(t, err)
	t.Logf("Batch delete %d values in %v\n", len(keysForBatchDelete), time.Since(startTime))

	assertSmtDbStructure(t, smtBatch, false)
}

func BenchmarkIncrementalInsert(b *testing.B) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smtIncremental := smt.NewSMT(nil, false)
		incrementalInsert(smtIncremental, keys, vals)
	}
}

func BenchmarkBatchInsert(b *testing.B) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smtBatch := smt.NewSMT(nil, false)
		batchInsert(smtBatch, keys, vals)
	}
}

func BenchmarkBatchInsertNoSave(b *testing.B) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smtBatch := smt.NewSMT(nil, true)
		batchInsert(smtBatch, keys, vals)
	}
}

func TestBatchSimpleInsert2(t *testing.T) {
	keys := []*big.Int{}
	vals := []*big.Int{}
	for i := 0; i < 1000; i++ {
		rand.Seed(time.Now().UnixNano())
		keys = append(keys, big.NewInt(int64(rand.Intn(10000))))

		rand.Seed(time.Now().UnixNano())
		vals = append(vals, big.NewInt(int64(rand.Intn(10000))))
	}

	smtIncremental := smt.NewSMT(nil, false)
	incrementalInsert(smtIncremental, keys, vals)

	smtBatch := smt.NewSMT(nil, false)
	batchInsert(smtBatch, keys, vals)

	smtBatchNoSave := smt.NewSMT(nil, false)
	batchInsert(smtBatchNoSave, keys, vals)

	smtIncrementalRootHash, _ := smtIncremental.Db.GetLastRoot()
	smtBatchRootHash, _ := smtBatch.Db.GetLastRoot()
	smtBatchNoSaveRootHash, _ := smtBatchNoSave.Db.GetLastRoot()

	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtIncrementalRootHash))
	assert.Equal(t, utils.ConvertBigIntToHex(smtBatchRootHash), utils.ConvertBigIntToHex(smtBatchNoSaveRootHash))
}

func incrementalInsert(tree *smt.SMT, key, val []*big.Int) {
	for i := range key {
		k := utils.ScalarToNodeKey(key[i])
		tree.InsertKA(k, val[i])
	}
}

func batchInsert(tree *smt.SMT, key, val []*big.Int) {
	keyPointers := []*utils.NodeKey{}
	valuePointers := []*utils.NodeValue8{}

	for i := range key {
		k := utils.ScalarToNodeKey(key[i])
		vArray := utils.ScalarToArrayBig(val[i])
		v, _ := utils.NodeValue8FromBigIntArray(vArray)

		keyPointers = append(keyPointers, &k)
		valuePointers = append(valuePointers, v)
	}
	insertBatchCfg := smt.NewInsertBatchConfig(context.Background(), "", false)
	tree.InsertBatch(insertBatchCfg, keyPointers, valuePointers, nil, nil)
}
