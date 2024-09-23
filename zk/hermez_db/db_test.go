package hermez_db

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type IHermezDb interface {
	WriteSequence(uint64, uint64, common.Hash, common.Hash) error
	WriteVerification(uint64, uint64, common.Hash, common.Hash) error
}

func GetDbTx() (tx kv.RwTx, cleanup func()) {
	dbi, err := mdbx.NewTemporaryMdbx(context.Background(), "")
	if err != nil {
		panic(err)
	}
	tx, err = dbi.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}

	err = CreateHermezBuckets(tx)
	if err != nil {
		panic(err)
	}

	return tx, func() {
		tx.Rollback()
		dbi.Close()
	}
}

func TestNewHermezDb(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)
	assert.NotNil(t, db)
}

func TestGetSequenceByL1Block(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	require.NoError(t, db.WriteSequence(1, 1001, common.HexToHash("0xabc"), common.HexToHash("0xabc")))
	require.NoError(t, db.WriteSequence(2, 1002, common.HexToHash("0xdef"), common.HexToHash("0xdef")))

	info, err := db.GetSequenceByL1Block(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.L1BlockNo)
	assert.Equal(t, uint64(1001), info.BatchNo)
	assert.Equal(t, common.HexToHash("0xabc"), info.L1TxHash)

	info, err = db.GetSequenceByL1Block(2)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), info.L1BlockNo)
	assert.Equal(t, uint64(1002), info.BatchNo)
	assert.Equal(t, common.HexToHash("0xdef"), info.L1TxHash)
}

func TestGetSequenceByBatchNo(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	require.NoError(t, db.WriteSequence(1, 1001, common.HexToHash("0xabc"), common.HexToHash("0xabcd")))
	require.NoError(t, db.WriteSequence(2, 1002, common.HexToHash("0xdef"), common.HexToHash("0xdefg")))

	info, err := db.GetSequenceByBatchNo(1001)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.L1BlockNo)
	assert.Equal(t, uint64(1001), info.BatchNo)
	assert.Equal(t, common.HexToHash("0xabc"), info.L1TxHash)
	assert.Equal(t, common.HexToHash("0xabcd"), info.StateRoot)

	info, err = db.GetSequenceByBatchNo(1002)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), info.L1BlockNo)
	assert.Equal(t, uint64(1002), info.BatchNo)
	assert.Equal(t, common.HexToHash("0xdef"), info.L1TxHash)
	assert.Equal(t, common.HexToHash("0xdefg"), info.StateRoot)
}

func TestGetVerificationByL1BlockAndBatchNo(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	require.NoError(t, db.WriteVerification(3, 1003, common.HexToHash("0xghi"), common.HexToHash("0x333lll")))
	require.NoError(t, db.WriteVerification(4, 1004, common.HexToHash("0xjkl"), common.HexToHash("0x444mmm")))

	info, err := db.GetVerificationByL1Block(3)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), info.L1BlockNo)
	assert.Equal(t, uint64(1003), info.BatchNo)
	assert.Equal(t, common.HexToHash("0xghi"), info.L1TxHash)
	assert.Equal(t, common.HexToHash("0x333lll"), info.StateRoot)

	info, err = db.GetVerificationByBatchNo(1004)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), info.L1BlockNo)
	assert.Equal(t, uint64(1004), info.BatchNo)
	assert.Equal(t, common.HexToHash("0xjkl"), info.L1TxHash)
	assert.Equal(t, common.HexToHash("0x444mmm"), info.StateRoot)
}

func TestGetAndSetLatest(t *testing.T) {

	testCases := []struct {
		desc                    string
		table                   string
		writeSequenceMethod     func(IHermezDb, uint64, uint64, common.Hash, common.Hash) error
		writeVerificationMethod func(IHermezDb, uint64, uint64, common.Hash, common.Hash) error
		l1BlockNo               uint64
		batchNo                 uint64
		l1TxHashBytes           common.Hash
		stateRoot               common.Hash
		l1InfoRoot              common.Hash
	}{
		{"sequence 1", L1SEQUENCES, IHermezDb.WriteSequence, IHermezDb.WriteVerification, 1, 1001, common.HexToHash("0xabc"), common.HexToHash("0xabc"), common.HexToHash("0xabc")},
		{"sequence 2", L1SEQUENCES, IHermezDb.WriteSequence, IHermezDb.WriteVerification, 2, 1002, common.HexToHash("0xdef"), common.HexToHash("0xdef"), common.HexToHash("0xdef")},
		{"verification 1", L1VERIFICATIONS, IHermezDb.WriteSequence, IHermezDb.WriteVerification, 3, 1003, common.HexToHash("0xghi"), common.HexToHash("0xghi"), common.HexToHash("0xghi")},
		{"verification 2", L1VERIFICATIONS, IHermezDb.WriteSequence, IHermezDb.WriteVerification, 4, 1004, common.HexToHash("0xjkl"), common.HexToHash("0xjkl"), common.HexToHash("0xjkl")},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tx, cleanup := GetDbTx()
			db := NewHermezDb(tx)
			var err error
			if tc.table == L1SEQUENCES {
				err = tc.writeSequenceMethod(db, tc.l1BlockNo, tc.batchNo, tc.l1TxHashBytes, tc.stateRoot)
			} else {
				err = tc.writeVerificationMethod(db, tc.l1BlockNo, tc.batchNo, tc.l1TxHashBytes, tc.stateRoot)
			}
			require.NoError(t, err)

			info, err := db.getLatest(tc.table)
			require.NoError(t, err)
			assert.Equal(t, tc.batchNo, info.BatchNo)
			assert.Equal(t, tc.l1BlockNo, info.L1BlockNo)
			assert.Equal(t, tc.l1TxHashBytes, info.L1TxHash)
			assert.Equal(t, tc.stateRoot, info.StateRoot)
			cleanup()
		})
	}
}

func TestGetAndSetLatestUnordered(t *testing.T) {
	testCases := []struct {
		desc          string
		table         string
		writeMethod   func(IHermezDb, uint64, uint64, common.Hash, common.Hash) error
		l1BlockNo     uint64
		batchNo       uint64
		l1TxHashBytes common.Hash
		stateRoot     common.Hash
	}{
		{"verification 2", L1VERIFICATIONS, IHermezDb.WriteVerification, 4, 1004, common.HexToHash("0xjkl"), common.HexToHash("0xjkl")},
		{"verification 3", L1VERIFICATIONS, IHermezDb.WriteVerification, 6, 1007, common.HexToHash("0xrst"), common.HexToHash("0xrst")},
		{"verification 1", L1VERIFICATIONS, IHermezDb.WriteVerification, 3, 1003, common.HexToHash("0xghi"), common.HexToHash("0xghi")},
	}

	var highestBatchNo uint64

	tx, cleanup := GetDbTx()
	db := NewHermezDb(tx)

	for _, tc := range testCases {
		err := tc.writeMethod(db, tc.l1BlockNo, tc.batchNo, tc.l1TxHashBytes, tc.stateRoot)
		require.NoError(t, err)

		if tc.batchNo > highestBatchNo {
			highestBatchNo = tc.batchNo
		}
	}

	info, err := db.getLatest(L1VERIFICATIONS)
	require.NoError(t, err)
	assert.Equal(t, highestBatchNo, info.BatchNo)

	cleanup()
}

func TestGetAndSetForkId(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	forkIntervals := []struct {
		ForkId          uint64
		FromBatchNumber uint64
		ToBatchNumber   uint64
	}{
		{ForkId: 1, FromBatchNumber: 1, ToBatchNumber: 10},
		{ForkId: 2, FromBatchNumber: 11, ToBatchNumber: 100},
		{ForkId: 3, FromBatchNumber: 101, ToBatchNumber: 1000},
	}

	for _, forkInterval := range forkIntervals {
		for b := forkInterval.FromBatchNumber; b <= forkInterval.ToBatchNumber; b++ {
			err := db.WriteForkId(b, forkInterval.ForkId)
			require.NoError(t, err, "Failed to write ForkId")
		}
	}

	testCases := []struct {
		batchNo        uint64
		expectedForkId uint64
	}{
		{0, 1}, // batch 0 = forkID, special case, batch 0 has the same forkId as batch 1

		{1, 1},  // batch 1  = forkId 1, first batch for forkId 1
		{5, 1},  // batch 5  = forkId 1, a batch between first and last for forkId 1
		{10, 1}, // batch 10 = forkId 1, last batch for forkId 1

		{11, 2},  // batch 11  = forkId 1, first batch for forkId 2
		{50, 2},  // batch 50  = forkId 1, a batch between first and last for forkId 2
		{100, 2}, // batch 100 = forkId 1, last batch for forkId 2

		{101, 3},  // batch 101  = forkId 1, first batch for forkId 3
		{500, 3},  // batch 500  = forkId 1, a batch between first and last for forkId 3
		{1000, 3}, // batch 1000 = forkId 1, last batch for forkId 3

		{1001, 0}, // batch 1001 = a batch out of the range of the known forks
	}

	for _, tc := range testCases {
		fetchedForkId, err := db.GetForkId(tc.batchNo)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedForkId, fetchedForkId, "invalid expected fork id when getting fork id by batch number")
	}
}

func TestGetL2BlockBatchNo(t *testing.T) {
	testCases := make([]struct {
		l2BlockNo uint64
		batchNo   uint64
	}, 100)

	for i := 0; i < 100; i++ {
		testCases[i] = struct {
			l2BlockNo uint64
			batchNo   uint64
		}{uint64(i + 1), uint64(1000 + i + 1)}
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("L2BlockNo: %d BatchNo: %d", tc.l2BlockNo, tc.batchNo), func(t *testing.T) {
			tx, cleanup := GetDbTx()
			db := NewHermezDb(tx)

			err := db.WriteBlockBatch(tc.l2BlockNo, tc.batchNo)
			require.NoError(t, err, "Failed to write BlockBatch")

			fetchedBatchNo, err := db.GetBatchNoByL2Block(tc.l2BlockNo)
			require.NoError(t, err, "Failed to get BlockBatch")
			assert.Equal(t, tc.batchNo, fetchedBatchNo, "Fetched BlockBatch doesn't match expected")
			cleanup()
		})
	}
}

func TestGetL2BlockNosByBatch(t *testing.T) {
	testCases := []struct {
		l2BlockNo uint64
		batchNo   uint64
	}{
		{1, 1001},
		{2, 1001},
		{3, 1001},
		{4, 1002},
		{5, 1002},
		{6, 1003},
		{7, 1003},
		{8, 1003},
		{9, 1003},
		{10, 1004},
	}

	expectedBatchMapping := make(map[uint64][]uint64)
	for _, tc := range testCases {
		expectedBatchMapping[tc.batchNo] = append(expectedBatchMapping[tc.batchNo], tc.l2BlockNo)
	}

	for batchNo, expectedL2BlockNos := range expectedBatchMapping {
		t.Run(fmt.Sprintf("BatchNo: %d", batchNo), func(t *testing.T) {
			tx, cleanup := GetDbTx()
			db := NewHermezDb(tx)

			for _, tc := range testCases {
				err := db.WriteBlockBatch(tc.l2BlockNo, tc.batchNo)
				require.NoError(t, err, "Failed to write BlockBatch")
			}

			fetchedL2BlockNos, err := db.GetL2BlockNosByBatch(batchNo)
			require.NoError(t, err, "Failed to get L2BlockNos by Batch")
			assert.ElementsMatch(t, expectedL2BlockNos, fetchedL2BlockNos, "Fetched L2BlockNos don't match expected")
			cleanup()
		})
	}
}

func TestTruncateSequences(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()

	db := NewHermezDb(tx)

	for i := 0; i < 1000; i++ {
		err := db.WriteSequence(uint64(i), uint64(i), common.HexToHash("0xabc"), common.HexToHash("0xabc"))
		require.NoError(t, err)
		err = db.WriteBlockBatch(uint64(i), uint64(i))
		require.NoError(t, err)
	}

	err := db.TruncateSequences(500)
	require.NoError(t, err)

	batchNo, err := db.GetBatchNoByL2Block(500)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), batchNo)
}

func TestTruncateVerifications(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()

	db := NewHermezDb(tx)

	for i := 0; i < 1000; i++ {
		err := db.WriteVerification(uint64(i), uint64(i), common.HexToHash("0xabc"), common.HexToHash("0xabc"))
		require.NoError(t, err)
		err = db.WriteBlockBatch(uint64(i), uint64(i))
		require.NoError(t, err)
	}

	err := db.TruncateVerifications(500)
	require.NoError(t, err)

	batchNo, err := db.GetBatchNoByL2Block(500)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), batchNo)
}

func TestTruncateBlockBatches(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()

	db := NewHermezDb(tx)

	for i := uint64(1); i <= 1000; i++ {
		err := db.WriteBlockBatch(i, i)
		require.NoError(t, err)
	}

	l2BlockNo := uint64(500)
	err := db.DeleteBlockBatches(l2BlockNo+1, 1000)
	require.NoError(t, err)

	for i := l2BlockNo + 1; i <= 1000; i++ {
		_, err := db.GetBatchNoByL2Block(i)
		require.Error(t, err)
	}

	for i := uint64(1); i <= l2BlockNo; i++ {
		batchNo, err := db.GetBatchNoByL2Block(i)
		require.NoError(t, err)
		assert.Equal(t, i, batchNo)
	}
}

// Benchmarks

func BenchmarkWriteSequence(b *testing.B) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := db.WriteSequence(uint64(i), uint64(i+1000), common.HexToHash("0xabc"), common.HexToHash("0xabc"))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteVerification(b *testing.B) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := db.WriteVerification(uint64(i), uint64(i+2000), common.HexToHash("0xdef"), common.HexToHash("0xdef"))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSequenceByL1Block(b *testing.B) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	for i := 0; i < 1000; i++ {
		err := db.WriteSequence(uint64(i), uint64(i+1000), common.HexToHash("0xabc"), common.HexToHash("0xabc"))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.GetSequenceByL1Block(uint64(i % 1000))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetVerificationByL1Block(b *testing.B) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	for i := 0; i < 1000; i++ {
		err := db.WriteVerification(uint64(i), uint64(i+2000), common.HexToHash("0xdef"), common.HexToHash("0xdef"))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.GetVerificationByL1Block(uint64(i % 1000))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSequenceByBatchNo(b *testing.B) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	for i := 0; i < 1000; i++ {
		err := db.WriteSequence(uint64(i), uint64(i+1000), common.HexToHash("0xabc"), common.HexToHash("0xabc"))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.GetSequenceByBatchNo(uint64(i % 1000))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetVerificationByBatchNo(b *testing.B) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	for i := 0; i < 1000; i++ {
		err := db.WriteVerification(uint64(i), uint64(i+2000), common.HexToHash("0xdef"), common.HexToHash("0xdef"))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.GetVerificationByBatchNo(uint64(i % 1000))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestBatchBlocks(t *testing.T) {
	tx, cleanup := GetDbTx()
	defer cleanup()
	db := NewHermezDb(tx)

	for i := 0; i < 1000; i++ {
		err := db.WriteBlockBatch(uint64(i), uint64(1))
		if err != nil {
			t.Fatal(err)
		}
	}

	blocks, err := db.GetL2BlockNosByBatch(1)
	if err != nil {
		t.Fatal(err)
	}

	if len(blocks) != 1000 {
		t.Fatal("Expected 1000 blocks")
	}
}

func TestDeleteForkId(t *testing.T) {
	type forkInterval struct {
		ForkId          uint64
		FromBatchNumber uint64
		ToBatchNumber   uint64
	}
	forkIntervals := []forkInterval{
		{1, 1, 10},
		{2, 11, 20},
		{3, 21, 30},
		{4, 31, 40},
		{5, 41, 50},
		{6, 51, 60},
		{7, 61, 70},
	}

	testCases := []struct {
		name                           string
		fromBatchToDelete              uint64
		toBatchToDelete                uint64
		expectedDeletedForksIds        []uint64
		expectedRemainingForkIntervals []forkInterval
	}{
		{"delete fork id only for the last batch", 70, 70, nil, []forkInterval{
			{1, 1, 10},
			{2, 11, 20},
			{3, 21, 30},
			{4, 31, 40},
			{5, 41, 50},
			{6, 51, 60},
			{7, 61, math.MaxUint64},
		}},
		{"delete fork id for batches that don't exist", 80, 90, nil, []forkInterval{
			{1, 1, 10},
			{2, 11, 20},
			{3, 21, 30},
			{4, 31, 40},
			{5, 41, 50},
			{6, 51, 60},
			{7, 61, math.MaxUint64},
		}},
		{"delete fork id for batches that cross multiple forks from some point until the last one - unwind", 27, 70, []uint64{4, 5, 6, 7}, []forkInterval{
			{1, 1, 10},
			{2, 11, 20},
			{3, 21, math.MaxUint64},
		}},
		{"delete fork id for batches that cross multiple forks from zero to some point - prune", 0, 36, []uint64{1, 2, 3}, []forkInterval{
			{4, 37, 40},
			{5, 41, 50},
			{6, 51, 60},
			{7, 61, math.MaxUint64},
		}},
		{"delete fork id for batches that cross multiple forks from some point after the beginning to some point before the end - hole", 23, 42, []uint64{4}, []forkInterval{
			{1, 1, 10},
			{2, 11, 20},
			{3, 21, 22},
			{5, 43, 50},
			{6, 51, 60},
			{7, 61, math.MaxUint64},
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx, cleanup := GetDbTx()
			defer cleanup()
			db := NewHermezDb(tx)

			for _, forkInterval := range forkIntervals {
				for b := forkInterval.FromBatchNumber; b <= forkInterval.ToBatchNumber; b++ {
					err := db.WriteForkId(b, forkInterval.ForkId)
					require.NoError(t, err, "Failed to write ForkId")
				}
			}

			err := db.DeleteForkIds(tc.fromBatchToDelete, tc.toBatchToDelete)
			require.NoError(t, err)

			for batchNum := tc.fromBatchToDelete; batchNum <= tc.toBatchToDelete; batchNum++ {
				forkId, err := db.GetForkId(batchNum)
				require.NoError(t, err)
				assert.Equal(t, uint64(0), forkId)
			}

			for _, forkId := range tc.expectedDeletedForksIds {
				forkInterval, found, err := db.GetForkInterval(forkId)
				require.NoError(t, err)
				assert.False(t, found)
				assert.Nil(t, forkInterval)
			}

			for _, remainingForkInterval := range tc.expectedRemainingForkIntervals {
				forkInterval, found, err := db.GetForkInterval(remainingForkInterval.ForkId)
				require.NoError(t, err)
				assert.True(t, found)
				assert.Equal(t, remainingForkInterval.FromBatchNumber, forkInterval.FromBatchNumber)
				assert.Equal(t, remainingForkInterval.ToBatchNumber, forkInterval.ToBatchNumber)
			}
		})
	}
}
