package migrations

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/stretchr/testify/require"
)

type testCases struct {
	testName         string
	mapCounters      map[uint64]map[string]int
	arrayCounters    map[uint64][vm.CounterTypesCount]int
	expectedCounters map[uint64][vm.CounterTypesCount]int
}

func TestCountersToArray(t *testing.T) {
	testCases := []testCases{
		{
			testName: "only map entries",
			mapCounters: map[uint64]map[string]int{
				1: {string(vm.CounterKeyNames[vm.A]): 1, string(vm.CounterKeyNames[vm.B]): 2},
				2: {string(vm.CounterKeyNames[vm.A]): 3, string(vm.CounterKeyNames[vm.B]): 4},
			},
			arrayCounters: map[uint64][vm.CounterTypesCount]int{},
			expectedCounters: map[uint64][vm.CounterTypesCount]int{
				1: {0, 1, 2, 0, 0, 0, 0, 0},
				2: {0, 3, 4, 0, 0, 0, 0, 0},
			},
		},
		{
			testName:    "only array entries",
			mapCounters: map[uint64]map[string]int{},
			arrayCounters: map[uint64][vm.CounterTypesCount]int{
				1: {0, 1, 2, 0, 0, 0, 0, 0},
				2: {0, 3, 4, 0, 0, 0, 0, 0},
			},
			expectedCounters: map[uint64][vm.CounterTypesCount]int{
				1: {0, 1, 2, 0, 0, 0, 0, 0},
				2: {0, 3, 4, 0, 0, 0, 0, 0},
			},
		},
		{
			testName: "arrays and maps entries",
			mapCounters: map[uint64]map[string]int{
				1: {string(vm.CounterKeyNames[vm.A]): 1, string(vm.CounterKeyNames[vm.B]): 2},
				2: {string(vm.CounterKeyNames[vm.A]): 3, string(vm.CounterKeyNames[vm.B]): 4},
			},
			arrayCounters: map[uint64][vm.CounterTypesCount]int{
				3: {2, 1, 2, 0, 0, 0, 0, 0},
				4: {1, 3, 4, 0, 0, 0, 0, 0},
			},
			expectedCounters: map[uint64][vm.CounterTypesCount]int{
				1: {0, 1, 2, 0, 0, 0, 0, 0},
				2: {0, 3, 4, 0, 0, 0, 0, 0},
				3: {2, 1, 2, 0, 0, 0, 0, 0},
				4: {1, 3, 4, 0, 0, 0, 0, 0},
			},
		},
	}

	for _, tc := range testCases {
		require, tmpDir, db := require.New(t), t.TempDir(), memdb.NewTestDB(t)

		err := prepareDbCounters(db, tc.mapCounters, tc.arrayCounters)
		require.NoError(err)

		migrator := NewMigrator(kv.ChainDB)

		migrator.Migrations = []Migration{countersToArray}
		err = migrator.Apply(db, tmpDir)
		require.NoError(err)

		err = assertDbCounters(t, db, tc.testName, tc.expectedCounters)
		require.NoError(err)
	}
}

func prepareDbCounters(db kv.RwDB, mapCounters map[uint64]map[string]int, arrayCounters map[uint64][vm.CounterTypesCount]int) error {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = tx.CreateBucket(kv.BATCH_COUNTERS); err != nil {
		return err
	}

	for l2BlockNo, countersMap := range mapCounters {
		countersMapBytes, err := json.Marshal(countersMap)
		if err != nil {
			return err
		}

		if err = tx.Put(kv.BATCH_COUNTERS, hermez_db.Uint64ToBytes(l2BlockNo), countersMapBytes); err != nil {
			return err
		}
	}

	for l2BlockNo, countersArray := range arrayCounters {
		countersArrayBytes, err := json.Marshal(countersArray)
		if err != nil {
			return err
		}

		if err = tx.Put(kv.BATCH_COUNTERS, hermez_db.Uint64ToBytes(l2BlockNo), countersArrayBytes); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func assertDbCounters(t *testing.T, db kv.RwDB, testName string, expectedCounters map[uint64][vm.CounterTypesCount]int) error {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := tx.Cursor(kv.BATCH_COUNTERS)
	if err != nil {
		return err
	}
	defer c.Close()

	actualCounters := map[uint64][vm.CounterTypesCount]int{}
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		var counters []int
		if err = json.Unmarshal(v, &counters); err != nil {
			return err
		}

		l2BlockNo := hermez_db.BytesToUint64(k)
		blockCounters := [vm.CounterTypesCount]int{}
		copy(blockCounters[:], counters)

		actualCounters[l2BlockNo] = blockCounters
	}

	require.Equal(t, expectedCounters, actualCounters, testName)

	return tx.Commit()
}
