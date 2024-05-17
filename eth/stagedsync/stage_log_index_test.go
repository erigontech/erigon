package stagedsync

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/prune"

	"github.com/stretchr/testify/require"
)

func genReceipts(t *testing.T, tx kv.RwTx, blocks uint64) (map[libcommon.Address]uint64, map[libcommon.Hash]uint64) {
	addrs := []libcommon.Address{{1}, {2}, {3}}
	topics := []libcommon.Hash{{1}, {2}, {3}}

	expectAddrs := map[libcommon.Address]uint64{}
	expectTopics := map[libcommon.Hash]uint64{}
	for i := range addrs {
		expectAddrs[addrs[i]] = 0
	}
	for i := range topics {
		expectTopics[topics[i]] = 0
	}

	var receipts types.Receipts
	for i := uint64(0); i < blocks; i++ {
		switch i % 3 {
		case 0:
			a, t1, t2 := addrs[i%3], topics[i%3], topics[(i+1)%3]
			receipts = types.Receipts{{
				Logs: []*types.Log{
					{
						Address: a,
						Topics:  []libcommon.Hash{t1, t2},
					},
					{
						Address: a,
						Topics:  []libcommon.Hash{t2},
					},
					{
						Address: a,
						Topics:  []libcommon.Hash{},
					},
				},
			}}
			expectAddrs[a]++
			expectTopics[t1]++
			expectTopics[t2]++

		case 1:
			a1, a2, t1, t2 := addrs[i%3], addrs[(i+1)%3], topics[i%3], topics[(i+1)%3]
			receipts = types.Receipts{{
				Logs: []*types.Log{
					{
						Address: a1,
						Topics:  []libcommon.Hash{t1, t2, t1, t2},
					},
				},
			}, {
				Logs: []*types.Log{
					{
						Address: a2,
						Topics:  []libcommon.Hash{t1, t2, t1, t2},
					},
					{
						Address: a1,
						Topics:  []libcommon.Hash{t1},
					},
				},
			}}
			expectAddrs[a1]++
			expectAddrs[a2]++
			expectTopics[t1]++
			expectTopics[t2]++
		case 2:
			receipts = types.Receipts{{}, {}, {}}
		}
		err := rawdb.AppendReceipts(tx, i, receipts)
		require.NoError(t, err)
	}
	return expectAddrs, expectTopics
}

func TestPruneLogIndex(t *testing.T) {
	logger := log.New()
	require, tmpDir, ctx := require.New(t), t.TempDir(), context.Background()
	_, tx := memdb.NewTestTx(t)

	_, _ = genReceipts(t, tx, 90)

	cfg := StageLogIndexCfg(nil, prune.DefaultMode, "", nil)
	cfgCopy := cfg
	cfgCopy.bufLimit = 10
	cfgCopy.flushEvery = time.Nanosecond
	err := promoteLogIndex("logPrefix", tx, 0, 0, 0, cfgCopy, ctx, logger)
	require.NoError(err)

	// Mode test
	err = pruneLogIndex("", tx, tmpDir, 0, 45, ctx, logger, map[libcommon.Address]bool{{1}: true}) // using addr {1} from genReceipts
	require.NoError(err)

	{
		total := 0
		err = tx.ForEach(kv.LogAddressIndex, nil, func(k, v []byte) error {
			require.True(binary.BigEndian.Uint32(k[length.Addr:]) == 4294967295)
			total++
			return nil
		})
		require.NoError(err)
		require.True(total == 3)
	}
	{
		total := 0
		err = tx.ForEach(kv.LogTopicIndex, nil, func(k, v []byte) error {
			require.True(binary.BigEndian.Uint32(k[length.Hash:]) == 4294967295)
			total++
			return nil
		})
		require.NoError(err)
		require.True(total == 3)
	}
	{
		total := 0
		err = tx.ForEach(kv.Log, nil, func(k, v []byte) error {
			total++
			return nil
		})
		require.NoError(err)
		require.Equal(total, 60) // 1/3rd of 45 not pruned as it has address "1", so 30 Pruned in total, remaining 90-30
	}
}
