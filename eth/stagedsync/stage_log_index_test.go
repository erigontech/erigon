package stagedsync

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"

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

func TestPromoteLogIndex(t *testing.T) {
	require, ctx := require.New(t), context.Background()
	_, tx := memdb.NewTestTx(t)

	expectAddrs, expectTopics := genReceipts(t, tx, 100)

	cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
	cfgCopy := cfg
	cfgCopy.bufLimit = 10
	cfgCopy.flushEvery = time.Nanosecond

	err := promoteLogIndex("logPrefix", tx, 0, 0, cfgCopy, ctx)
	require.NoError(err)

	// Check indices GetCardinality (in how many blocks they meet)
	for addr, expect := range expectAddrs {
		m, err := bitmapdb.Get(tx, kv.LogAddressIndex, addr[:], 0, 10_000_000)
		require.NoError(err)
		require.Equal(expect, m.GetCardinality())
	}
	for topic, expect := range expectTopics {
		m, err := bitmapdb.Get(tx, kv.LogTopicIndex, topic[:], 0, 10_000_000)
		require.NoError(err)
		require.Equal(expect, m.GetCardinality())
	}
}

func TestPruneLogIndex(t *testing.T) {
	require, tmpDir, ctx := require.New(t), t.TempDir(), context.Background()
	_, tx := memdb.NewTestTx(t)

	_, _ = genReceipts(t, tx, 100)

	cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
	cfgCopy := cfg
	cfgCopy.bufLimit = 10
	cfgCopy.flushEvery = time.Nanosecond
	err := promoteLogIndex("logPrefix", tx, 0, 0, cfgCopy, ctx)
	require.NoError(err)

	// Mode test
	err = pruneLogIndex("", tx, tmpDir, 50, ctx)
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
}

func TestUnwindLogIndex(t *testing.T) {
	require, tmpDir, ctx := require.New(t), t.TempDir(), context.Background()
	_, tx := memdb.NewTestTx(t)

	expectAddrs, expectTopics := genReceipts(t, tx, 100)

	cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
	cfgCopy := cfg
	cfgCopy.bufLimit = 10
	cfgCopy.flushEvery = time.Nanosecond
	err := promoteLogIndex("logPrefix", tx, 0, 0, cfgCopy, ctx)
	require.NoError(err)

	// Mode test
	err = pruneLogIndex("", tx, tmpDir, 50, ctx)
	require.NoError(err)

	// Unwind test
	err = unwindLogIndex("logPrefix", tx, 70, cfg, nil)
	require.NoError(err)

	for addr := range expectAddrs {
		m, err := bitmapdb.Get(tx, kv.LogAddressIndex, addr[:], 0, 10_000_000)
		require.NoError(err)
		require.True(m.Maximum() <= 700)
	}
	for topic := range expectTopics {
		m, err := bitmapdb.Get(tx, kv.LogTopicIndex, topic[:], 0, 10_000_000)
		require.NoError(err)
		require.True(m.Maximum() <= 700)
	}
}
