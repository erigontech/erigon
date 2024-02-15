package stagedsync

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/ethdb/prune"

	"github.com/stretchr/testify/require"
)

func TestPromoteCustomTrace(t *testing.T) {
	t.Skip("TODO: fix this test")
	logger := log.New()
	require, ctx := require.New(t), context.Background()
	_, tx := memdb.NewTestTx(t)

	expectAddrs, expectTopics := genReceipts(t, tx, 100)

	cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
	cfgCopy := cfg
	cfgCopy.bufLimit = 10
	cfgCopy.flushEvery = time.Nanosecond

	err := promoteLogIndex("logPrefix", tx, 0, 0, cfgCopy, ctx, logger)
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

func TestPruneCustomTrace(t *testing.T) {
	t.Skip("TODO: fix this test")
	logger := log.New()
	require, tmpDir, ctx := require.New(t), t.TempDir(), context.Background()
	_, tx := memdb.NewTestTx(t)

	_, _ = genReceipts(t, tx, 100)

	cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
	cfgCopy := cfg
	cfgCopy.bufLimit = 10
	cfgCopy.flushEvery = time.Nanosecond
	err := promoteLogIndex("logPrefix", tx, 0, 0, cfgCopy, ctx, logger)
	require.NoError(err)

	// Mode test
	err = pruneLogIndex("", tx, tmpDir, 50, ctx, logger)
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

func TestUnwindCustomTrace(t *testing.T) {
	t.Skip("TODO: fix this test")
	logger := log.New()
	require, tmpDir, ctx := require.New(t), t.TempDir(), context.Background()
	_, tx := memdb.NewTestTx(t)

	expectAddrs, expectTopics := genReceipts(t, tx, 100)

	cfg := StageLogIndexCfg(nil, prune.DefaultMode, "")
	cfgCopy := cfg
	cfgCopy.bufLimit = 10
	cfgCopy.flushEvery = time.Nanosecond
	err := promoteLogIndex("logPrefix", tx, 0, 0, cfgCopy, ctx, logger)
	require.NoError(err)

	// Mode test
	err = pruneLogIndex("", tx, tmpDir, 50, ctx, logger)
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
