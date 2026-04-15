package sentry

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/types"
)

// --- test helpers ---

type testBlockReader struct {
	services.FullBlockReader
}

func (r *testBlockReader) MinimumBlockAvailable(context.Context, kv.Tx) (uint64, error) {
	return 0, nil
}

func seedTestHeader(t *testing.T, db kv.RwDB, number uint64, difficulty uint64) common.Hash {
	t.Helper()

	header := &types.Header{
		Number:     *uint256.NewInt(number),
		Difficulty: *uint256.NewInt(difficulty),
		Time:       1700000000 + number,
		Extra:      []byte("test"),
	}
	hash := header.Hash()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteTd(tx, hash, number, big.NewInt(int64(difficulty))))
	rawdb.WriteHeadBlockHash(tx, hash)
	require.NoError(t, tx.Commit())

	return hash
}

func newTestProvider(t *testing.T, db kv.RoDB) *StatusDataProvider {
	t.Helper()
	return &StatusDataProvider{
		db:          db,
		blockReader: &testBlockReader{},
		networkId:   1,
		genesisHash: common.HexToHash("0xdead"),
		logger:      log.New(),
	}
}

// --- tests ---

func TestGetStatusData_ReturnsDistinctProtobufs(t *testing.T) {
	t.Parallel()

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, db, 42, 100)
	p := newTestProvider(t, db)

	ctx := context.Background()

	sd1, err := p.GetStatusData(ctx)
	require.NoError(t, err)

	sd2, err := p.GetStatusData(ctx)
	require.NoError(t, err)

	assert.NotSame(t, sd1, sd2, "two calls must return distinct protobuf pointers")

	sd1.MaxBlockHeight = 999999
	assert.NotEqual(t, sd1.MaxBlockHeight, sd2.MaxBlockHeight,
		"mutation of first result must not be visible in second result")
}

func TestGetStatusData_CacheInvalidatedByHeaderNotification(t *testing.T) {
	t.Parallel()

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, db, 42, 100)
	p := newTestProvider(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First call populates cache.
	sd1, err := p.GetStatusData(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), sd1.MaxBlockHeight)

	// Write a new head.
	seedTestHeader(t, db, 43, 200)

	// Cache is still warm — returns stale data.
	sd2, err := p.GetStatusData(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), sd2.MaxBlockHeight, "cache should still return old head")

	// Simulate header notification → invalidates cache.
	headersCh := make(chan [][]byte, 1)
	snapshotsCh := make(chan struct{}, 1)
	headersCh <- [][]byte{{}} // any value

	go p.Run(ctx, headersCh, snapshotsCh)

	// Give Run a moment to process the notification.
	// After invalidation, next call should fetch the new head.
	require.Eventually(t, func() bool {
		sd3, err := p.GetStatusData(ctx)
		return err == nil && sd3.MaxBlockHeight == 43
	}, time.Second, 10*time.Millisecond,
		"cache should be invalidated after header notification, returning new head")
}

func TestGetStatusData_ConcurrentCallsCoalesce(t *testing.T) {
	t.Parallel()

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, db, 42, 100)
	p := newTestProvider(t, db)

	ctx := context.Background()
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, err := p.GetStatusData(ctx)
			errs <- err
		}()
	}

	for i := 0; i < 10; i++ {
		require.NoError(t, <-errs)
	}
}
