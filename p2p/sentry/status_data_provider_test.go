package sentry

import (
	"context"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

// --- test helpers ---

type testBlockReader struct {
	dbservices.FullBlockReader
	frozenInView uint64
	frozenHeader *types.Header
}

func (r *testBlockReader) MinimumBlockAvailable(context.Context, kv.Tx) (uint64, error) {
	return 0, nil
}

func (r *testBlockReader) FrozenBlocksInView(kv.Getter) uint64 { return r.frozenInView }

func (r *testBlockReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	return r.frozenHeader, nil
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
	require.NoError(t, rawdb.WriteTd(tx, hash, number, *uint256.NewInt(uint64(difficulty))))
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

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
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

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, db, 42, 100)
	p := newTestProvider(t, db)

	ctx := t.Context()

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

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, db, 42, 100)
	p := newTestProvider(t, db)

	ctx := context.Background()
	errs := make(chan error, 10)
	for range 10 {
		go func() {
			_, err := p.GetStatusData(ctx)
			errs <- err
		}()
	}

	for range 10 {
		require.NoError(t, <-errs)
	}
}

// TestGetStatusData_SnapshotFallback covers the arm taken when the db holds no
// head header: it reads through the tx's pinned block-files view, so a wrongly
// wired db panics here rather than returning ErrNoSnapshots.
func TestGetStatusData_SnapshotFallback(t *testing.T) {
	t.Parallel()

	header := &types.Header{
		Number:     *uint256.NewInt(42),
		Difficulty: *uint256.NewInt(7),
		Time:       1700000042,
	}

	for _, tc := range []struct {
		name   string
		reader *testBlockReader
		height uint64
		err    error
	}{
		{"no snapshots", &testBlockReader{}, 0, ErrNoSnapshots},
		{"header missing from files", &testBlockReader{frozenInView: 42}, 0, ErrNoSnapshots},
		{"head from files", &testBlockReader{frozenInView: 42, frozenHeader: header}, 42, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			s := newTestProvider(t, db)
			s.blockReader = tc.reader

			head, err := s.fetchChainHead(context.Background())
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.height, head.HeadHeight)
			assert.Equal(t, header.Hash(), head.HeadHash)
		})
	}
}
