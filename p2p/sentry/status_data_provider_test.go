package sentry

import (
	"context"
	"math/big"
	"sync"
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

// testBlockReader implements only the FullBlockReader methods used by
// StatusDataProvider. Embedding the interface gives nil-pointer panics on
// any method we forgot to stub — which is the correct failure mode for a test.
type testBlockReader struct {
	services.FullBlockReader
}

func (r *testBlockReader) MinimumBlockAvailable(context.Context, kv.Tx) (uint64, error) {
	return 0, nil
}

// slowDB wraps a real kv.RoDB but blocks inside View until unblockCh is closed.
// This lets a test hold the refresh semaphore for a controlled duration.
type slowDB struct {
	kv.RoDB
	unblockCh chan struct{}
}

func (d *slowDB) View(ctx context.Context, fn func(kv.Tx) error) error {
	select {
	case <-d.unblockCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return d.RoDB.View(ctx, fn)
}

// seedTestHeader writes a minimal header + TD into db and sets the head block
// hash so that ReadChainHeadWithTx can succeed.
func seedTestHeader(t *testing.T, db kv.RwDB) common.Hash {
	t.Helper()

	header := &types.Header{
		Number:     *uint256.NewInt(42),
		Difficulty: *uint256.NewInt(100),
		Time:       1700000000,
		Extra:      []byte("test"),
	}
	hash := header.Hash()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteTd(tx, hash, 42, big.NewInt(100)))
	rawdb.WriteHeadBlockHash(tx, hash)
	require.NoError(t, tx.Commit())

	return hash
}

// newTestProvider builds a StatusDataProvider backed by the given db.
func newTestProvider(t *testing.T, db kv.RoDB) *StatusDataProvider {
	t.Helper()
	return &StatusDataProvider{
		db:          db,
		blockReader: &testBlockReader{},
		networkId:   1,
		genesisHash: common.HexToHash("0xdead"),
		logger:      log.New(),
		refreshSem:  make(chan struct{}, 1),
	}
}

// --- tests ---

// TestGetStatusData_ReturnsDistinctProtobufs verifies that each call returns a
// freshly-built protobuf, not a shared cached pointer. Mutating one result must
// not affect subsequent calls.
func TestGetStatusData_ReturnsDistinctProtobufs(t *testing.T) {
	t.Parallel()

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, db)
	p := newTestProvider(t, db)

	ctx := context.Background()

	sd1, err := p.GetStatusData(ctx)
	require.NoError(t, err)

	sd2, err := p.GetStatusData(ctx)
	require.NoError(t, err)

	// Different pointers — not the same object.
	assert.NotSame(t, sd1, sd2, "two calls must return distinct protobuf pointers")

	// Mutating sd1 must not affect sd2.
	sd1.MaxBlockHeight = 999999
	sd1.ForkData.HeightForks = []uint64{1, 2, 3}

	assert.NotEqual(t, sd1.MaxBlockHeight, sd2.MaxBlockHeight,
		"mutation of first result must not be visible in second result")
	assert.NotEqual(t, sd1.ForkData.HeightForks, sd2.ForkData.HeightForks,
		"mutation of nested ForkData must not be visible in second result")
}

// TestGetStatusData_CancelledCtxDoesNotBlock verifies that a caller whose
// context is cancelled does not hang waiting for the refresh semaphore.
func TestGetStatusData_CancelledCtxDoesNotBlock(t *testing.T) {
	t.Parallel()

	realDB := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, realDB)

	unblock := make(chan struct{})
	t.Cleanup(func() {
		// Ensure goroutine is not leaked even if the test fails early.
		select {
		case <-unblock:
		default:
			close(unblock)
		}
	})

	slow := &slowDB{RoDB: realDB, unblockCh: unblock}
	p := newTestProvider(t, slow)

	// goroutine 1: holds the refresh semaphore (blocks in slowDB.View).
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This call will block inside View until unblock is closed.
		_, _ = p.GetStatusData(context.Background())
	}()

	// Give goroutine 1 time to enter View and hold the semaphore.
	time.Sleep(50 * time.Millisecond)

	// goroutine 2: call with an already-cancelled context.
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	var err2 error
	go func() {
		_, err2 = p.GetStatusData(cancelledCtx)
		close(done)
	}()

	select {
	case <-done:
		// Must return ctx error, not block.
		assert.ErrorIs(t, err2, context.Canceled,
			"cancelled caller must get context.Canceled, not block")
	case <-time.After(2 * time.Second):
		t.Fatal("GetStatusData blocked on cancelled context — ctx cancellation not honoured")
	}

	// Unblock the first goroutine and wait for cleanup.
	close(unblock)
	wg.Wait()
}

// TestGetStatusData_ShortDeadlineDoesNotBlock is similar to the cancellation
// test but uses a tight deadline to verify the select path for DeadlineExceeded.
func TestGetStatusData_ShortDeadlineDoesNotBlock(t *testing.T) {
	t.Parallel()

	realDB := memdb.NewTestDB(t, dbcfg.ChainDB)
	seedTestHeader(t, realDB)

	unblock := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-unblock:
		default:
			close(unblock)
		}
	})

	slow := &slowDB{RoDB: realDB, unblockCh: unblock}
	p := newTestProvider(t, slow)

	// Hold the semaphore via a blocking call.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = p.GetStatusData(context.Background())
	}()
	time.Sleep(50 * time.Millisecond)

	// Call with a short deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := p.GetStatusData(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded,
		"short-deadline caller must get DeadlineExceeded, not block")

	close(unblock)
	wg.Wait()
}
