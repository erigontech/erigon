package blockreplay_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/blockreplay"
)

// TestWitnessPrefixOps pins that a prefix scan over the witness-backed mem batch
// sees storage that exists only in the witness pre-state (not just exec writes):
// HasPrefix/IteratePrefix must find it, and DomainDelPrefix must clear it. Before
// the overlay-aware prefix methods these all missed the witness, so HasStorage
// returned false for a stored account and a cleared account kept readable slots.
func TestWitnessPrefixOps(t *testing.T) {
	fx := loadFixture(t, "25604144")
	ctx := context.Background()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	const seedTxNum = 1 << 20

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	doms, _, err := blockreplay.NewWitnessDomains(ctx, tx, fx, seedTxNum, logger)
	require.NoError(t, err)
	defer doms.Close()

	// Pick a present account with at least one non-zero witness storage slot.
	// Zero-valued slots (e.g. fresh reads by a just-created contract) are not
	// stored, so they never appear in a prefix scan and must not be counted.
	var addr common.Address
	var wantSlots int
	for a, slots := range fx.Storage {
		if !fx.Accounts[a].Present {
			continue
		}
		nz := 0
		for _, v := range slots {
			if v != [32]byte{} {
				nz++
			}
		}
		if nz > 0 {
			addr = common.Address(a)
			wantSlots = nz
			break
		}
	}
	require.NotZero(t, wantSlots, "fixture must have a present account with a non-zero storage slot")

	_, _, has, err := doms.HasPrefix(kv.StorageDomain, addr[:], tx)
	require.NoError(t, err)
	require.True(t, has, "HasPrefix must see witness-backed storage for %x", addr)

	got := 0
	require.NoError(t, doms.IteratePrefix(kv.StorageDomain, addr[:], tx, func(k, v []byte) (bool, error) {
		require.Len(t, k, len(addr)+32, "storage key is address+slot")
		require.NotEmpty(t, v, "iterated slot must be live")
		got++
		return true, nil
	}))
	require.Equal(t, wantSlots, got, "IteratePrefix must yield every witness slot")

	require.NoError(t, doms.DomainDelPrefix(kv.StorageDomain, tx, addr[:], seedTxNum))

	_, _, has, err = doms.HasPrefix(kv.StorageDomain, addr[:], tx)
	require.NoError(t, err)
	require.False(t, has, "DomainDelPrefix must clear the witness storage")

	got = 0
	require.NoError(t, doms.IteratePrefix(kv.StorageDomain, addr[:], tx, func(k, v []byte) (bool, error) {
		got++
		return true, nil
	}))
	require.Zero(t, got, "no witness slots may remain after DomainDelPrefix")
}
