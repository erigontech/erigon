package blockreplay_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSeedReadback proves the fixture's decoded pre-state re-encodes faithfully
// into the domain form: seed via the production Writer, read back through
// SharedDomains, and require the account/code/storage match the fixture.
func TestSeedReadback(t *testing.T) {
	fx := loadFixture(t, "25604144")
	ctx := context.Background()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	const seedTxNum = 1 << 20

	require.NoError(t, blockreplay.SeedDomains(ctx, db, fx, seedTxNum, logger))

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, roTx, logger)
	require.NoError(t, err)
	defer doms.Close()
	r := state.NewReaderV3(doms.AsStateGetter(roTx, execctxapi.StateGetterOptions{}))

	checkedContract := false
	for a, d := range fx.Accounts {
		if !d.Present {
			continue
		}
		addr := accounts.InternAddress(common.Address(a))
		got, err := r.ReadAccountData(addr)
		require.NoError(t, err)
		require.NotNil(t, got, "account %x must read back", a)
		require.Equal(t, d.Nonce, got.Nonce, "nonce %x", a)
		var wantBal uint256.Int
		wantBal.SetBytes(d.Balance[:])
		require.Equal(t, wantBal, got.Balance, "balance %x", a)

		if code := fx.Code[a]; len(code) > 0 {
			gotCode, err := r.ReadAccountCode(addr)
			require.NoError(t, err)
			require.Equal(t, code, gotCode, "code %x", a)
			require.Equal(t, common.BytesToHash(d.CodeHash[:]), got.CodeHash.Value(), "codehash %x", a)
			checkedContract = true
		}
	}
	require.True(t, checkedContract, "fixture must contain at least one contract to validate code form")

	for a, slots := range fx.Storage {
		addr := accounts.InternAddress(common.Address(a))
		for k, v := range slots {
			var want uint256.Int
			want.SetBytes(v[:])
			got, _, err := r.ReadAccountStorage(addr, accounts.InternKey(common.Hash(k)))
			require.NoError(t, err)
			require.Equal(t, want, got, "storage %x/%x", a, k)
		}
	}
}
