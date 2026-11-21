package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// A staged round's commitment re-hashes untouched leaves through the as-of getter, so it must see what the
// block's earlier rounds wrote into the parent.
func TestAsOfGetterReadsThroughTheParent(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := newTestDb(t, 100)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	block, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer block.Close()

	addr := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	acc := accounts.Account{Nonce: 1}
	require.NoError(t, block.DomainPut(kv.AccountsDomain, rwTx, addr[:], accounts.SerialiseV3(&acc), 1, nil))

	round, err := execctx.NewSharedDomains(ctx, rwTx, log.New(), execctx.WithParent(block))
	require.NoError(t, err)
	defer round.Close()

	enc, ok, err := round.AsOfGetter(rwTx).GetAsOf(kv.AccountsDomain, addr[:], 2)
	require.NoError(t, err)
	require.True(t, ok, "the round does not see the account its block wrote")
	require.Equal(t, accounts.SerialiseV3(&acc), enc)
}
