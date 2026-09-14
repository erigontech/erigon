package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"

	"github.com/erigontech/erigon/common/log/v3"
)

// A block built over several staged rounds commits the state of its LAST round, not its first.
//
// Each round runs into a child parented on the block's SharedDomains and is committed by merging that
// child back in. The merge hands the child's buffered domain writers to the block, and the flush replays
// them — so the ORDER they are replayed in decides which write to a key survives. Get that order wrong by
// one merge and the block commits as if only its first round had ever run, while its state root — advanced
// cumulatively through the shared commitment context — still says otherwise. Nothing downstream compares
// the two, so the block is accepted and the chain silently drops every transaction after the first round's.
func TestSharedDomains_MergedRoundsCommitNewestWrite(t *testing.T) {
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

	// Three rounds, each writing the same account with a higher nonce — a sender sending three
	// transactions across three rounds of one block.
	for round := uint64(1); round <= 3; round++ {
		child, cerr := execctx.NewSharedDomains(ctx, rwTx, log.New(), execctx.WithParent(block))
		require.NoError(t, cerr)

		acc := accounts.Account{Nonce: round}
		require.NoError(t, child.DomainPut(kv.AccountsDomain, rwTx, addr[:], accounts.SerialiseV3(&acc), round, nil))

		require.NoError(t, block.Merge(ctx, block.TxNum(), child, round, true))
	}

	require.NoError(t, block.Flush(ctx, rwTx))

	enc, _, err := rwTx.GetLatest(kv.AccountsDomain, addr[:])
	require.NoError(t, err)
	require.NotEmpty(t, enc, "the account never reached the committed state")

	var got accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&got, enc))
	require.Equal(t, uint64(3), got.Nonce,
		"committed the state of an earlier round: merged writers are replayed in the wrong order")
}
