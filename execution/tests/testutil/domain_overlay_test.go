package testutil

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestDomainVisibilityAfterGenesis verifies that genesis domain state
// can be read through a MemoryMutation overlay on a RO temporal tx.
func TestDomainVisibilityAfterGenesis(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	db := temporaltest.NewTestDB(t, dirs)
	ctx := context.Background()

	funded := accounts.InternAddress(common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"))
	gspec := &types.Genesis{
		Config:     chain.TestChainConfig,
		GasLimit:   30_000_000,
		Difficulty: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			funded.Value(): {Balance: big.NewInt(1_000_000_000)},
		},
	}

	// Step 1: CommitGenesisBlock (writes headers/TDs/config to KV, but NOT domain state).
	_, genesis, err := genesiswrite.CommitGenesisBlock(db, gspec, "", dirs, logger)
	require.NoError(t, err)
	require.NotNil(t, genesis)

	// Step 2: Write genesis alloc to domains via SharedDomains + Flush.
	{
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)

		sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
		require.NoError(t, err)

		_, _, err = genesiswrite.ComputeGenesisCommitment(ctx, gspec, rwTx, sd, genesis.Header())
		require.NoError(t, err)

		err = sd.Flush(ctx, rwTx)
		require.NoError(t, err)
		sd.Close()

		err = rwTx.Commit()
		require.NoError(t, err)
	}

	// Step 3: Read back via RO temporal tx.
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	addr := funded.Value()
	v, _, err := roTx.GetLatest(kv.AccountsDomain, addr[:])
	t.Logf("RO tx GetLatest: len=%d err=%v", len(v), err)
	require.NotEmpty(t, v, "account should be visible via direct GetLatest after SD flush")

	reader := state.NewReaderV3(roTx)
	acct, err := reader.ReadAccountData(funded)
	require.NoError(t, err)
	require.NotNil(t, acct, "account should be visible via ReaderV3")
	t.Logf("ReaderV3: balance=%s nonce=%d", &acct.Balance, acct.Nonce)
	require.False(t, acct.Balance.IsZero(), "should have non-zero balance")

	// Step 4: Read through MemoryMutation overlay.
	overlay, err := membatchwithdb.NewMemoryBatch(roTx, "", logger)
	require.NoError(t, err)
	defer overlay.Close()

	overlayReader := state.NewReaderV3(overlay)
	acct2, err := overlayReader.ReadAccountData(funded)
	require.NoError(t, err)
	require.NotNil(t, acct2, "account should be visible via overlay ReaderV3")
	require.Equal(t, acct.Balance, acct2.Balance, "balance should match")
}
