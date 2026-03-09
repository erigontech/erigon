// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// these tests have cleanup issues for mdbx on windows
package stagedsync

import (
	"context"
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

// setup2CacheTest creates the minimal mdbx+temporal+domains stack used by the
// 2-cache baseline tests. Cleanup is registered via t.Cleanup.
func setup2CacheTest(t *testing.T) (kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	lgr := log.New()

	tmpDir, err := os.MkdirTemp("", "erigon-2cache-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { dir.RemoveAll(tmpDir) })

	dirs := datadir.New(tmpDir)
	rawDb := mdbx.New(dbcfg.ChainDB, lgr).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(rawDb.Close)

	agg, err := dbstate.NewTest(dirs).StepSize(16).Logger(lgr).Open(context.Background(), rawDb)
	require.NoError(t, err)
	t.Cleanup(agg.Close)

	db, err := temporal.New(rawDb, agg)
	require.NoError(t, err)

	tx, err := db.BeginTemporalRw(context.Background()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	domains, err := execctx.NewSharedDomains(context.Background(), tx, lgr)
	require.NoError(t, err)
	t.Cleanup(domains.Close)

	return tx, domains
}

// TestCrossBlockTimingRace demonstrates that rs.accounts (StateV3Buffered.accounts)
// allows block N+1 workers to read block N's state even when SharedDomains has
// not yet been updated by the async applyResults goroutine.
//
// Current behaviour (Phase 1 baseline):
//   - BufferedWriter.UpdateAccountData writes to rs.accounts synchronously
//     during finalize(), before the applyResults channel is sent.
//   - Block N+1 workers use NewBufferedReader, which checks rs.accounts before
//     falling back to the (potentially stale) domain reader.
//   - This test PASSES with the current code because rs.accounts is populated.
//
// Phase 3 (#19702) will make this test pass via synchronous domain apply
// instead of rs.accounts, then remove rs.accounts entirely.
func TestCrossBlockTimingRace(t *testing.T) {
	if testing.Short() {
		t.Skip("requires mdbx")
	}

	tx, domains := setup2CacheTest(t)

	addr := accounts.InternAddress(common.HexToAddress("0xF00D"))
	lgr := log.New()

	rs := state.NewStateV3Buffered(state.NewStateV3(domains, ethconfig.Sync{}, lgr))

	// Simulate block N's finalize(): BufferedWriter writes to rs.accounts
	// synchronously, before the async applyResults channel fires.
	// Domains are NOT updated yet.
	bw := state.NewBufferedWriter(rs, nil)
	blockNAccount := &accounts.Account{Balance: *uint256.NewInt(500), Nonce: 7}
	original := &accounts.Account{}
	err := bw.UpdateAccountData(addr, original, blockNAccount)
	require.NoError(t, err)

	// Block N+1 worker reads via NewBufferedReader.
	// The base reader points at domains, which still have zero balance.
	baseReader := state.NewReaderV3(domains.AsGetter(tx))
	bufferedRdr := state.NewBufferedReader(rs, baseReader)
	ibsN1 := state.New(bufferedRdr)

	// Must read block N's value from rs.accounts, not stale domains.
	gotBal, err := ibsN1.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(500), gotBal.Uint64(),
		"block N+1 worker must read block N's balance from rs.accounts before domain apply")

	gotNonce, err := ibsN1.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(7), gotNonce,
		"block N+1 worker must read block N's nonce from rs.accounts before domain apply")

	// Sanity check: a plain domain reader (no buffering) still sees zero —
	// the timing hole is real without rs.accounts.
	ibsRaw := state.New(state.NewReaderV3(domains.AsGetter(tx)))
	rawBal, err := ibsRaw.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(0), rawBal.Uint64(),
		"plain domain reader must NOT see block N's balance yet (the timing hole)")

	// Simulate ApplyTxState completing (domain apply catches up).
	w := state.NewWriter(domains.AsPutDel(tx), nil, 5)
	ibsApply := state.New(state.NewReaderV3(domains.AsGetter(tx)))
	ibsApply.SetTxContext(1, 0)
	err = ibsApply.SetBalance(addr, *uint256.NewInt(500), tracing.BalanceChangeUnspecified)
	require.NoError(t, err)
	err = ibsApply.SetNonce(addr, 7)
	require.NoError(t, err)
	err = ibsApply.FinalizeTx(&chain.Rules{}, w)
	require.NoError(t, err)

	// After domain apply, plain reader must see the correct value.
	ibsRawAfter := state.New(state.NewReaderV3(domains.AsGetter(tx)))
	rawBalAfter, err := ibsRawAfter.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(500), rawBalAfter.Uint64(),
		"after domain apply, plain reader must see block N's balance")
}
