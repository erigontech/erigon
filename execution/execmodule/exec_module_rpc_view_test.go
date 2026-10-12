// Copyright 2026 The Erigon Authors
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

package execmodule_test

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// After a bulk InsertBlocks, RPC state served at a published but uncommitted
// head must come from the same generation as that head.
func TestRPCLatestStateMatchesPublishedHeadAfterBulkInsert(t *testing.T) {
	const (
		bulkInsertLength = 17 // above InsertBlocks' flush-to-DB threshold
		fcuHeadIndex     = 15 // a jump of 16 blocks keeps the FCU on the commit-after-publish path
	)

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	recipient := common.Address{0xaa}

	var armed atomic.Bool
	published := make(chan struct{})
	proceed := make(chan struct{})
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.AllProtocolChanges,
			Alloc: types.GenesisAlloc{
				sender:    {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
				recipient: {Balance: big.NewInt(1)}, // pre-existing, so each transfer fits in TxGas
			},
		}),
		execmoduletester.WithKey(key),
		execmoduletester.WithStateTransitionObserver(func(ctx context.Context, point execmodule.StateTransitionPoint) {
			if point != execmodule.StateTransitionOverlayPublished || !armed.CompareAndSwap(true, false) {
				return
			}
			close(published)
			select {
			case <-proceed:
			case <-ctx.Done():
			}
		}),
	)
	m.StateCache.SetPublishedSD(m.Notifications.Events.LatestSD)

	signer := types.LatestSignerForChainID(nil)
	generated, err := m.GenerateChain(bulkInsertLength, func(_ int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), recipient, uint256.NewInt(1), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *signer, key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)

	ctx := t.Context()
	status, err := m.InsertBlocks(ctx, generated.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	fcuHead := generated.Blocks[fcuHeadIndex]
	armed.Store(true)
	fcuDone := make(chan error, 1)
	go func() {
		_, err := m.UpdateForkChoice(ctx, fcuHead.Header())
		fcuDone <- err
	}()
	release := sync.OnceFunc(func() { close(proceed) })
	defer release()

	select {
	case <-published:
	case <-time.After(time.Minute):
		t.Fatal("FCU result was not published")
	}

	sd, _ := m.Notifications.Events.OverlaySnapshot()
	require.NotNil(t, sd)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	pinned := rpchelper.PinToOverlay(tx, sd.BlockOverlay())

	head, err := rpchelper.GetLatestBlockNumber(pinned)
	require.NoError(t, err)
	require.Equal(t, fcuHead.NumberU64(), head)

	reader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, pinned, head, true, -1, m.StateCache, m.BlockReader.TxnumReader())
	require.NoError(t, err)
	acc, err := reader.ReadAccountData(accounts.InternAddress(recipient))
	require.NoError(t, err)
	require.NotNil(t, acc)
	require.Equal(t, *uint256.NewInt(1 + head), acc.Balance, "state does not match reported head %d", head)

	release()
	require.NoError(t, <-fcuDone)
	m.ExecModule.WaitIdle(ctx)
}
