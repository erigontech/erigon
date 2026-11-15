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

package deriver_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// transfer signs the user's transfer with nonce.
func (b *stateGasBlock) transfer(t *testing.T, nonce uint64) types.Transaction {
	t.Helper()
	to := common.HexToAddress("0x00000000000000000000000000000000000b0b0b")
	txn, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: nonce, GasLimit: params.TxGas, To: &to, Value: *uint256.NewInt(1)},
		ChainID:  *b.genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *types.LatestSignerForChainID(b.genesis.Config.ChainID), derivertest.UserKey)
	require.NoError(t, err)
	return txn
}

// TestExtendBuildsEveryTransactionOnce: a batch of more transactions than the
// builder asks its provider for at once makes a block of each, once, in order.
func TestExtendBuildsEveryTransactionOnce(t *testing.T) {
	const count = 60
	b := newStateGasBlock(t, 1, false)
	b.txs = make([]types.Transaction, count)
	want := make([]common.Hash, count)
	for nonce := range b.txs {
		b.txs[nonce] = b.transfer(t, uint64(nonce))
		want[nonce] = b.txs[nonce].Hash()
	}
	_, built := b.build(t, func(e *eezrules.Engine) rules.Engine { return headerless{e} })
	got := make([]common.Hash, 0, count)
	for _, txn := range built.Block.Transactions() {
		got = append(got, txn.Hash())
	}
	require.Equal(t, want, got)
}

// extend derives block 1 of b's chain from txs under ctx.
func (b *stateGasBlock) extend(t *testing.T, ctx context.Context, txs ...types.Transaction) (*types.BlockWithReceipts, error) {
	t.Helper()
	_, chain := b.chain(t, 0, func(e *eezrules.Engine) rules.Engine { return headerless{e} })
	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	l2 := b.genesis.Config.L2.(*config.L2Config)
	return chain.Extend(ctx, parent, deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, txs, common.Hash{}, common.Hash{})
}

// TestExtendRejectsUnincludableTransaction: a transaction the block cannot
// include rejects the block instead of failing the build like an outage.
func TestExtendRejectsUnincludableTransaction(t *testing.T) {
	b := newStateGasBlock(t, 1, false)
	_, err := b.extend(t, t.Context(), b.transfer(t, 0), b.transfer(t, 2))
	require.ErrorIs(t, err, deriver.ErrRejected)
	require.ErrorContains(t, err, "nonce too high")
}

// slowPrepare is the EEZ engine taking delay to prepare a block's header.
type slowPrepare struct {
	headerless
	delay time.Duration
}

func (e slowPrepare) Prepare(chain rules.ChainHeaderReader, header *types.Header) error {
	time.Sleep(e.delay)
	return e.headerless.Prepare(chain, header)
}

// TestExtendOutlastsTheSlotBudget: a derived block is built past the budget of
// its slot, since L1 already fixed it; the slot paces only production.
func TestExtendOutlastsTheSlotBudget(t *testing.T) {
	b := newStateGasBlock(t, 1, false)
	// Each Prepare alone takes twice what the exec module allows the build of a
	// past slot: a quarter slot, then half a slot of grace.
	delay := time.Duration(b.genesis.Config.SecondsPerSlot()) * time.Second * 3 / 2
	b.txs = []types.Transaction{b.transfer(t, 0)}
	_, built := b.build(t, func(e *eezrules.Engine) rules.Engine { return slowPrepare{headerless{e}, delay} })
	require.Len(t, built.Block.Transactions(), 1)
}

// callbackContext counts the callbacks registered on it that are still live.
// Its own Done channel makes context.AfterFunc and children register here.
type callbackContext struct {
	context.Context
	done chan struct{}
	mu   sync.Mutex
	live int
}

func (c *callbackContext) Done() <-chan struct{} { return c.done }

func (c *callbackContext) AfterFunc(func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.live++
	var once sync.Once
	return func() bool {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.live--
		})
		return true
	}
}

// TestExtendReleasesItsContext: a derived block leaves nothing on the
// follower's context, which lives as long as the follower.
func TestExtendReleasesItsContext(t *testing.T) {
	b := newStateGasBlock(t, 1, false)
	ctx := &callbackContext{Context: context.Background(), done: make(chan struct{})}
	_, err := b.extend(t, ctx, b.transfer(t, 0))
	require.NoError(t, err)
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	require.Zero(t, ctx.live)
}

// drip is a pool yielding one batch per request, each after delay, until it
// runs dry; senders recovered as a pool's are.
type drip struct {
	mu      sync.Mutex
	batches [][]types.Transaction
	delay   time.Duration
}

func (b *stateGasBlock) drip(t *testing.T, delay time.Duration, sizes ...int) *drip {
	t.Helper()
	signer := types.LatestSignerForChainID(b.genesis.Config.ChainID)
	d := &drip{delay: delay}
	var nonce uint64
	for _, size := range sizes {
		var batch []types.Transaction
		for range size {
			tx := b.transfer(t, nonce)
			sender, err := signer.Sender(tx)
			require.NoError(t, err)
			tx.SetSender(sender)
			batch = append(batch, tx)
			nonce++
		}
		d.batches = append(d.batches, batch)
	}
	return d
}

func (d *drip) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.batches) == 0 {
		return nil, nil
	}
	time.Sleep(d.delay)
	batch := d.batches[0]
	d.batches = d.batches[1:]
	return batch, nil
}

// produce builds block 1 from txs as the sequencer does.
func (b *stateGasBlock) produce(t *testing.T, wrap func(*eezrules.Engine) rules.Engine, txs txnprovider.TxnProvider) *types.BlockWithReceipts {
	t.Helper()
	_, chain := b.chain(t, 0, wrap)
	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	l2 := b.genesis.Config.L2.(*config.L2Config)
	built, err := chain.Build(t.Context(), deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, txs, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	return built
}

// TestBuildOutlastsTheSlotBudget: a produced block is built to completion
// however long it takes; the Sync block alone answers to the clock.
func TestBuildOutlastsTheSlotBudget(t *testing.T) {
	b := newStateGasBlock(t, 1, false)
	delay := time.Duration(b.genesis.Config.SecondsPerSlot()) * time.Second * 3 / 2
	built := b.produce(t, func(e *eezrules.Engine) rules.Engine { return slowPrepare{headerless{e}, delay} }, b.drip(t, 0, 1))
	require.Len(t, built.Block.Transactions(), 1)
}

// TestBuildDrainsThePool: a produced block takes the pool's transactions
// until the pool runs dry, past a batch the builder filters short.
func TestBuildDrainsThePool(t *testing.T) {
	b := newStateGasBlock(t, 1, false)
	pool := b.drip(t, 100*time.Millisecond, 3, 3)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := types.LatestSignerForChainID(b.genesis.Config.ChainID)
	to := common.HexToAddress("0x00000000000000000000000000000000000b0b0b")
	unfunded, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{GasLimit: params.TxGas, To: &to, Value: *uint256.NewInt(1)},
		ChainID:  *b.genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *signer, key)
	require.NoError(t, err)
	sender, err := signer.Sender(unfunded)
	require.NoError(t, err)
	unfunded.SetSender(sender)
	pool.batches[0] = append(pool.batches[0], unfunded)
	built := b.produce(t, func(e *eezrules.Engine) rules.Engine { return headerless{e} }, pool)
	require.Len(t, built.Block.Transactions(), 6)
}
