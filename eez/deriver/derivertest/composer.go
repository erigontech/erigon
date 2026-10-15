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

// Package derivertest composes EEZ devnet L2 chains and the L1 evidence of
// their settlement, for derivation tests.
package derivertest

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
)

// UserKey funds the devnet's second prefunded account.
var UserKey, _ = crypto.HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")

// UserAddress is the account of UserKey.
func UserAddress() common.Address { return crypto.PubkeyToAddress(UserKey.PublicKey) }

// Config is the devnet chain configuration.
func Config() *chain.Config {
	eez.Register()
	return networks.Devnet.Config
}

// NewL2 returns the devnet L2 at genesis in a fresh execution module, and
// the Chain derivation drives it through.
func NewL2(t testing.TB) (*execmoduletester.ExecModuleTester, *deriver.ExecChain) {
	t.Helper()
	eez.Register()
	genesis := *networks.Devnet.Genesis
	engine, err := eezrules.New(genesis.Config)
	require.NoError(t, err)
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&genesis),
		execmoduletester.WithChainConfig(genesis.Config),
		execmoduletester.WithEngine(engine),
		execmoduletester.WithKey(UserKey),
		execmoduletester.WithoutSystemContracts(),
	)
	return m, deriver.NewExecChain(m.ExecModule, m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig)
}

// Block is the content of one composed L2 block.
type Block struct {
	Coinbase common.Address
	Extra    []byte
	// Txs are the block's user transactions. A Sync block's begin with the
	// user transactions of its outbound actions, in order.
	Txs [][]byte
}

// Batch is a composed batch, its blocks built on the composer's chain.
type Batch struct {
	Batch   *protocol.ProofSystemBatch
	Payload *deriver.Payload
	// Blocks are the composer's blocks of the batch's range.
	Blocks []*types.Block
	// Syncs holds the Sync block of entries 0..k at k, the block the root
	// of entry k commits to.
	Syncs []*types.Block
}

// Composer builds an L2 chain and posts its batches to an L1 chain.
type Composer struct {
	t     testing.TB
	L1    *recording.MemoryChain
	L2    *execmoduletester.ExecModuleTester
	Chain *deriver.ExecChain
	l2    *config.L2Config
	// head is the composer's L2 head, settled the latest settled block.
	head, settled *types.Header
	nonce         uint64
}

// NewComposer returns a composer at the devnet genesis and an L1 chain of
// one block.
func NewComposer(t testing.TB) *Composer {
	t.Helper()
	m, chain := NewL2(t)
	l1Chain, err := recording.NewMemoryChain(m.ChainConfig.L2.(*config.L2Config).L1ChainID, &recording.Block{Header: l1.Header{Hash: l1Hash(0)}})
	require.NoError(t, err)
	return &Composer{t: t, L1: l1Chain, L2: m, Chain: chain, l2: m.ChainConfig.L2.(*config.L2Config), head: m.Genesis.Header(), settled: m.Genesis.Header()}
}

// Registry is the devnet registry.
func (c *Composer) Registry() common.Address { return c.l2.Registry }

// Head is the composer's L2 head.
func (c *Composer) Head() *types.Header { return c.head }

// Transfer returns a signed transfer of value wei to to, from the user
// account at its next nonce.
func (c *Composer) Transfer(to common.Address, value uint64) []byte {
	c.t.Helper()
	signer := types.LatestSignerForChainID(c.L2.ChainConfig.ChainID)
	tx, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: c.nonce, GasLimit: 21_000, To: &to, Value: *uint256.NewInt(value)},
		ChainID:  *c.L2.ChainConfig.ChainID,
		TipCap:   *uint256.NewInt(1),
		FeeCap:   *uint256.NewInt(10 * common.GWei),
	}, *signer, UserKey)
	require.NoError(c.t, err)
	c.nonce++
	var buf bytes.Buffer
	require.NoError(c.t, tx.MarshalBinary(&buf))
	return buf.Bytes()
}

// Compose builds blocks on the composer's head, the last one the Sync block
// of actions, and returns the batch settling them from the latest settled
// block. Its entries are an anchor, the outbound actions and the inbound
// ones; each commits to the Sync block holding the effects of the entries up
// to it.
func (c *Composer) Compose(actions []da.Action, blocks ...Block) *Batch {
	c.t.Helper()
	span := &da.Span{}
	for _, b := range blocks {
		span.TxCounts = append(span.TxCounts, uint32(len(b.Txs)))
		span.Beneficiaries = append(span.Beneficiaries, b.Coinbase)
		span.ExtraData = append(span.ExtraData, b.Extra)
		span.Transactions = append(span.Transactions, b.Txs...)
	}
	payload := &deriver.Payload{Span: span, Actions: actions}

	batch := &protocol.ProofSystemBatch{
		ImmediateEntryCount:       big.NewInt(1),
		ImmediateStaticEntryCount: new(big.Int),
		RollupIDsWithProofSystems: []protocol.RollupIDWithProofSystems{{RollupID: c.l2.RollupID, ProofSystemIndexes: []uint64{}}},
		ProofSystems:              []common.Address{},
		BlobIndices:               []*big.Int{},
		Proofs:                    [][]byte{},
		CallData:                  da.Encode((&da.Payload{RollupID: c.l2.RollupID, Operations: span.Encode(), Actions: actions}).Messages()...),
	}
	batch.Entries = append(batch.Entries, protocol.ExecutionEntry{RollupUpdates: []protocol.RollupUpdate{{RollupID: c.l2.RollupID}}})
	var inbound []protocol.ExecutionEntry
	for i := range actions {
		e, err := protocol.EntryFromAction(&actions[i], c.l2.RollupID)
		require.NoError(c.t, err)
		e.RollupUpdates = []protocol.RollupUpdate{{RollupID: c.l2.RollupID}}
		if e.ProxyEntryHash == (common.Hash{}) {
			batch.Entries = append(batch.Entries, *e)
			batch.ImmediateEntryCount.Add(batch.ImmediateEntryCount, big.NewInt(1))
		} else {
			inbound = append(inbound, *e)
		}
	}
	batch.Entries = append(batch.Entries, inbound...)

	out := &Batch{Batch: batch, Payload: payload}
	last := len(blocks) - 1
	for i := range last {
		out.Blocks = append(out.Blocks, c.extend(c.head, span, i, payload.BlockTxs(i)))
		c.head = out.Blocks[i].Header()
	}
	// The root after entry k is the Sync block of entries 0..k: L1 can
	// apply a prefix of the batch.
	nonce, err := c.Chain.Nonce(context.Background(), c.head.Number.Uint64(), protocol.SystemAddress)
	require.NoError(c.t, err)
	all := AppliedAll(batch)
	parent, prev := c.head, c.settled.Hash()
	for k := range batch.Entries {
		sb, err := deriver.BuildSyncBlock(payload, &protocol.Settlement{Applied: all.Applied[:k+1]}, c.syncTxContext(), nonce)
		require.NoError(c.t, err)
		sync := c.extend(parent, span, last, sb.Txs)
		out.Syncs = append(out.Syncs, sync)
		u := &batch.Entries[k].RollupUpdates[0]
		u.EtherDelta, u.CurrentRoot, u.NewRoot = new(big.Int), prev, sync.Hash()
		prev = u.NewRoot
		if k == len(batch.Entries)-1 {
			out.Blocks = append(out.Blocks, sync)
			c.head = sync.Header()
		}
	}
	return out
}

func (c *Composer) extend(parent *types.Header, span *da.Span, i int, txs [][]byte) *types.Block {
	c.t.Helper()
	decoded, err := deriver.DecodeTxs(txs)
	require.NoError(c.t, err)
	built, err := c.Chain.Extend(context.Background(), parent, deriver.DeriveInputs(c.l2, parent, span, i), decoded, common.Hash{}, common.Hash{})
	require.NoError(c.t, err)
	return built.Block
}

func (c *Composer) syncTxContext() protocol.SyncTxContext {
	return protocol.SyncTxContext{ChainID: c.L2.ChainConfig.ChainID.Uint64(), RollupID: c.l2.RollupID}
}

// AppliedAll is the settlement of every entry of b.
func AppliedAll(b *protocol.ProofSystemBatch) *protocol.Settlement {
	s := &protocol.Settlement{}
	var outbound, inbound int
	for i := range b.Entries {
		e := &b.Entries[i]
		a := protocol.AppliedEntry{Index: i}
		switch {
		case e.ProxyEntryHash != (common.Hash{}):
			a.Role, a.Ordinal = protocol.RoleInbound, inbound
			inbound++
		case len(e.L2ToL1Calls) == 0:
			a.Role = protocol.RoleAnchor
		default:
			a.Role, a.Ordinal = protocol.RoleOutbound, outbound
			outbound++
		}
		s.Applied = append(s.Applied, a)
	}
	return s
}

// Resume returns the batch of a composer that saw L1 apply rival's entries
// before from and settles the rest in the Sync block of that applied run:
// the batch's span is sync alone, and each of its later entries commits to
// the rival's Sync block with the effects up to it and sync's own
// transactions appended.
func (c *Composer) Resume(rival *Batch, from int, sync Block) *Batch {
	c.t.Helper()
	ctx := context.Background()
	span := &da.Span{
		TxCounts:      []uint32{uint32(len(sync.Txs))},
		Beneficiaries: []common.Address{sync.Coinbase},
		ExtraData:     [][]byte{sync.Extra},
		Transactions:  sync.Txs,
	}
	payload := &deriver.Payload{Span: span, Actions: rival.Payload.Actions}
	batch := *rival.Batch
	batch.Entries = slices.Clone(rival.Batch.Entries)
	batch.CallData = da.Encode((&da.Payload{RollupID: c.l2.RollupID, Operations: span.Encode(), Actions: payload.Actions}).Messages()...)
	out := &Batch{Batch: &batch, Payload: payload, Syncs: slices.Clone(rival.Syncs[:from])}

	base := rival.Syncs[from-1]
	parent, err := c.Chain.HeaderByNumber(ctx, base.NumberU64()-1)
	require.NoError(c.t, err)
	nonce, err := c.Chain.Nonce(ctx, parent.Number.Uint64(), protocol.SystemAddress)
	require.NoError(c.t, err)
	existing := make([][]byte, len(base.Transactions()))
	for i, tx := range base.Transactions() {
		var buf bytes.Buffer
		require.NoError(c.t, tx.MarshalBinary(&buf))
		existing[i] = buf.Bytes()
	}
	all := AppliedAll(&batch).Applied
	prev := base.Hash()
	for k := from; k < len(batch.Entries); k++ {
		sb, err := deriver.BuildSyncBlock(payload, &protocol.Settlement{Applied: all[from : k+1]}, c.syncTxContext(), nonce)
		require.NoError(c.t, err)
		block := c.extend(parent, span, 0, append(slices.Clone(existing), sb.Txs...))
		out.Syncs = append(out.Syncs, block)
		e := &batch.Entries[k]
		e.RollupUpdates = []protocol.RollupUpdate{{RollupID: c.l2.RollupID, EtherDelta: new(big.Int), CurrentRoot: prev, NewRoot: block.Hash()}}
		prev = block.Hash()
	}
	out.Blocks = []*types.Block{out.Syncs[len(out.Syncs)-1]}
	c.head = out.Blocks[0].Header()
	return out
}

// Posting is a batch to post and the entries of it L1 applies.
type Posting struct {
	Batch *Batch
	// Applied are the indexes of the entries L1 applies; nil applies all.
	Applied []int
}

// Post appends an L1 block holding one posting transaction per posting, in
// order, and the logs of what each applied. The settled block is the one
// the last applied entry of the last posting committed to.
func (c *Composer) Post(postings ...Posting) *recording.Block {
	c.t.Helper()
	head, err := c.L1.Head(context.Background())
	require.NoError(c.t, err)
	n := head.Number + 1
	b := &recording.Block{Header: l1.Header{Number: n, Hash: l1Hash(n), ParentHash: head.Hash, Time: n * c.l2.L1BlockTime}}
	for _, p := range postings {
		input, err := protocol.EncodePostAndVerifyBatch(p.Batch.Batch)
		require.NoError(c.t, err)
		index := len(b.Txs)
		b.Txs = append(b.Txs, types.NewTransaction(n*1000+uint64(index), c.l2.Registry, new(uint256.Int), 10_000_000, new(uint256.Int), input))
		applied := p.Applied
		if applied == nil {
			for i := range p.Batch.Batch.Entries {
				applied = append(applied, i)
			}
		}
		var after []*types.Log
		immediate := int(p.Batch.Batch.ImmediateEntryCount.Int64())
		// An immediate entry that did not apply was skipped.
		for i := range immediate {
			if !slices.Contains(applied, i) {
				skipped, err := protocol.L2TxSkippedLog(c.l2.Registry, big.NewInt(int64(i)), []byte{})
				require.NoError(c.t, err)
				b.Logs = append(b.Logs, c.at(skipped, index))
			}
		}
		for _, i := range applied {
			e := &p.Batch.Batch.Entries[i]
			root, err := protocol.L2ExecutionPerformedLog(c.l2.Registry, c.l2.RollupID, e.RollupUpdates[0].NewRoot, new(big.Int))
			require.NoError(c.t, err)
			if i < immediate {
				b.Logs = append(b.Logs, c.at(root, index))
				continue
			}
			consumed, err := protocol.ExecutionConsumedLog(c.l2.Registry, e.ProxyEntryHash, c.l2.RollupID, big.NewInt(int64(i-immediate)))
			require.NoError(c.t, err)
			after = append(after, c.at(consumed, index), c.at(root, index))
		}
		posted, err := protocol.BatchPostedLog(c.l2.Registry, common.Hash{}, []uint64{c.l2.RollupID})
		require.NoError(c.t, err)
		b.Logs = append(b.Logs, c.at(posted, index))
		b.Logs = append(b.Logs, after...)
	}
	require.NoError(c.t, c.L1.Append(b))
	c.settled = c.settledBy(postings[len(postings)-1])
	return b
}

// settledBy is the block the posting's last applied entry committed to.
func (c *Composer) settledBy(p Posting) *types.Header {
	last := len(p.Batch.Batch.Entries) - 1
	if p.Applied != nil {
		last = p.Applied[len(p.Applied)-1]
	}
	return p.Batch.Syncs[last].Header()
}

// Settle marks block as the latest settled one, the entry state of the next
// composed batch.
func (c *Composer) Settle(h *types.Header) { c.settled = h }

func (c *Composer) at(l *types.Log, txIndex int) *types.Log {
	l.TxIndex = hexutil.Uint(txIndex)
	return l
}

func l1Hash(n uint64) common.Hash {
	return crypto.Keccak256Hash(binary.BigEndian.AppendUint64([]byte("l1"), n))
}

// Scan returns every batch the L1 chain settled for the devnet rollup.
func (c *Composer) Scan() []l1.ScannedBatch {
	c.t.Helper()
	head, err := c.L1.Head(context.Background())
	require.NoError(c.t, err)
	batches, reached, err := l1.NewScanner(c.L1, c.l2.Registry, c.l2.RollupID).Scan(context.Background(), 0, head.Number)
	require.NoError(c.t, err)
	require.Equal(c.t, head.Number, reached)
	return batches
}

// RequireChain checks that chain holds want as its canonical blocks.
func RequireChain(t testing.TB, chain deriver.Chain, want ...*types.Block) {
	t.Helper()
	for _, b := range want {
		got, err := chain.BlockByNumber(context.Background(), b.NumberU64())
		require.NoError(t, err)
		require.NotNil(t, got, "block %d", b.NumberU64())
		require.Equal(t, b.Hash(), got.Hash(), "block %d", b.NumberU64())
	}
}
