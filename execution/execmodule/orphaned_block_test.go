package execmodule_test

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

// noopAssembler stands in for the DAG driver so AssembleBlock/GetAssembledBlock take the boundary path; the
// test drives pre-execution and the seal itself.
type noopAssembler struct{}

func (noopAssembler) NewPayloadAttrs(context.Context, *builder.Parameters) error   { return nil }
func (noopAssembler) AssembleBlock(context.Context, *builder.Parameters) error     { return nil }
func (noopAssembler) GetAssembledBlock(context.Context, *builder.Parameters) error { return nil }

type orphanHarness struct {
	t    *testing.T
	ctx  context.Context
	m    *execmoduletester.ExecModuleTester
	sign func(nonce uint64) []byte
}

func newOrphanHarness(t *testing.T) *orphanHarness {
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privKey.PublicKey)
	cfg := *chain.AllProtocolChanges
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	m.ExecModule.SetBlockAssembler(noopAssembler{})
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	return &orphanHarness{t: t, ctx: t.Context(), m: m, sign: func(nonce uint64) []byte {
		tx, serr := types.SignTx(
			types.NewTransaction(nonce, sender, uint256.NewInt(1), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*signer, privKey)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes()
	}}
}

func (h *orphanHarness) attrs(parent *types.Header, timestamp uint64) (*builder.Parameters, execmodule.FlashblockInputs) {
	var beaconRoot common.Hash
	params := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             timestamp,
		Withdrawals:           []*types.Withdrawal{},
		ParentBeaconBlockRoot: &beaconRoot,
	}
	return params, execmodule.FlashblockInputs{
		Parent:                parent.Hash(),
		Number:                parent.Number.Uint64() + 1,
		GasLimit:              parent.GasLimit,
		BaseFee:               *misc.CalcBaseFee(h.m.ChainConfig, parent),
		Timestamp:             timestamp,
		ParentBeaconBlockRoot: beaconRoot,
		Withdrawals:           params.Withdrawals,
	}
}

func (h *orphanHarness) preExec(in execmodule.FlashblockInputs, nonces ...uint64) {
	h.t.Helper()
	rlps := make([][]byte, 0, len(nonces))
	for _, n := range nonces {
		rlps = append(rlps, h.sign(n))
	}
	_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(h.ctx, in, rlps)
	require.NoError(h.t, err, "pre-exec block %d", in.Number)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "pre-exec block %d: %s", in.Number, vr.ValidationError)
}

func (h *orphanHarness) seal(params *builder.Parameters, wantTxs int) *types.Header {
	h.t.Helper()
	br, err := h.m.ExecModule.SealBlock(h.ctx, params, false)
	require.NoError(h.t, err)
	require.NotNil(h.t, br)
	require.Len(h.t, br.Block.Transactions(), wantTxs)
	return br.Block.Header()
}

func (h *orphanHarness) adopt(hdr *types.Header) {
	h.t.Helper()
	vr, err := validateChain(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "newPayload block %d: %s", hdr.Number.Uint64(), vr.ValidationError)
	ur, err := updateForkChoice(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, ur.Status, "fork choice block %d", hdr.Number.Uint64())
}

func (h *orphanHarness) getPayload(id uint64) *types.Header {
	h.t.Helper()
	for i := 0; i < 600; i++ {
		got, err := h.m.ExecModule.GetAssembledBlock(h.ctx, id)
		require.NoError(h.t, err)
		if !got.Busy {
			if got.Block == nil {
				return nil
			}
			return got.Block.Block.Header()
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("GetAssembledBlock(%d) stayed busy", id)
	return nil
}

// The consensus layer passed over a block this node sealed — it was late for its slot — and asks for that height
// again on the same parent, for a later slot. The sealed block is an orphan: its state must not survive into the
// block that replaces it. Before, the re-opened block merged into the orphan's state ("can't merge backwards")
// on every round, so the height could never be produced again and the chain stopped (live38, block 4).
func TestOrphanedSealedBlockIsReplacedWhenItsHeightReopens(t *testing.T) {
	h := newOrphanHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)

	late, lateIn := h.attrs(head, head.Time+1)
	h.preExec(lateIn, 0, 1)
	orphan := h.seal(late, 2)
	// The successor opens on it straight away, as the driver does after every seal.
	_, nextIn := h.attrs(orphan, orphan.Time+1)
	h.preExec(nextIn, 2)

	again, againIn := h.attrs(head, head.Time+3)
	h.preExec(againIn, 0, 1)
	replaced := h.seal(again, 2)
	require.NotEqual(t, orphan.Hash(), replaced.Hash())
	require.Equal(t, again.Timestamp, replaced.Time, "the height is sealed under the slot that asked for it")
	h.adopt(replaced)

	after, afterIn := h.attrs(replaced, replaced.Time+1)
	h.preExec(afterIn, 2)
	h.adopt(h.seal(after, 1))
}

// The same, arriving through the consensus layer's assemble: the re-anchor must drop the orphan, and the
// payload it asks for must never be answered with the orphan it passed over.
func TestAssembleOnAnEarlierParentOrphansTheSealedBlock(t *testing.T) {
	h := newOrphanHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)

	late, lateIn := h.attrs(head, head.Time+1)
	_, err = h.m.ExecModule.AssembleBlock(h.ctx, late)
	require.NoError(t, err)
	h.preExec(lateIn, 0, 1)
	orphan := h.seal(late, 2)
	_, nextIn := h.attrs(orphan, orphan.Time+1)
	h.preExec(nextIn, 2)

	again, againIn := h.attrs(head, head.Time+3)
	ab, err := h.m.ExecModule.AssembleBlock(h.ctx, again)
	require.NoError(t, err)
	require.Nil(t, h.getPayload(ab.PayloadID), "the orphan is not served for a slot it was not built for")

	h.preExec(againIn, 0, 1)
	h.seal(again, 2)
	got := h.getPayload(ab.PayloadID)
	require.NotNil(t, got, "the re-sealed block is served for the slot")
	require.Equal(t, again.Timestamp, got.Time)
	h.adopt(got)
}

// A seal that completes AFTER the consensus layer has already asked for the height again, same parent, later
// slot: the block it stores was built for the slot that passed. It must not be served for the new one, and the
// height must re-open cleanly.
func TestSealLandingAfterTheNextAssembleIsNotServed(t *testing.T) {
	h := newOrphanHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)

	late, lateIn := h.attrs(head, head.Time+1)
	_, err = h.m.ExecModule.AssembleBlock(h.ctx, late)
	require.NoError(t, err)
	h.preExec(lateIn, 0, 1)

	again, againIn := h.attrs(head, head.Time+3)
	ab, err := h.m.ExecModule.AssembleBlock(h.ctx, again)
	require.NoError(t, err)
	h.seal(late, 2)
	require.Nil(t, h.getPayload(ab.PayloadID), "a block sealed for the previous slot is not served for this one")

	h.preExec(againIn, 0, 1)
	h.seal(again, 2)
	got := h.getPayload(ab.PayloadID)
	require.NotNil(t, got)
	require.Equal(t, again.Timestamp, got.Time)
	h.adopt(got)
}
