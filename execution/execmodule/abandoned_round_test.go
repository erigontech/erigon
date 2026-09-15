package execmodule_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

type roundHarness struct {
	t    *testing.T
	ctx  context.Context
	m    *execmoduletester.ExecModuleTester
	keys [2]*ecdsa.PrivateKey
}

func newRoundHarness(t *testing.T) *roundHarness {
	h := &roundHarness{t: t, ctx: t.Context()}
	for i := range h.keys {
		k, err := crypto.GenerateKey()
		require.NoError(t, err)
		h.keys[i] = k
	}
	funds := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc: types.GenesisAlloc{
			crypto.PubkeyToAddress(h.keys[0].PublicKey): {Balance: funds},
			crypto.PubkeyToAddress(h.keys[1].PublicKey): {Balance: funds},
		},
	}
	h.m = execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(h.keys[0]))
	return h
}

func (h *roundHarness) attrs(parent *types.Header) (*builder.Parameters, execmodule.FlashblockInputs) {
	var beaconRoot common.Hash
	params := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time + 1,
		Withdrawals:           []*types.Withdrawal{},
		ParentBeaconBlockRoot: &beaconRoot,
	}
	return params, execmodule.FlashblockInputs{
		Parent:                parent.Hash(),
		Number:                parent.Number.Uint64() + 1,
		GasLimit:              parent.GasLimit,
		BaseFee:               *misc.CalcBaseFee(h.m.ChainConfig, parent),
		Timestamp:             params.Timestamp,
		ParentBeaconBlockRoot: beaconRoot,
		Withdrawals:           params.Withdrawals,
	}
}

func (h *roundHarness) round(ctx context.Context, in execmodule.FlashblockInputs, key int, nonce uint64) error {
	k := h.keys[key]
	tx, err := types.SignTx(
		types.NewTransaction(nonce, crypto.PubkeyToAddress(k.PublicKey), uint256.NewInt(1), 50_000, uint256.NewInt(h.m.Genesis.BaseFee().Uint64()), nil),
		*types.LatestSignerForChainID(h.m.ChainConfig.ChainID), k)
	require.NoError(h.t, err)
	var buf bytes.Buffer
	require.NoError(h.t, tx.MarshalBinary(&buf))
	_, _, vr, err := h.m.ExecModule.PreExecuteFlashblock(ctx, in, [][]byte{buf.Bytes()})
	if err == nil && vr.ValidationStatus != execmodule.ExecutionStatusSuccess {
		h.t.Fatalf("round for block %d: %v %s", in.Number, vr.ValidationStatus, vr.ValidationError)
	}
	return err
}

func (h *roundHarness) sealAndAdopt(params *builder.Parameters, wantTxs int) *types.Header {
	h.t.Helper()
	br, err := h.m.ExecModule.SealBlock(h.ctx, params, false)
	require.NoError(h.t, err)
	require.NotNil(h.t, br)
	require.Len(h.t, br.Block.Transactions(), wantTxs)
	hdr := br.Block.Header()
	vr, err := validateChain(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "newPayload: %s", vr.ValidationError)
	ur, err := updateForkChoice(h.ctx, h.m.ExecModule, hdr)
	require.NoError(h.t, err)
	require.Equal(h.t, execmodule.ExecutionStatusSuccess, ur.Status)
	return hdr
}

// A round cut at its deadline gives the executor back at the deadline, not when its workers have finished shutting
// down: its results are never used, so nothing has to wait for them — least of all the seal. live40: a round cut on
// time still held the executor through a 2.3s shutdown, the seal behind it missed its slot, and the same heavy
// heights were orphaned over and over.
func TestAbandonedRoundReleasesTheExecutorAtItsDeadline(t *testing.T) {
	h := newRoundHarness(t)
	head, err := h.m.ExecModule.CurrentHeader(h.ctx)
	require.NoError(t, err)
	params, in := h.attrs(head)
	require.NoError(t, h.round(h.ctx, in, 0, 0))

	const stall = 2 * time.Second
	prevStall, prevEvery := dbg.ExecShutdownStall, dbg.ExecShutdownStallEvery
	dbg.ExecShutdownStall, dbg.ExecShutdownStallEvery = stall, 1
	restore := func() { dbg.ExecShutdownStall, dbg.ExecShutdownStallEvery = prevStall, prevEvery }
	defer restore()

	// As the driver runs it: on its own goroutine, with a deadline and a commit claim the caller takes when it
	// stops waiting.
	var outcome atomic.Int32
	roundCtx, cancel := context.WithTimeout(h.ctx, 200*time.Millisecond)
	defer cancel()
	claimed := execmodule.WithRoundCommit(roundCtx, func() bool { return outcome.CompareAndSwap(0, 1) })
	result := make(chan error, 1)
	go func() { result <- h.round(claimed, in, 1, 0) }()
	<-roundCtx.Done()
	require.True(t, outcome.CompareAndSwap(0, 2), "the round committed before its deadline: the stall never held it")
	restore()

	start := time.Now()
	hdr := h.sealAndAdopt(params, 1)
	took := time.Since(start)
	require.Less(t, took, stall/2, "the seal waited %s behind a round abandoned at its deadline", took)

	select {
	case err := <-result:
		require.ErrorIs(t, err, execmodule.ErrRoundAbandoned)
	case <-time.After(2 * stall):
		t.Fatal("the abandoned round never returned")
	}

	// Once its shutdown has run out, the chain goes on, and the abandoned transaction still applies.
	time.Sleep(stall + 500*time.Millisecond)
	params, in = h.attrs(hdr)
	require.NoError(t, h.round(h.ctx, in, 1, 0))
	h.sealAndAdopt(params, 1)
}
