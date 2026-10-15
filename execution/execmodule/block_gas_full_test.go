package execmodule_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

// GAS-FULL IS NOT OUT OF GAS, and the block builder has to tell them apart.
//
//   - OUT OF GAS is a TRANSACTION exhausting its own limit. It failed, it gets a receipt saying so,
//     and it goes into the block like any other. Nothing about the block changed.
//   - GAS-FULL is the BLOCK having no room for the next transaction. That transaction is fine — it
//     would execute anywhere with room — so it is EXCLUDED and offered again, and the block ends at
//     the last transaction that fitted.
//
// These run on a block whose gas limit is small enough that a handful of transactions reach it, and
// with transactions that do real work: a transfer spends its intrinsic 21,000 and says nothing about
// a limit being approached from inside execution. burnerCode (round_deadline_test.go) hashes in a
// loop until its gas runs out, so it spends exactly what it is given, in the interpreter.

// gasRig is a block builder on a REDUCED gas limit, with a sender funded to fill it many times over.
type gasRig struct {
	m      *execmoduletester.ExecModuleTester
	ctx    context.Context
	inputs execmodule.FlashblockInputs
}

// newGasRig returns the rig and a burner factory: burn(nonce, gas) is a transaction that will spend
// the whole of `gas`.
func newGasRig(t *testing.T, blockGasLimit uint64) (*gasRig, func(nonce, gas uint64) []byte) {
	t.Helper()
	burner := common.HexToAddress("0x00000000000000000000000000000000000b0817")

	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config:   &cfg,
		GasLimit: blockGasLimit,
		Alloc: types.GenesisAlloc{
			burner: {Balance: new(big.Int), Code: burnerCode},
			crypto.PubkeyToAddress(key.PublicKey): {
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil),
			},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))

	parent := m.Genesis.HeaderNoCopy()
	require.Equal(t, blockGasLimit, parent.GasLimit, "the rig's whole point is the reduced limit")

	rig := &gasRig{
		m:   m,
		ctx: t.Context(),
		inputs: execmodule.FlashblockInputs{
			Parent:                parent.Hash(),
			Number:                parent.Number.Uint64() + 1,
			GasLimit:              parent.GasLimit,
			BaseFee:               *misc.CalcBaseFee(m.ChainConfig, parent),
			Timestamp:             parent.Time + 1,
			ParentBeaconBlockRoot: common.Hash{0x01},
		},
	}
	return rig, burnerTx(t, m, key, burner)
}

func burnerTx(t *testing.T, m *execmoduletester.ExecModuleTester, key *ecdsa.PrivateKey, burner common.Address) func(nonce, gas uint64) []byte {
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	return func(nonce, gas uint64) []byte {
		t.Helper()
		tx, err := types.SignTx(
			types.NewTransaction(nonce, burner, uint256.NewInt(0), gas,
				uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *signer, key)
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes()
	}
}

// A round offered more than the block can hold stops at the last transaction that FITS, and the one
// that did not fit is not in the block.
func TestBlockGasFull_RoundStopsAtTheLastThatFits(t *testing.T) {
	const txGas = 10_000_000
	// Room for exactly three, with change too small for a fourth.
	rig, burn := newGasRig(t, 3*txGas+txGas/2)

	round := make([][]byte, 0, 6)
	for n := uint64(0); n < 6; n++ {
		round = append(round, burn(n, txGas))
	}

	_, _, vr, err := rig.m.ExecModule.PreExecuteFlashblock(rig.ctx, rig.inputs, round)

	var stopped execmodule.ExecutionStopped
	require.ErrorAs(t, err, &stopped, "a block with no room for the next transaction must say so")
	require.Equal(t, execmodule.StopGasFull, stopped.Reason,
		"the block ran out, not the caller's clock — %v", err)
	require.Equal(t, 3, stopped.Applied, "exactly what fitted")
	require.True(t, vr.GasFull)
	require.Equal(t, round[:3], rig.m.ExecModule.FlashBodyForTest(),
		"the block holds what fitted and nothing more")
}

// THE BUDGET RUNS ACROSS ROUNDS. A block is built over many rounds, each its own execution run; a gas
// pool built per run starts every round at the full limit, so each round would believe it had the
// whole block and the body would be built far past the limit with nothing in execution saying so.
func TestBlockGasFull_BudgetRunsAcrossRounds(t *testing.T) {
	const txGas = 10_000_000
	rig, burn := newGasRig(t, 3*txGas+txGas/2)

	// Two fit in round one, leaving room for one more.
	_, _, vr, err := rig.m.ExecModule.PreExecuteFlashblock(rig.ctx, rig.inputs,
		[][]byte{burn(0, txGas), burn(1, txGas)})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	require.False(t, vr.GasFull, "two of three-and-a-half is not full")
	require.Len(t, rig.m.ExecModule.FlashBodyForTest(), 2)

	// Round two is offered three more. If the budget restarted here it would take all three and the
	// block would hold five — 50,000,000 gas against a 35,000,000 limit.
	_, _, vr2, err2 := rig.m.ExecModule.PreExecuteFlashblock(rig.ctx, rig.inputs,
		[][]byte{burn(2, txGas), burn(3, txGas), burn(4, txGas)})

	var stopped execmodule.ExecutionStopped
	require.ErrorAs(t, err2, &stopped, "the second round must find the block full")
	require.Equal(t, execmodule.StopGasFull, stopped.Reason)
	require.Equal(t, 1, stopped.Applied, "only one more fitted")
	require.True(t, vr2.GasFull)
	require.Len(t, rig.m.ExecModule.FlashBodyForTest(), 3,
		"the block's budget did not run across its rounds — it took more than its limit")
}

// A TRANSACTION THAT RUNS OUT OF ITS OWN GAS IS NOT A BLOCK EVENT. It failed, it is in the block with
// a failed receipt, and the round carries on. Confusing the two would end a block every time a caller
// under-estimated a transaction.
func TestBlockGasFull_OutOfGasIsNotGasFull(t *testing.T) {
	const txGas = 2_000_000
	rig, burn := newGasRig(t, 300_000_000)

	// The burner spends everything it is given, so each of these runs out of its OWN gas.
	round := [][]byte{burn(0, txGas), burn(1, txGas), burn(2, txGas)}
	_, _, vr, err := rig.m.ExecModule.PreExecuteFlashblock(rig.ctx, rig.inputs, round)

	require.NoError(t, err, "a transaction exhausting its OWN gas is an ordinary failed transaction")
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	require.False(t, vr.GasFull, "the block had 300M and these asked for 6M")
	require.Equal(t, round, rig.m.ExecModule.FlashBodyForTest(),
		"a failed transaction still belongs in the block")
	require.Equal(t, 3, vr.FlashblockReceiptCount)
}

// The slot ending and the block filling are different endings, and they treat the transaction at the
// boundary oppositely — the clock keeps the last one that completed, gas-full excludes the one that
// did not fit. This pins that they are reported apart.
func TestBlockGasFull_IsDistinctFromTheSlotEnding(t *testing.T) {
	const txGas = 10_000_000
	rig, burn := newGasRig(t, 300_000_000)

	round := make([][]byte, 0, 12)
	for n := uint64(0); n < 12; n++ {
		round = append(round, burn(n, txGas))
	}
	// A budget that expires part way through, on a block with ample gas.
	deadlined, cancel := context.WithTimeout(rig.ctx, 120*time.Millisecond)
	defer cancel()

	_, _, vr, err := rig.m.ExecModule.PreExecuteFlashblock(deadlined, rig.inputs, round)

	var stopped execmodule.ExecutionStopped
	require.ErrorAs(t, err, &stopped)
	require.Equal(t, execmodule.StopSlotEnd, stopped.Reason, "the clock ran out, not the block's gas")
	require.False(t, vr.GasFull)
}
