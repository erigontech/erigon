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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

// burnerCode hashes 32 bytes of memory in a loop until it runs out of gas: JUMPDEST, PUSH1 32, PUSH1 0,
// SHA3, POP, PUSH1 0, JUMP. Whatever gas a call gives it, it spends, in the interpreter, doing real work.
var burnerCode = hexutil.MustDecode("0x5b602060002050600056")

// THE HARD STOP, against the real executor.
//
// The valve rests on one assumption that no amount of reading the driver can settle: that when a round's
// deadline passes, execution actually STOPS. If the EVM runs the round to completion regardless and only
// notices the deadline on the way out, the seal still waits for it and the budget is still missed — the
// valve would be decoration. So this drives genuinely expensive work (a gas burner, in the interpreter,
// not a sleep) through the real module, with a deadline that expires part-way in.
//
// What it pins: the round comes back in roughly the time it was given, not the time the work needed; it
// says it was abandoned rather than failed; and the block is left exactly as it was, so the seal that
// follows is unaffected.
func TestRoundStopsAtItsDeadline(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privKey.PublicKey)
	burner := common.HexToAddress("0x00000000000000000000000000000000000b0817")

	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config:   &cfg,
		GasLimit: 300_000_000,
		Alloc: types.GenesisAlloc{
			sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
			burner: {Balance: new(big.Int), Code: burnerCode},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	exec := m.ExecModule

	parent := m.Genesis.HeaderNoCopy()
	beaconRoot := common.Hash{0x01}
	inputs := execmodule.FlashblockInputs{
		Parent:                parent.Hash(),
		Number:                parent.Number.Uint64() + 1,
		GasLimit:              parent.GasLimit,
		BaseFee:               *misc.CalcBaseFee(m.ChainConfig, parent),
		Timestamp:             parent.Time + 1,
		ParentBeaconBlockRoot: beaconRoot,
	}

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	burn := func(nonce, gas uint64) []byte {
		tx, serr := types.SignTx(
			types.NewTransaction(nonce, burner, uint256.NewInt(0), gas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*signer, privKey)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes()
	}

	// A first round that is allowed to finish, both to give the block a body worth protecting and to
	// measure what this work really costs — the deadline below has to be well inside it to mean anything.
	start := time.Now()
	_, _, vr, err := exec.PreExecuteFlashblock(ctx, inputs, [][]byte{burn(0, 10_000_000)})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	perBurn := time.Since(start)
	t.Logf("one 10M-gas burn costs %s", perBurn.Round(time.Millisecond))
	committed := exec.FlashBodyForTest()
	require.Len(t, committed, 1)

	// Now a round of many burns with a deadline that only covers a fraction of them.
	const burns = 12
	round := make([][]byte, 0, burns)
	for i := uint64(1); i <= burns; i++ {
		round = append(round, burn(i, 10_000_000))
	}
	budget := perBurn * 2
	deadlined, cancel := context.WithDeadline(ctx, time.Now().Add(budget))
	defer cancel()

	start = time.Now()
	_, _, _, err = exec.PreExecuteFlashblock(deadlined, inputs, round)
	took := time.Since(start)

	require.ErrorIs(t, err, execmodule.ErrRoundAbandoned,
		"a round cut off by its deadline must say so, not look like a failed block")
	require.Less(t, took, time.Duration(burns/2)*perBurn,
		"the round ran on past its deadline (%s for work budgeted at %s): the seal would wait for all of it",
		took.Round(time.Millisecond), budget.Round(time.Millisecond))
	require.Equal(t, committed, exec.FlashBodyForTest(),
		"the abandoned round left its transactions in the body")
}
