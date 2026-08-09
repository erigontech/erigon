package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// feeCreditRound mirrors one apply-loop validation round for a single tx:
// calcFees derives the credit, and a non-empty result is folded into the
// recorded write set the way nextResult does it.
type feeCreditRound struct {
	result   *execResult
	task     *taskVersion
	vm       *state.VersionMap
	reader   *mapStateReader
	rules    *chain.Rules
	recorded *state.WriteSet
	// credited tracks the recorded set once a fee merge produced it, which is
	// blockExecutor.feeMergeTemp's job in the apply loop.
	credited *state.WriteSet
}

func newFeeCreditRound(t testing.TB, s *testFinalizeScenario) *feeCreditRound {
	t.Helper()

	result := s.buildExecResult()
	result.TxIn = copyReadSet(s.txIn)
	result.TxOut = copyWrites(s.txOut)

	vm := state.NewVersionMap(nil)
	vm.FlushVersionedWrites(result.TxOut, true, "")

	return &feeCreditRound{
		result:   result,
		task:     result.Task.(*taskVersion),
		vm:       vm,
		reader:   s.makeReader(),
		rules:    s.rules,
		recorded: result.TxOut,
	}
}

// run performs one round and returns the credit calcFees produced, nil when it
// found the recorded set already carries it.
func (r *feeCreditRound) run(t testing.TB) *state.WriteSet {
	t.Helper()

	tip, err := r.result.calcFees(r.task, r.vm, r.reader, r.rules, r.credited)
	require.NoError(t, err)
	if tip.IsEmpty() {
		return nil
	}
	r.recorded = r.recorded.MergeInto(tip)
	r.credited = r.recorded
	return tip
}

func TestCalcFees_SkipsRedundantReCredit(t *testing.T) {
	t.Parallel()
	r := newFeeCreditRound(t, simpleTransferScenario())

	require.NotNil(t, r.run(t), "the first round must credit the tip")
	require.Nil(t, r.run(t),
		"re-crediting a set that already carries this exact credit rebuilds an identical "+
			"write set and re-runs the merge for nothing")
}

func TestCalcFees_ReCreditsWhenPriorBalanceChanged(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	r := newFeeCreditRound(t, s)

	require.NotNil(t, r.run(t), "the first round must credit the tip")

	// A prior tx moved the coinbase balance, so the tip lands on a new base.
	priorBalance := uint256.NewInt(7_000_000)
	r.reader.accounts[s.coinbase] = &accounts.Account{Balance: *priorBalance, CodeHash: accounts.EmptyCodeHash}

	tip := r.run(t)
	require.NotNil(t, tip, "a changed base balance must produce a fresh credit")

	credited := findBalance(tip, s.coinbase)
	require.NotNil(t, credited)
	require.Equal(t, *new(uint256.Int).Add(priorBalance, &s.feeTipped), credited.Val)
}

func TestCalcFees_ReCreditsWhenAddressPathMissing(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	r := newFeeCreditRound(t, s)

	first := r.run(t)
	require.NotNil(t, first, "the first round must credit the tip")

	// A half-recorded credit is not a credit: the balance alone leaves
	// downstream reads without an account record.
	balanceOnly := &state.WriteSet{}
	bw, ok := first.GetBalance(s.coinbase)
	require.True(t, ok)
	balanceOnly.SetBalance(s.coinbase, bw)

	tip, err := r.result.calcFees(r.task, r.vm, r.reader, r.rules, balanceOnly)
	require.NoError(t, err)
	require.False(t, tip.IsEmpty(), "a recorded balance without its AddressPath sibling must be re-credited")
	require.NotNil(t, findAddress(tip, s.coinbase))
}

func TestCalcFees_SkipsRedundantReCreditWithBurntContract(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	s.rules = &chain.Rules{IsSpuriousDragon: true, IsLondon: true}
	s.burntAddr = fAddr("burntcontract")
	s.feeBurnt = *uint256.NewInt(1000)
	s.accts[s.burntAddr] = fMakeAccount(500_000, 0)

	r := newFeeCreditRound(t, s)

	first := r.run(t)
	require.NotNil(t, first, "the first round must credit the tip")
	require.NotNil(t, findBalance(first, s.burntAddr), "London burns to the burnt contract")

	require.Nil(t, r.run(t),
		"both halves of the credit are already recorded, so the round is a no-op")
}

var feeCreditSink *state.WriteSet

func BenchmarkCalcFees(b *testing.B) {
	for _, bc := range []struct {
		name     string
		recredit bool
	}{
		{"first_credit", false},
		{"redundant_recredit", true},
	} {
		b.Run(bc.name, func(b *testing.B) {
			r := newFeeCreditRound(b, simpleTransferScenario())
			var credited *state.WriteSet
			if bc.recredit {
				require.NotNil(b, r.run(b))
				credited = r.credited
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tip, err := r.result.calcFees(r.task, r.vm, r.reader, r.rules, credited)
				if err != nil {
					b.Fatal(err)
				}
				feeCreditSink = tip
			}
		})
	}
}

func TestCreditedWrites(t *testing.T) {
	t.Parallel()
	be := &blockExecutor{feeMergeTemp: map[int]*state.WriteSet{}}
	txOut, merged := &state.WriteSet{}, &state.WriteSet{}

	require.Nil(t, be.creditedWrites(0, txOut),
		"before any fee merge the recorded set is the worker's own output")

	be.recordFeeMerge(0, txOut, merged)
	require.Same(t, merged, be.creditedWrites(0, merged))
	require.Nil(t, be.creditedWrites(0, txOut),
		"a re-execution re-records the worker's TxOut, which carries no credit")
	require.Nil(t, be.creditedWrites(1, merged),
		"another tx's fee-merge product says nothing about this tx")
	require.Nil(t, be.creditedWrites(2, nil),
		"a tx with no writes at all must not read as credited")
}
