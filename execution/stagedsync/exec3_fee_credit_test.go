package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
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
// found the recorded set already carries it. The returned set is a copy: the
// merge folds the recorded set into the credit in place, so the credit itself is
// only observable before it.
func (r *feeCreditRound) run(t testing.TB) *state.WriteSet {
	t.Helper()

	tip, err := r.result.calcFees(r.task, r.vm, r.reader, r.rules, r.credited)
	require.NoError(t, err)
	if tip.IsEmpty() {
		return nil
	}
	credit := copyWrites(tip)
	r.recorded = r.recorded.MergeInto(tip)
	r.credited = r.recorded
	return credit
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
	// Only the balance changes — the rest of the account must stay put, or the
	// test would pass even if recordedIn stopped comparing balances.
	priorBalance := uint256.NewInt(7_000_000)
	r.reader.accounts[s.coinbase].Balance = *priorBalance

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
	s := londonTransferScenario()
	r := newFeeCreditRound(t, s)

	first := r.run(t)
	require.NotNil(t, first, "the first round must credit the tip")
	require.NotNil(t, findBalance(first, s.burntAddr), "London burns to the burnt contract")

	require.Nil(t, r.run(t),
		"both halves of the credit are already recorded, so the round is a no-op")
}

func TestCalcFees_SkipsRedundantReCreditOnEmptyRemoval(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	// A zero tip on an already-empty coinbase is the EIP-161 case: the credit
	// is a delete rather than a balance write, and takes a different path
	// through both recordedIn and writeTo.
	s.feeTipped = uint256.Int{}
	r := newFeeCreditRound(t, s)

	first := r.run(t)
	require.NotNil(t, first, "an emptied coinbase must still be touched")
	sd, ok := first.GetSelfDestruct(s.coinbase)
	require.True(t, ok, "the credit is a SelfDestructPath delete")
	require.True(t, sd.Val)

	require.Nil(t, r.run(t),
		"the delete is already recorded, so the round is a no-op")
}

// TestFeeEntryWriteToIsRecordedIn pins the two halves against each other: if
// recordedIn stops accepting what writeTo produces the skip silently never
// fires, and if it accepts more the credit can be skipped without ever having
// been written.
func TestFeeEntryWriteToIsRecordedIn(t *testing.T) {
	t.Parallel()
	version := state.Version{TxIndex: 3, Incarnation: 1}
	addr := fAddr("credited")

	entries := []*feeEntry{
		{
			addr:   addr,
			acc:    accounts.Account{Balance: *uint256.NewInt(7), Nonce: 2, Incarnation: 1, CodeHash: accounts.EmptyCodeHash},
			reason: tracing.BalanceIncreaseRewardTransactionFee,
		},
		{
			addr:   addr,
			acc:    accounts.Account{Balance: *uint256.NewInt(11), CodeHash: accounts.EmptyCodeHash},
			reason: tracing.BalanceDecreaseGasBuy,
		},
		{addr: addr, deleted: true},
	}

	for i, e := range entries {
		ws := &state.WriteSet{}
		e.writeTo(ws, version)

		require.True(t, e.recordedIn(ws, version),
			"entry %d: recordedIn must accept what writeTo wrote", i)
		require.False(t, e.recordedIn(ws, state.Version{TxIndex: 3, Incarnation: 2}),
			"entry %d: a credit stamped at another incarnation is not this credit", i)
		require.False(t, e.recordedIn(&state.WriteSet{}, version),
			"entry %d: an empty set carries no credit", i)
	}
}

// TestFeeEntryNilIsAbsent pins the absent entry: an adjustment that does not
// touch this address writes nothing and reads as already recorded, so the skip
// turns on the entries that do exist.
func TestFeeEntryNilIsAbsent(t *testing.T) {
	t.Parallel()
	var absent *feeEntry
	ws := &state.WriteSet{}

	absent.writeTo(ws, state.Version{TxIndex: 3})
	require.True(t, ws.IsEmpty(), "an absent entry has nothing to write")
	require.True(t, absent.recordedIn(ws, state.Version{TxIndex: 3}))
}

// TestFeeEntryDeletedRejectsForeignSelfDestruct pins the fence the deleted arm
// rests on: it accepts any SelfDestruct write for its address, so what keeps a
// worker's own SELFDESTRUCT from reading as this credit is the version stamp.
func TestFeeEntryDeletedRejectsForeignSelfDestruct(t *testing.T) {
	t.Parallel()
	addr := fAddr("emptied")
	version := state.Version{TxIndex: 3, Incarnation: 1}
	e := &feeEntry{addr: addr, deleted: true}

	workerWrites := &state.WriteSet{}
	workerWrites.SetSelfDestruct(addr, &state.VersionedWrite[bool]{
		WriteHeader: state.WriteHeader{Address: addr, Path: state.SelfDestructPath},
		Val:         true,
	})
	require.False(t, e.recordedIn(workerWrites, version),
		"a worker's SELFDESTRUCT is stamped with its own version, not the credit's")

	revived := &state.WriteSet{}
	revived.SetSelfDestruct(addr, &state.VersionedWrite[bool]{
		WriteHeader: state.WriteHeader{Address: addr, Path: state.SelfDestructPath, Version: version},
		Val:         false,
	})
	require.False(t, e.recordedIn(revived, version),
		"a SelfDestruct write that is not a delete carries no empty-removal")
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
				// The apply loop recycles the emitted set's maps through
				// recordFeeMerge; without this the pools stay empty and the
				// emit arm is measured against a permanently cold pool.
				tip.ReleaseMaps()
			}
		})
	}
}
