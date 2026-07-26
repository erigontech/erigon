package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// These pin the edge conditions of dependency-ordered validation selection.
// readyForDepOrderValidation is the pure predicate that decides whether a tx
// may be validated out of contiguous order: exec-complete, not-yet-validated,
// all versionMap predecessors validated, and — unless it reads the coinbase —
// free of the all-prior gate. A coinbase reader is implicitly dependent on
// every prior tx (all credit fees to the coinbase) and stays all-prior gated.

// beForSelection builds a blockExecutor with per-tx read-sets, marks a set of
// txs exec-complete, and a set validated. execComplete drives execTasks;
// validated drives validateTasks (contiguously via setComplete order given).
func beForSelection(reads map[int]state.ReadSet, execComplete, validated []int) *blockExecutor {
	io := &state.VersionedIO{}
	for tx, rs := range reads {
		io.RecordReads(state.Version{TxIndex: tx}, rs)
	}
	be := &blockExecutor{blockIO: io}
	for _, tx := range execComplete {
		be.execTasks.setComplete(tx)
	}
	for _, tx := range validated {
		be.validateTasks.setComplete(tx)
	}
	return be
}

var coinbaseAddr = addr(0xc0)

func TestReady_NotExecCompleteOrAlreadyValidated(t *testing.T) {
	be := beForSelection(nil, []int{0}, []int{0})
	require.False(t, be.readyForDepOrderValidation(0, coinbaseAddr), "already-validated tx is not re-selected")

	be2 := beForSelection(nil, nil, nil)
	require.False(t, be2.readyForDepOrderValidation(0, coinbaseAddr), "not-exec-complete tx is not selectable")
}

func TestReady_NonCoinbase_DeepButShallowDeps(t *testing.T) {
	// tx 200's only versionMap dependency is tx1 (validated). Txs 2..199 are
	// exec-complete but unvalidated — under total order tx200 would wait for
	// all of them; dependency-ordered validation releases it now.
	be := beForSelection(map[int]state.ReadSet{
		200: mapReadOn(addr(0xaa), 1),
	}, []int{200}, []int{1})
	require.True(t, be.readyForDepOrderValidation(200, coinbaseAddr),
		"a shallow dep on a validated predecessor is enough regardless of index distance")
}

func TestReady_NonCoinbase_BlockedByUnvalidatedDep(t *testing.T) {
	be := beForSelection(map[int]state.ReadSet{
		5: mapReadOn(addr(0xaa), 3),
	}, []int{5}, nil) // tx3 not validated
	require.False(t, be.readyForDepOrderValidation(5, coinbaseAddr),
		"unvalidated versionMap predecessor blocks selection")
}

func TestReady_CoinbaseReader_StaysAllPriorGated(t *testing.T) {
	// tx5 reads the coinbase and its explicit dep (tx1) is validated, so
	// depsValidated is true — but the coinbase read makes it implicitly
	// dependent on ALL prior txs, so it must wait for the contiguous
	// maxValidated to reach tx4.
	reads := map[int]state.ReadSet{5: mapReadOn(coinbaseAddr, 1)}

	be := beForSelection(reads, []int{5}, []int{0, 1}) // maxValidated == 1 (2..4 unvalidated)
	require.False(t, be.readyForDepOrderValidation(5, coinbaseAddr),
		"coinbase reader blocked until all prior txs validated")

	beReady := beForSelection(reads, []int{5}, []int{0, 1, 2, 3, 4}) // maxValidated == 4 == tx-1
	require.True(t, beReady.readyForDepOrderValidation(5, coinbaseAddr),
		"coinbase reader selectable once every prior tx is validated")
}

func TestReady_SingleTxBlock_Tx0(t *testing.T) {
	// tx0 has no predecessors. Whether or not it reads the coinbase, the
	// all-prior gate (maxValidated >= -1) is trivially satisfied.
	be := beForSelection(nil, []int{0}, nil)
	require.True(t, be.readyForDepOrderValidation(0, coinbaseAddr), "tx0 with no reads is immediately ready")

	beCb := beForSelection(map[int]state.ReadSet{0: mapReadOn(coinbaseAddr, -1)}, []int{0}, nil)
	require.True(t, beCb.readyForDepOrderValidation(0, coinbaseAddr), "tx0 reading coinbase is still ready (no priors)")
}

func TestReady_CascadeReValidation_PredecessorInvalidated(t *testing.T) {
	// tx3 depends on tx1. tx1 validated → tx3 ready. Then tx1 is invalidated
	// (its committed incarnation was wrong): tx3 must no longer be selectable
	// on the stale predecessor — dependency-ordered validation must re-gate it.
	addrA := addr(0xaa)
	be := beForSelection(map[int]state.ReadSet{
		3: mapReadOn(addrA, 1),
	}, []int{1, 3}, []int{1})
	require.True(t, be.readyForDepOrderValidation(3, coinbaseAddr), "predecessor validated → ready")

	be.validateTasks.clearComplete(1) // tx1 invalidated / must re-run
	require.False(t, be.readyForDepOrderValidation(3, coinbaseAddr),
		"once the predecessor is un-validated, the dependent must not be committed on it")
}

// Ordering-sensitive correctness (SD→recreate, CREATE2 collision) is enforced
// by versionMap ValidateVersion, which is unchanged by dependency-ordered
// selection: a dependent that read a predecessor's pre-recreate state records a
// MapRead on it, so it cannot be selected until that predecessor is validated,
// and if the predecessor's committed incarnation later changes the dependent is
// re-gated (see the cascade test). This property is exercised end-to-end by the
// harness data-check and the eest SD/CREATE2 suites; the selection-level
// invariant it relies on is: a MapRead predecessor gates selection.
func TestReady_SelfDestructRecreate_GatedByMapReadPredecessor(t *testing.T) {
	// tx7 read account X at the incarnation produced by tx4 (the recreate).
	// Until tx4 is validated, tx7 cannot be selected — so it can never commit
	// against a stale (pre-recreate) view.
	x := addr(0x77)
	reads := map[int]state.ReadSet{7: mapReadOn(x, 4)}

	blocked := beForSelection(reads, []int{7}, nil)
	require.False(t, blocked.readyForDepOrderValidation(7, coinbaseAddr), "gated until the recreate tx validates")

	ready := beForSelection(reads, []int{7}, []int{4})
	require.True(t, ready.readyForDepOrderValidation(7, coinbaseAddr), "selectable once the recreate tx is validated")
}

// sanity: coinbaseAddr must be a non-nil, distinct account so ReadsAccount can
// separate coinbase reads from ordinary ones.
func TestReady_CoinbaseAddrDistinct(t *testing.T) {
	require.NotEqual(t, coinbaseAddr, accounts.Address{})
	require.NotEqual(t, coinbaseAddr, addr(0xaa))
}
