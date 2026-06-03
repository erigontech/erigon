package stagedsync

import (
	"context"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// schedTestTask wraps TxTask so processRequest's state-cache validation
// (which needs SharedDomains) is skipped; scheduling behaviour is unchanged.
type schedTestTask struct {
	*exec.TxTask
}

func newSchedTask(txIndex int, txs []types.Transaction) *schedTestTask {
	return &schedTestTask{
		TxTask: &exec.TxTask{
			Header:  &types.Header{Number: *uint256.NewInt(1)},
			TxNum:   uint64(txIndex + 2),
			TxIndex: txIndex,
			Txs:     txs,
		},
	}
}

func transferTx(sender accounts.Address, nonce uint64) types.Transaction {
	to := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	txn := &types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 21_000, To: &to, Value: *uint256.NewInt(1)},
		GasPrice: *uint256.NewInt(1),
	}
	txn.SetSender(sender)
	return txn
}

func withNoncePreWrites(t *testing.T, enabled bool) {
	t.Helper()
	prev := dbg.Exec3NoncePreWrites
	dbg.Exec3NoncePreWrites = enabled
	t.Cleanup(func() { dbg.Exec3NoncePreWrites = prev })
}

// processSchedRequest feeds one block of txs (plus the block-end task) through
// processRequest on a quiescent executor with no workers, so the initial
// dispatch wave and the version map can be inspected afterwards.
func processSchedRequest(t *testing.T, txs []types.Transaction) (*parallelExecutor, *blockExecutor) {
	t.Helper()

	tasks := make([]exec.Task, 0, len(txs)+1)
	for i := range txs {
		tasks = append(tasks, newSchedTask(i, txs))
	}
	tasks = append(tasks, newSchedTask(len(txs), txs))

	pe := &parallelExecutor{
		txExecutor: txExecutor{
			blockExecMetrics: newBlockExecMetrics(),
		},
		in: exec.NewQueueWithRetry(len(tasks) + 1),
	}

	require.NoError(t, pe.processRequest(context.Background(), &execRequest{blockNum: 1, tasks: tasks}))

	be := pe.blockExecutors[1]
	require.NotNil(t, be)
	return pe, be
}

func requireNoncePreWrite(t *testing.T, vm *state.VersionMap, sender accounts.Address, txIndex int, txNonce uint64) {
	t.Helper()
	res := vm.Read(sender, state.NoncePath, accounts.NilKey, txIndex+1)
	require.Equal(t, state.MVReadResultDone, res.Status(), "expected a pre-written nonce at txIndex %d", txIndex)
	require.Equal(t, txIndex, res.DepIdx())
	require.Equal(t, 0, res.Incarnation())
	require.Equal(t, txNonce+1, res.Value())
}

func TestProcessRequestPreWritesNonces(t *testing.T) {
	withNoncePreWrites(t, true)
	senderA := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000001"))
	senderB := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000002"))

	txs := []types.Transaction{
		transferTx(senderA, 5),
		transferTx(senderA, 6),
		transferTx(senderA, 7),
		transferTx(senderB, 0),
	}
	pe, be := processSchedRequest(t, txs)

	// No sender-based serialization: every task (incl. block end) is
	// dispatched in the initial wave.
	require.Equal(t, len(txs)+1, pe.in.NewTasksLen())
	for i := range txs {
		require.False(t, be.execTasks.isBlocked(i), "tx %d should not be blocked", i)
	}

	requireNoncePreWrite(t, be.versionMap, senderA, 0, 5)
	requireNoncePreWrite(t, be.versionMap, senderA, 1, 6)
	requireNoncePreWrite(t, be.versionMap, senderA, 2, 7)
	requireNoncePreWrite(t, be.versionMap, senderB, 3, 0)

	// A tx reads the floor below its own index: the same-sender successor
	// sees its predecessor's pre-written output nonce, never its own.
	res := be.versionMap.Read(senderA, state.NoncePath, accounts.NilKey, 2)
	require.Equal(t, state.MVReadResultDone, res.Status())
	require.Equal(t, 1, res.DepIdx())
	require.Equal(t, uint64(7), res.Value())
}

func TestProcessRequestPreWritesNoncesSetCodeTx(t *testing.T) {
	withNoncePreWrites(t, true)
	sender := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000003"))
	to := common.HexToAddress("0x00000000000000000000000000000000000000bb")

	setCodeTx := &types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 3, GasLimit: 100_000, To: &to},
			ChainID:  *uint256.NewInt(1),
		},
		Authorizations: []types.Authorization{{Nonce: 4}},
	}
	setCodeTx.SetSender(sender)

	_, be := processSchedRequest(t, []types.Transaction{transferTx(sender, 2), setCodeTx})

	// The pre-write is the declared tx nonce + 1 regardless of authorizations;
	// a valid self-authorization bump diverges from it and is corrected by the
	// execution write at flush time.
	requireNoncePreWrite(t, be.versionMap, sender, 0, 2)
	requireNoncePreWrite(t, be.versionMap, sender, 1, 3)
}

func TestProcessRequestNoPreWriteForAATxs(t *testing.T) {
	withNoncePreWrites(t, true)
	sender := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000004"))

	aaTx := func(nonce uint64) types.Transaction {
		return &types.AccountAbstractionTransaction{
			Nonce:         nonce,
			ChainID:       uint256.NewInt(1),
			Tip:           uint256.NewInt(1),
			FeeCap:        uint256.NewInt(1),
			GasLimit:      100_000,
			SenderAddress: sender,
		}
	}
	txs := []types.Transaction{aaTx(0), aaTx(1)}
	pe, be := processSchedRequest(t, txs)

	// AA nonce semantics differ (RIP-7560), so AA txs keep the sender-based
	// dependency: the second tx is held out of the initial wave.
	require.Equal(t, 2, pe.in.NewTasksLen())
	require.True(t, be.execTasks.isBlocked(1))
	res := be.versionMap.Read(sender, state.NoncePath, accounts.NilKey, len(txs))
	require.Equal(t, state.MVReadResultNone, res.Status())
}

func TestProcessRequestNoPreWriteForMaxNonce(t *testing.T) {
	withNoncePreWrites(t, true)
	sender := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000005"))

	txs := []types.Transaction{transferTx(sender, math.MaxUint64)}
	_, be := processSchedRequest(t, txs)

	// nonce+1 would overflow; such a tx makes the block invalid, so don't
	// inject a wrapped value into the version map.
	res := be.versionMap.Read(sender, state.NoncePath, accounts.NilKey, len(txs))
	require.Equal(t, state.MVReadResultNone, res.Status())
}

func TestProcessRequestSenderDepsWithPreWritesDisabled(t *testing.T) {
	withNoncePreWrites(t, false)
	sender := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000006"))

	txs := []types.Transaction{transferTx(sender, 0), transferTx(sender, 1)}
	pe, be := processSchedRequest(t, txs)

	require.Equal(t, 2, pe.in.NewTasksLen())
	require.True(t, be.execTasks.isBlocked(1))
	res := be.versionMap.Read(sender, state.NoncePath, accounts.NilKey, len(txs))
	require.Equal(t, state.MVReadResultNone, res.Status())
}
