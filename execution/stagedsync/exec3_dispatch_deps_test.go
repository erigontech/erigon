package stagedsync

import (
	"context"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type depsTest struct {
	t         *testing.T
	pe        *parallelExecutor
	be        *blockExecutor
	deliverTx func(tx, inc int, reads state.ReadSet)
}

func valueTransfers(senders, recipients []accounts.Address) types.Transactions {
	txs := make(types.Transactions, len(senders))
	for i, sender := range senders {
		to := recipients[i].Value()
		tx := &types.LegacyTx{CommonTx: types.CommonTx{To: &to, Value: *uint256.NewInt(1), GasLimit: 21000}}
		tx.SetSender(sender)
		txs[i] = tx
	}
	return txs
}

// newDepsTest seeds every sender and recipient as a funded account; a recipient in withCode
// also has code.
func newDepsTest(t *testing.T, txs types.Transactions, withCode ...accounts.Address) *depsTest {
	t.Helper()
	chainSpec, _ := chainspec.ChainSpecByName(networkname.Mainnet)
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000}
	db := newResumeTestDB(t)
	seedResumeTestDB(t, db, func(putter kv.TemporalPutDel) error {
		put := func(addr common.Address, hasCode bool) error {
			acc := accounts.NewAccount()
			acc.Balance = *uint256.NewInt(1_000_000)
			if hasCode {
				acc.CodeHash = accounts.InternCodeHash(common.Hash{1})
			}
			return putter.DomainPut(kv.AccountsDomain, addr[:], accounts.SerialiseV3(&acc), 0, nil)
		}
		for _, txn := range txs {
			sender, _ := txn.GetSender()
			if err := put(sender.Value(), false); err != nil {
				return err
			}
			if to := txn.GetTo(); to != nil {
				if err := put(*to, slices.Contains(withCode, accounts.InternAddress(*to))); err != nil {
					return err
				}
			}
		}
		return nil
	})
	pe, roTx := newResumeTestExec(t, db, chainSpec.Config)
	workersCtx, cancel := context.WithCancel(context.Background())
	cancel()
	pe.workersCtx = workersCtx
	pe.pool = newTaskPool(pe, nil)
	t.Cleanup(pe.runWG.Wait)
	be := newBlockExec(1, common.Hash{}, new(protocol.GasPool).AddGas(10_000_000), nil, discardConsumers(16), false, nil)
	pe.blockExecutors = map[uint64]*blockExecutor{1: be}
	tasks := make([]exec.Task, len(txs))
	for i := range txs {
		tasks[i] = &exec.TxTask{
			Header:          header,
			EvmBlockContext: evmtypes.BlockContext{BlockNumber: 1},
			TxNum:           uint64(i + 1),
			TxIndex:         i,
			Config:          chainSpec.Config,
			Txs:             txs,
		}
	}
	require.NoError(t, pe.processRequest(context.Background(), &execRequest{blockNum: 1, tasks: tasks}, roTx))
	be.scheduleExecution(context.Background(), pe)
	d := &depsTest{t: t, pe: pe, be: be}
	d.deliverTx = func(tx, inc int, reads state.ReadSet) {
		res := &exec.TxResult{
			Task: &taskVersion{
				execTask: be.tasks[tx],
				version:  state.Version{BlockNum: 1, TxIndex: tx, Incarnation: state.Incarnation(inc), TxNum: uint64(tx + 1)},
			},
			TxIn:            reads,
			TxOut:           &state.WriteSet{},
			ExecutionResult: evmtypes.ExecutionResult{ReceiptGasUsed: 21000},
		}
		blockRes, err := be.nextResult(context.Background(), pe, res, roTx)
		require.NoError(t, err)
		require.Nil(t, blockRes)
	}
	return d
}

func TestValueTransferWaitsForRecipientPredecessor(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xa0"))
	b := accounts.InternAddress(common.HexToAddress("0xb1"))
	for _, tc := range []struct {
		name       string
		senders    []accounts.Address
		recipients []accounts.Address
	}{
		{"self_transfer", []accounts.Address{a, b, b}, []accounts.Address{a, b, b}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newDepsTest(t, valueTransfers(tc.senders, tc.recipients))
			require.True(t, s.be.selfLoopDispatched[0] && s.be.selfLoopDispatched[1])
			require.True(t, s.be.execTasks.isBlocked(2), "successor schedulable alongside its predecessor")
			require.False(t, s.be.selfLoopDispatched[2], "successor dispatched alongside its predecessor")

			s.deliverTx(0, 0, state.ReadSet{})
			require.True(t, s.be.validateTasks.checkComplete(0))
			require.True(t, s.be.execTasks.isBlocked(2), "successor released by an unrelated transaction's result")
			require.False(t, s.be.selfLoopDispatched[2], "successor dispatched after an unrelated transaction's result")

			s.deliverTx(1, 0, state.ReadSet{})
			require.True(t, s.be.validateTasks.checkComplete(1))
			require.False(t, s.be.execTasks.isBlocked(2))
			require.True(t, s.be.selfLoopDispatched[2], "successor not dispatched after its predecessor's result")
		})
	}
}

func TestRecipientDependencyNeedsValueAndRecipient(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xa0"))
	b := accounts.InternAddress(common.HexToAddress("0xb1"))
	d := accounts.InternAddress(common.HexToAddress("0xd3"))
	zeroValue := valueTransfers([]accounts.Address{a, b}, []accounts.Address{d, d})
	zeroValue[1].(*types.LegacyTx).Value = uint256.Int{}
	creation := valueTransfers([]accounts.Address{a, b}, []accounts.Address{d, d})
	creation[1].(*types.LegacyTx).To = nil
	for name, txs := range map[string]types.Transactions{"zero_value": zeroValue, "contract_creation": creation} {
		t.Run(name, func(t *testing.T) {
			s := newDepsTest(t, txs)
			require.False(t, s.be.execTasks.isBlocked(1), "tx without a value transfer waits on the recipient's previous tx")
			require.True(t, s.be.selfLoopDispatched[1])
		})
	}
}

// A sender charged at validation is charged after every lower tx, so it need not wait
// for an earlier transfer to it.
func TestChargedSenderDoesNotWaitForTransferToIt(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xa0"))
	b := accounts.InternAddress(common.HexToAddress("0xb1"))
	d := accounts.InternAddress(common.HexToAddress("0xd3"))
	e := accounts.InternAddress(common.HexToAddress("0xe4"))
	f := accounts.InternAddress(common.HexToAddress("0xf5"))
	s := newDepsTest(t, valueTransfers([]accounts.Address{a, b, d}, []accounts.Address{f, d, e}))
	require.False(t, s.be.execTasks.isBlocked(2))
	require.True(t, s.be.selfLoopDispatched[2])
}

// A value transfer charged at validation credits its recipient after every lower tx, so it
// need not wait for the recipient's previous tx.
func TestChargedValueTransferDoesNotWaitForRecipient(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xa0"))
	b := accounts.InternAddress(common.HexToAddress("0xb1"))
	c := accounts.InternAddress(common.HexToAddress("0xc2"))
	d := accounts.InternAddress(common.HexToAddress("0xd3"))
	e := accounts.InternAddress(common.HexToAddress("0xe4"))
	f := accounts.InternAddress(common.HexToAddress("0xf5"))
	for name, tc := range map[string][2][]accounts.Address{
		"shared_recipient":      {{a, b, c}, {f, d, d}},
		"sender_then_recipient": {{a, d, c}, {f, e, d}},
	} {
		t.Run(name, func(t *testing.T) {
			s := newDepsTest(t, valueTransfers(tc[0], tc[1]))
			require.False(t, s.be.execTasks.isBlocked(2))
			require.True(t, s.be.selfLoopDispatched[2])
		})
	}
	// A recipient with code runs it in the transfer frame, so the transfer still waits.
	s := newDepsTest(t, valueTransfers([]accounts.Address{a, b, c}, []accounts.Address{f, d, d}), d)
	require.True(t, s.be.execTasks.isBlocked(2))
	require.False(t, s.be.selfLoopDispatched[2])
}

// The run peeks at the recipient's code at block start; validation credits only a recipient that
// still has no code after the lower txs, else the tx reruns with the code executed.
func TestValidationCreditsOnlyCodelessRecipient(t *testing.T) {
	sender := accounts.InternAddress(common.HexToAddress("0xa0"))
	recipient := accounts.InternAddress(common.HexToAddress("0xd3"))
	chainSpec, _ := chainspec.ChainSpecByName(networkname.Mainnet)
	task := &exec.TxTask{
		Header: &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000},
		Config: chainSpec.Config,
		Txs:    valueTransfers([]accounts.Address{sender}, []accounts.Address{recipient}),
	}
	reader := newMapStateReader()
	funded := accounts.NewAccount()
	funded.Balance = *uint256.NewInt(1_000_000)
	reader.accounts[sender] = &funded
	for name, codeHash := range map[string]accounts.CodeHash{"no_code": accounts.EmptyCodeHash, "code": accounts.InternCodeHash(common.Hash{1})} {
		t.Run(name, func(t *testing.T) {
			to := accounts.NewAccount()
			to.CodeHash = codeHash
			reader.accounts[recipient] = &to
			result := &execResult{TxResult: &exec.TxResult{TxOut: &state.WriteSet{}}}
			ws, _, err := chargeSenderAtValidation(task, result, state.Version{}, state.NewVersionMap(nil), reader)
			require.NoError(t, err)
			if codeHash.IsEmpty() {
				credit, ok := ws.GetBalance(recipient)
				require.True(t, ok)
				require.Equal(t, uint64(1), credit.Val.Uint64())
			} else {
				require.Nil(t, ws, "a recipient with code was credited without running it")
			}
		})
	}
}
