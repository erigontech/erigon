package exec

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type historicalStateReaderStub struct {
	txNum       uint64
	trace       bool
	tracePrefix string
	accounts    map[uint64]map[accounts.Address]*accounts.Account
	latest      map[accounts.Address]*accounts.Account
	readTxNums  []uint64
}

func (r *historicalStateReaderStub) SetTx(kv.TemporalTx) {}

func (r *historicalStateReaderStub) SetTxNum(txNum uint64) {
	r.txNum = txNum
}

func (r *historicalStateReaderStub) SetTrace(trace bool, tracePrefix string) {
	r.trace = trace
	r.tracePrefix = tracePrefix
}

func (r *historicalStateReaderStub) Trace() bool {
	return r.trace
}

func (r *historicalStateReaderStub) TracePrefix() string {
	return r.tracePrefix
}

func (r *historicalStateReaderStub) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	r.readTxNums = append(r.readTxNums, r.txNum)
	if perTx, ok := r.accounts[r.txNum]; ok {
		if acc, ok := perTx[address]; ok && acc != nil {
			copyAcc := *acc
			return &copyAcc, nil
		}
	}
	if acc, ok := r.latest[address]; ok && acc != nil {
		copyAcc := *acc
		return &copyAcc, nil
	}
	return nil, nil
}

func (r *historicalStateReaderStub) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *historicalStateReaderStub) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}

func (r *historicalStateReaderStub) HasStorage(address accounts.Address) (bool, error) {
	return false, nil
}

func (r *historicalStateReaderStub) ReadAccountCode(address accounts.Address) ([]byte, error) {
	return nil, nil
}

func (r *historicalStateReaderStub) ReadAccountCodeSize(address accounts.Address) (int, error) {
	return 0, nil
}

func (r *historicalStateReaderStub) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return 0, nil
}

func TestHistoricalTraceWorker_ReplaysBlockSequentiallyOnNonceMismatch(t *testing.T) {
	t.Parallel()

	engine := ethash.NewFaker()
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	senderAKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderBKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	senderA := accounts.InternAddress(crypto.PubkeyToAddress(senderAKey.PublicKey))
	senderB := accounts.InternAddress(crypto.PubkeyToAddress(senderBKey.PublicKey))
	receiver := common.HexToAddress("0x00000000000000000000000000000000000000ff")

	signer := types.MakeSigner(chain.AllProtocolChanges, 1, 1)
	gasPrice := uint256.NewInt(1)

	tx0, err := types.SignTx(types.NewTransaction(0, receiver, uint256.NewInt(0), 21_000, gasPrice, nil), *signer, senderAKey)
	require.NoError(t, err)
	tx1, err := types.SignTx(types.NewTransaction(6, receiver, uint256.NewInt(0), 21_000, gasPrice, nil), *signer, senderBKey)
	require.NoError(t, err)

	header := &types.Header{
		Number:     *uint256.NewInt(1),
		Time:       1,
		GasLimit:   1_000_000,
		Difficulty: *uint256.NewInt(1),
	}
	blockContext := protocol.NewEVMBlockContext(header, func(uint64) (common.Hash, error) { return common.Hash{}, nil }, engine, accounts.NilAddress, chain.AllProtocolChanges)

	blockStartTxNum := uint64(1)
	currentTxNum := uint64(3)

	accountA := accounts.NewAccount()
	accountA.Nonce = 0
	accountA.Balance.SetUint64(1_000_000)
	accountB := accounts.NewAccount()
	accountB.Nonce = 6
	accountB.Balance.SetUint64(1_000_000)

	newWorker := func(reader *historicalStateReaderStub) *HistoricalTraceWorker {
		execArgs := &ExecArgs{
			ChainConfig: chain.AllProtocolChanges,
			Engine:      engine,
		}
		rw := NewHistoricalTraceWorker(context.Background(), nil, nil, nil, false, execArgs, log.New())
		rw.stateReader = reader
		rw.ibs = state.New(reader)
		rw.evm = vm.NewEVM(blockContext, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, *rw.vmCfg)
		return rw
	}

	readerForIsolated := &historicalStateReaderStub{
		accounts: map[uint64]map[accounts.Address]*accounts.Account{
			blockStartTxNum: {
				senderA: &accountA,
				senderB: &accountB,
			},
		},
	}
	isolatedWorker := newWorker(readerForIsolated)
	txTask := &TxTask{
		Header:           header,
		Txs:              types.Transactions{tx0, tx1},
		TxNum:            currentTxNum,
		TxIndex:          1,
		EvmBlockContext:  blockContext,
		Config:           chain.AllProtocolChanges,
		HistoryExecution: true,
	}

	isolatedWorker.stateReader.SetTxNum(txTask.TxNum)
	isolatedWorker.ibs.Reset()
	isolatedWorker.ibs.SetTrace(false)
	isolatedWorker.stateReader.SetTrace(false, "")

	isolatedResult := TxResult{Task: txTask}
	err = isolatedWorker.runHistoricalTxIsolated(txTask, isolatedWorker.ibs, txTask.Rules(), nil, &isolatedResult)
	require.ErrorIs(t, err, protocol.ErrNonceTooHigh)

	readerForReplay := &historicalStateReaderStub{
		accounts: map[uint64]map[accounts.Address]*accounts.Account{
			blockStartTxNum: {
				senderA: &accountA,
				senderB: &accountB,
			},
		},
	}
	replayWorker := newWorker(readerForReplay)
	result := replayWorker.RunTxTask(txTask)
	require.NoError(t, result.Err)
	require.Equal(t, uint64(21_000), result.ExecutionResult.ReceiptGasUsed)
	require.Contains(t, readerForReplay.readTxNums, currentTxNum)
	require.Contains(t, readerForReplay.readTxNums, blockStartTxNum)
}

func TestS_NODE_18_HistoricalReplayDoesNotExecuteAgainstLatestFallbackState(t *testing.T) {
	t.Parallel()

	engine := ethash.NewFaker()
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	senderAKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderBKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	senderA := accounts.InternAddress(crypto.PubkeyToAddress(senderAKey.PublicKey))
	senderB := accounts.InternAddress(crypto.PubkeyToAddress(senderBKey.PublicKey))
	receiver := common.HexToAddress("0x00000000000000000000000000000000000000ff")

	signer := types.MakeSigner(chain.AllProtocolChanges, 1, 1)
	gasPrice := uint256.NewInt(1)

	tx0, err := types.SignTx(types.NewTransaction(0, receiver, uint256.NewInt(0), 21_000, gasPrice, nil), *signer, senderAKey)
	require.NoError(t, err)
	tx1, err := types.SignTx(types.NewTransaction(6, receiver, uint256.NewInt(0), 100_000, gasPrice, nil), *signer, senderBKey)
	require.NoError(t, err)

	header := &types.Header{
		// Mirrors the mainnet archive replay failure around block 204336:
		// tx 0x9596c9d9926ab91e27d072178b6540d0dc2a73b8048bbb8939e4ed84d81c07c5
		// is canonical with sender nonce 6, while the latest account nonce has
		// already advanced to 17. Historical replay must not execute against that
		// latest-fallback view.
		Number:     *uint256.NewInt(204336),
		Time:       1,
		GasLimit:   1_000_000,
		Difficulty: *uint256.NewInt(1),
	}
	blockContext := protocol.NewEVMBlockContext(header, func(uint64) (common.Hash, error) { return common.Hash{}, nil }, engine, accounts.NilAddress, chain.AllProtocolChanges)

	currentTxNum := uint64(3)

	accountALatest := accounts.NewAccount()
	accountALatest.Nonce = 0
	accountALatest.Balance.SetUint64(1_000_000)
	accountBLatest := accounts.NewAccount()
	accountBLatest.Nonce = 17
	accountBLatest.Balance.SetUint64(43_957_650_000_000_000)

	reader := &historicalStateReaderStub{
		accounts: map[uint64]map[accounts.Address]*accounts.Account{},
		latest: map[accounts.Address]*accounts.Account{
			senderA: &accountALatest,
			senderB: &accountBLatest,
		},
	}

	execArgs := &ExecArgs{
		ChainConfig: chain.AllProtocolChanges,
		Engine:      engine,
	}
	worker := NewHistoricalTraceWorker(context.Background(), nil, nil, nil, false, execArgs, log.New())
	worker.stateReader = reader
	worker.ibs = state.New(reader)
	worker.evm = vm.NewEVM(blockContext, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, *worker.vmCfg)

	txTask := &TxTask{
		Header:           header,
		Txs:              types.Transactions{tx0, tx1},
		TxNum:            currentTxNum,
		TxIndex:          1,
		EvmBlockContext:  blockContext,
		Config:           chain.AllProtocolChanges,
		HistoryExecution: true,
	}

	result := worker.RunTxTask(txTask)
	require.Error(t, result.Err)
	require.ErrorIs(
		t,
		result.Err,
		state.PrunedError,
		"archive historical replay must surface missing/inconsistent historical state instead of executing the target transaction on latest-fallback account data (mainnet block 204336, txIndex 4, sender nonce 6 vs latest nonce 17)",
	)
	require.NotErrorIs(t, result.Err, protocol.ErrNonceTooLow)
	require.NotErrorIs(t, result.Err, protocol.ErrNonceTooHigh)
}
