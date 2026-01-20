package exec_backtester

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/rulesconfig"
)

func dbCfg(label kv.Label, path string, logger log.Logger) mdbx.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	return mdbx.New(label, logger).Path(path).
		RoTxsLimiter(limiterB).
		Accede(true) // integration tool: open db without creation and without blocking erigon
}

func storedChanges(tx kv.TemporalTx, fromTxNum uint64, txnIndex int) (map[string]([]byte), error) {
	changes := make(map[string]([]byte))

	it, err := tx.HistoryRange(kv.AccountsDomain, int(fromTxNum+uint64(txnIndex+1)), int(fromTxNum+uint64(txnIndex+2)), order.Asc, -1)
	if err != nil {
		return changes, err
	}
	//defer it.Close()
	for it.HasNext() {
		key, _, err := it.Next()
		if err != nil {
			return changes, err
		}
		val, ok, err := tx.GetAsOf(kv.AccountsDomain, key, fromTxNum+uint64(txnIndex+2))
		if err != nil {
			return changes, err
		}
		if !ok {
			return changes, errors.New("account val not found")
		}
		changes[string(key)] = val
	}

	it, err = tx.HistoryRange(kv.StorageDomain, int(fromTxNum+uint64(txnIndex+1)), int(fromTxNum+uint64(txnIndex+2)), order.Asc, -1)
	if err != nil {
		return changes, err
	}
	//defer it.Close()
	for it.HasNext() {
		key, _, err := it.Next()
		if err != nil {
			return changes, err
		}
		val, ok, err := tx.GetAsOf(kv.StorageDomain, key, fromTxNum+uint64(txnIndex+2))
		if err != nil {
			return changes, err
		}
		if !ok {
			return changes, errors.New("storage val not found")
		}
		changes[string(key)] = val
	}

	return changes, err
}

// Use HistoricalTraceWorker instead???
func reExecutedChanges(ctx context.Context, tx kv.TemporalTx, blockReader services.FullBlockReader, chainConfig *chain.Config, header *types.Header, fromTxNum uint64, txnIndex int, logger log.Logger) (map[string]([]byte), error) {
	// What's the diff between commitmentdb.HistoryStateReader and state.HistoryReaderV3?
	stateReader := state.NewHistoryReaderV3(tx, fromTxNum+uint64(txnIndex+1))
	ibs := state.New(stateReader)
	engine := rulesconfig.CreateRulesEngineBareBones(ctx, chainConfig, logger)
	vmCfg := vm.Config{}
	syscall := func(contract accounts.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		ret, err := protocol.SysCallContract(contract, data, chainConfig, ibs, header, engine, constCall /* constCall */, vmCfg)
		return ret, err
	}
	chain := consensuschain.NewReader(chainConfig, tx, blockReader, logger)
	err := engine.Initialize(chainConfig, chain, header, ibs, syscall, logger, nil /* hooks */)
	if err != nil {
		return nil, err
	}
	noop := state.NewNoopWriter()
	blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, nil), engine, accounts.NilAddress, chainConfig)
	rules := blockContext.Rules(chainConfig)
	err = ibs.FinalizeTx(rules, noop)
	if err != nil {
		return nil, err
	}
	stateWriter := testutil.NewMapWriter()
	err = ibs.MakeWriteSet(rules, stateWriter)
	if err != nil {
		return nil, err
	}

	// TODO(yperbasis) implement for txnIndex != -1

	return stateWriter.Changes, err
}

func TestReExecution(t *testing.T) {
	// https://github.com/erigontech/erigon/issues/18276
	dataDir := "/Users/andrew/Library/Erigon/chiado"
	blockNum := uint64(19366160)

	dirs, _, err := datadir.New(dataDir).MustFlock()
	require.NoError(t, err)
	// defer func() {
	// 	err := l.Unlock()
	// 	require.NoError(t, err)
	// }()
	logger := log.New()
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata, logger).MustOpen()
	//defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	ctx := context.TODO()
	snaps, _, err := app.OpenSnaps(ctx, cfg, dirs, chainDB, logger)
	require.NoError(t, err)
	//	defer clean()
	blockReader, _ := snaps.BlockRetire.IO()
	db, err := temporal.New(chainDB, snaps.Aggregator)
	require.NoError(t, err)
	//	defer db.Close()

	tnr := blockReader.TxnumReader(ctx)
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	fromTxNum, err := tnr.Min(tx, blockNum)
	require.NoError(t, err)

	block, err := blockReader.BlockByNumber(ctx, tx, blockNum)
	require.NoError(t, err)

	txnIndex := -1 // beginning of the block

	// Check changes at the beginning of the block
	changesInDb, err := storedChanges(tx, fromTxNum, txnIndex)
	require.NoError(t, err)
	reExecChanges, err := reExecutedChanges(ctx, tx, blockReader, chainConfig, block.Header(), fromTxNum, txnIndex, logger)
	require.NoError(t, err)
	assert.Equal(t, changesInDb, reExecChanges)

	// TODO: re-execute and compare results for all transactions in the block
}
