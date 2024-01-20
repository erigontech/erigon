package stages

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"

	"bytes"
	mapset "github.com/deckarep/golang-set/v2"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	dstypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/txpool"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/secp256k1"
	"io"
	"math/big"
	"sync/atomic"
)

const (
	logInterval = 20 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000

	transactionGasLimit = 30000000
	blobGasLimit        = 30000000 // not sure if this applies to zk but separating it out anyway
)

var (
	fixedUncleHash = common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")

	// todo: seq: this should be read in from somewhere rather than hard coded!
	fixedMiner = common.HexToAddress("0x650c68bef674dc40b54962e1ae9b6c0e35b9d781")

	emptyUncles      = make([]*types.Header, 0)
	emptyReceipts    = make([]*types.Receipt, 0)
	emptyWithdrawals = make([]*types.Withdrawal, 0)
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type SequenceBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	changeSetHook ChangeSetHook
	chainConfig   *chain.Config
	engine        consensus.Engine
	vmConfig      *vm.Config
	badBlockHalt  bool
	stateStream   bool
	accumulator   *shards.Accumulator
	blockReader   services.FullBlockReader

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis
	agg       *libstate.AggregatorV3
	zk        *ethconfig.Zk

	txPool   *txpool.TxPool
	txPoolDb kv.RwDB
}

func StageSequenceBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	changeSetHook ChangeSetHook,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.Config,
	accumulator *shards.Accumulator,
	stateStream bool,
	badBlockHalt bool,

	historyV3 bool,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	agg *libstate.AggregatorV3,
	zk *ethconfig.Zk,

	txPool *txpool.TxPool,
	txPoolDb kv.RwDB,
) SequenceBlockCfg {
	return SequenceBlockCfg{
		db:            db,
		prune:         pm,
		batchSize:     batchSize,
		changeSetHook: changeSetHook,
		chainConfig:   chainConfig,
		engine:        engine,
		vmConfig:      vmConfig,
		dirs:          dirs,
		accumulator:   accumulator,
		stateStream:   stateStream,
		badBlockHalt:  badBlockHalt,
		blockReader:   blockReader,
		genesis:       genesis,
		historyV3:     historyV3,
		syncCfg:       syncCfg,
		agg:           agg,
		zk:            zk,
		txPool:        txPool,
		txPoolDb:      txPoolDb,
	}
}

func executeBlock(
	block *types.Block,
	header *types.Header,
	tx kv.RwTx,
	batch ethdb.Database,
	gers []*dstypes.GerUpdate,
	cfg SequenceBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	initialCycle bool,
	stateStream bool,
	roHermezDb state.ReadOnlyHermezDb,
) error {
	blockNum := block.NumberU64()

	stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, initialCycle, stateStream)
	if err != nil {
		return err
	}

	// [zkevm] - write the global exit root inside the batch so we can unwind it
	// [zkevm] push the global exit root for the related batch into the db ahead of batch execution
	blockNoBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNoBytes, blockNum)

	for _, ger := range gers {
		// [zkevm] - add GER if there is one for this batch
		if err := utils.WriteGlobalExitRoot(stateReader, stateWriter, ger.GlobalExitRoot, ger.Timestamp); err != nil {
			return err
		}
	}

	// [zkevm] - finished writing global exit root to state

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), tx, hash, number)
		return h
	}

	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		// return logger.NewJSONFileLogger(&logger.LogConfig{}, txHash.String()), nil
		return logger.NewStructLogger(&logger.LogConfig{}), nil
	}

	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	var receipts types.Receipts
	var stateSyncReceipt *types.Receipt
	var execRs *core.EphemeralExecResult
	getHashFn := core.GetHashFn(block.Header(), getHeader)

	execRs, err = core.ExecuteBlockEphemerally(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, stagedsync.NewChainReaderImpl(cfg.chainConfig /*config*/, tx /*tx*/, cfg.blockReader /*blockReader*/), getTracer, tx, roHermezDb)
	if err != nil {
		return err
	}
	receipts = execRs.Receipts
	stateSyncReceipt = execRs.StateSyncReceipt

	// [zkevm] - add in the state root to the receipts.  As we only have one tx per block
	// for now just add the header root to the receipt
	for _, r := range receipts {
		r.PostState = header.Root.Bytes()
	}

	header.GasUsed = uint64(execRs.GasUsed)
	header.ReceiptHash = types.DeriveSha(receipts)
	header.Bloom = execRs.Bloom

	if writeReceipts {
		if err = rawdb.AppendReceipts(tx, blockNum, receipts); err != nil {
			return err
		}

		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(tx, block.Hash(), block.NumberU64(), stateSyncReceipt); err != nil {
				return err
			}
		}
	}

	if cfg.changeSetHook != nil {
		if hasChangeSet, ok := stateWriter.(HasChangeSetWriter); ok {
			cfg.changeSetHook(blockNum, hasChangeSet.ChangeSetWriter())
		}
	}
	if writeCallTraces {
		return callTracer.WriteToDb(tx, block, *cfg.vmConfig)
	}
	return nil
}

func newStateReaderWriter(
	batch ethdb.Database,
	tx kv.RwTx,
	block *types.Block,
	writeChangesets bool,
	accumulator *shards.Accumulator,
	initialCycle bool,
	stateStream bool,
) (state.StateReader, state.WriterWithChangeSets, error) {

	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	stateReader = state.NewPlainStateReader(batch)

	if !initialCycle && stateStream {
		txs, err := rawdb.RawTransactionsRange(tx, block.NumberU64(), block.NumberU64())
		if err != nil {
			return nil, nil, err
		}
		accumulator.StartChange(block.NumberU64(), block.Hash(), txs, false)
	} else {
		accumulator = nil
	}
	if writeChangesets {
		stateWriter = state.NewPlainStateWriter(batch, tx, block.NumberU64()).SetAccumulator(accumulator)

	} else {
		stateWriter = state.NewPlainStateWriterNoHistory(batch).SetAccumulator(accumulator)
	}

	return stateReader, stateWriter, nil
}

func SpawnSequencingStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	toBlock uint64,
	ctx context.Context,
	cfg SequenceBlockCfg,
	initialCycle bool,
	quiet bool,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting sequencing stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished sequencing stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}
	blockNum := executionAt + 1

	slots := make([]types2.TxsRlp, 0)

	// start waiting for a new transaction to arrive
	ticker := time.NewTicker(10 * time.Second)
	log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))
	reachedDesiredLimit := false
	for {
		if reachedDesiredLimit {
			break
		}
		var count = 0
		var err error

		select {
		case <-ticker.C:
			log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
		default:
			/*
				todo: seq: for now we only care about one tx and we'll move on to executing the block, but in the future
				when we're timeboxing the block or using counters the yielded set will be useful to ensure we don't double
				up any tx
			*/
			yielded := mapset.NewSet[[32]byte]()

			if err := cfg.txPoolDb.View(context.Background(), func(poolTx kv.Tx) error {
				txSlots := types2.TxsRlp{}
				if _, count, err = cfg.txPool.YieldBest(1, &txSlots, poolTx, executionAt, transactionGasLimit, yielded); err != nil {
					return err
				}
				if count == 0 {
					time.Sleep(1 * time.Millisecond)
				} else {
					slots = append(slots, txSlots)

					reachedDesiredLimit = true
				}
				return nil
			}); err != nil {
				log.Error(fmt.Sprintf("error loading txpool view: %v", err))
			}
		}
	}

	var transactions []types.Transaction
	reader := bytes.NewReader([]byte{})
	stream := new(rlp.Stream)
	for _, slot := range slots {
		for idx, txBytes := range slot.Txs {
			reader.Reset(txBytes)
			stream.Reset(reader, uint64(len(txBytes)))
			transaction, err := types.DecodeTransaction(stream)
			if err == io.EOF {
				continue
			}
			if err != nil {
				return err
			}
			var sender common.Address
			copy(sender[:], slot.Senders.At(idx))
			transaction.SetSender(sender)
			transactions = append(transactions, transaction)
		}
	}

	txStream := types.NewTransactionsFixedOrder(transactions)

	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }

	previousHeader, err := rawdb.ReadBlockByNumber(tx, executionAt)
	if err != nil {
		return err
	}
	nextNumber := new(big.Int).SetUint64(blockNum)
	difficulty := new(big.Int).SetUint64(0)
	current := &stagedsync.MiningBlock{
		Header: &types.Header{
			ParentHash: previousHeader.Hash(),
			Coinbase:   common.Address{},
			Difficulty: difficulty,
			Number:     nextNumber,
			GasLimit:   math.MaxUint64,
			GasUsed:    0,
			Time:       0,
			Extra:      nil,
		},
	}

	quitChan := make(chan struct{})
	var interrupt *int32
	stateReader := state.NewPlainStateReader(tx)
	ibs := state.New(stateReader)

	_, _, err = addTransactionsToMiningBlock(
		s.LogPrefix(),
		current,
		*cfg.chainConfig,
		cfg.vmConfig,
		getHeader,
		cfg.engine,
		txStream,
		common.Address{}, // todo: zk: where do I get the miner address?
		ibs,
		quitChan,
		interrupt,
		0,
	)
	if err != nil {
		return err
	}

	if current.Uncles == nil {
		current.Uncles = []*types.Header{}
	}
	if current.Txs == nil {
		current.Txs = []types.Transaction{}
	}
	if current.Receipts == nil {
		current.Receipts = types.Receipts{}
	}

	parentHeader := getHeader(current.Header.ParentHash, current.Header.Number.Uint64()-1)

	stateWriter := state.NewPlainStateWriter(tx, tx, current.Header.Number.Uint64())
	//stateWriter := state.NewPlainStateWriter(batch, tx, block.NumberU64())
	chainReader := stagedsync.ChainReader{
		Cfg: *cfg.chainConfig,
		Db:  tx,
	}
	hermezDb, err := hermez_db.NewHermezDb(tx)

	var excessDataGas *big.Int
	if parentHeader != nil {
		excessDataGas = parentHeader.ExcessDataGas
	}

	finalBlock, finalTransactions, finalReceipts, err := core.FinalizeBlockExecution(
		cfg.engine,
		stateReader,
		current.Header,
		current.Txs,
		current.Uncles,
		stateWriter,
		cfg.chainConfig,
		ibs,
		current.Receipts,
		current.Withdrawals,
		chainReader,
		true,
		excessDataGas,
	)
	if err != nil {
		return err
	}

	newHeader := finalBlock.Header()
	newHeader.Coinbase = fixedMiner
	newHeader.GasLimit = transactionGasLimit
	newNum := finalBlock.Number()

	rawdb.WriteHeader(tx, newHeader)
	err = rawdb.WriteCanonicalHash(tx, newHeader.Hash(), newNum.Uint64())
	if err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	erigonDB := erigon_db.NewErigonDb(tx)
	err = erigonDB.WriteBody(newNum, newHeader.Hash(), finalTransactions)
	if err != nil {
		return fmt.Errorf("failed to write body: %v", err)
	}

	// write the new block lookup entries
	rawdb.WriteTxLookupEntries(tx, finalBlock)

	if err = rawdb.WriteReceipts(tx, newNum.Uint64(), finalReceipts); err != nil {
		return err
	}

	// now add in the zk batch to block references
	if err := hermezDb.WriteBlockBatch(newNum.Uint64(), newNum.Uint64()); err != nil {
		return fmt.Errorf("write block batch error: %v", err)
	}

	// now process the senders to avoid a stage by itself
	signer := types.MakeSigner(cfg.chainConfig, newNum.Uint64())
	cryptoContext := secp256k1.ContextForThread(1)
	var senders []common.Address
	for _, transaction := range finalTransactions {
		from, err := signer.SenderWithContext(cryptoContext, transaction)
		if err != nil {
			return err
		}
		senders = append(senders, from)
	}
	if err = rawdb.WriteSenders(tx, newHeader.Hash(), newNum.Uint64(), senders); err != nil {
		return err
	}

	// now update stages that will be used later on in stageloop.go and other stages. As we're the sequencer
	// we won't have headers stage for example as we're already writing them here
	if err = stages.SaveStageProgress(tx, stages.Execution, newNum.Uint64()); err != nil {
		return err
	}
	if err = stages.SaveStageProgress(tx, stages.Headers, newNum.Uint64()); err != nil {
		return err
	}
	// todo: hack! for now one block to one batch but this will change shortly
	if err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, newNum.Uint64()); err != nil {
		return err
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func addTransactionsToMiningBlock(
	logPrefix string,
	current *stagedsync.MiningBlock,
	chainConfig chain.Config,
	vmConfig *vm.Config,
	getHeader func(hash common.Hash, number uint64) *types.Header,
	engine consensus.Engine,
	txs types.TransactionsStream,
	coinbase common.Address,
	ibs *state.IntraBlockState,
	quit <-chan struct{},
	interrupt *int32,
	payloadId uint64,
) (types.Logs, bool, error) {
	header := current.Header
	tcount := 0
	gasPool := new(core.GasPool).AddGas(header.GasLimit - header.GasUsed)
	signer := types.MakeSigner(&chainConfig, header.Number.Uint64())

	var coalescedLogs types.Logs
	noop := state.NewNoopWriter()

	parentHeader := getHeader(header.ParentHash, header.Number.Uint64()-1)

	var miningCommitTx = func(txn types.Transaction, coinbase common.Address, vmConfig *vm.Config, chainConfig chain.Config, ibs *state.IntraBlockState, current *stagedsync.MiningBlock) ([]*types.Log, error) {
		ibs.Prepare(txn.Hash(), common.Hash{}, tcount)
		gasSnap := gasPool.Gas()
		snap := ibs.Snapshot()
		log.Debug("addTransactionsToMiningBlock", "txn hash", txn.Hash())
		receipt, _, err := core.ApplyTransaction(&chainConfig, core.GetHashFn(header, getHeader), engine, &coinbase, gasPool, ibs, noop, header, txn, &header.GasUsed, *vmConfig, parentHeader.ExcessDataGas, zktypes.EFFECTIVE_GAS_PRICE_PERCENTAGE_DISABLED)
		if err != nil {
			ibs.RevertToSnapshot(snap)
			gasPool = new(core.GasPool).AddGas(gasSnap) // restore gasPool as well as ibs
			return nil, err
		}

		current.Txs = append(current.Txs, txn)
		current.Receipts = append(current.Receipts, receipt)
		return receipt.Logs, nil
	}

	var stopped *time.Ticker
	defer func() {
		if stopped != nil {
			stopped.Stop()
		}
	}()

	done := false

LOOP:
	for {
		// see if we need to stop now
		if stopped != nil {
			select {
			case <-stopped.C:
				done = true
				break LOOP
			default:
			}
		}

		if err := common.Stopped(quit); err != nil {
			return nil, true, err
		}

		if interrupt != nil && atomic.LoadInt32(interrupt) != 0 && stopped == nil {
			log.Debug("Transaction adding was requested to stop", "payload", payloadId)
			// ensure we run for at least 500ms after the request to stop comes in from GetPayload
			stopped = time.NewTicker(500 * time.Millisecond)
		}
		// If we don't have enough gas for any further transactions then we're done
		if gasPool.Gas() < params.TxGas {
			log.Debug(fmt.Sprintf("[%s] Not enough gas for further transactions", logPrefix), "have", gasPool, "want", params.TxGas)
			done = true
			break
		}
		// Retrieve the next transaction and abort if all done
		txn := txs.Peek()
		if txn == nil {
			break
		}

		// We use the eip155 signer regardless of the env hf.
		from, err := txn.Sender(*signer)
		if err != nil {
			log.Warn(fmt.Sprintf("[%s] Could not recover transaction sender", logPrefix), "hash", txn.Hash(), "err", err)
			txs.Pop()
			continue
		}

		// Check whether the txn is replay protected. If we're not in the EIP155 (Spurious Dragon) hf
		// phase, start ignoring the sender until we do.
		if txn.Protected() && !chainConfig.IsSpuriousDragon(header.Number.Uint64()) {
			log.Debug(fmt.Sprintf("[%s] Ignoring replay protected transaction", logPrefix), "hash", txn.Hash(), "eip155", chainConfig.SpuriousDragonBlock)

			txs.Pop()
			continue
		}

		// Start executing the transaction
		logs, err := miningCommitTx(txn, coinbase, vmConfig, chainConfig, ibs, current)

		if errors.Is(err, core.ErrGasLimitReached) {
			// Pop the env out-of-gas transaction without shifting in the next from the account
			log.Debug(fmt.Sprintf("[%s] Gas limit exceeded for env block", logPrefix), "hash", txn.Hash(), "sender", from)
			txs.Pop()
		} else if errors.Is(err, core.ErrNonceTooLow) {
			// New head notification data race between the transaction pool and miner, shift
			log.Debug(fmt.Sprintf("[%s] Skipping transaction with low nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce())
			txs.Shift()
		} else if errors.Is(err, core.ErrNonceTooHigh) {
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Debug(fmt.Sprintf("[%s] Skipping transaction with high nonce", logPrefix), "hash", txn.Hash(), "sender", from, "nonce", txn.GetNonce())
			txs.Pop()
		} else if err == nil {
			// Everything ok, collect the logs and shift in the next transaction from the same account
			log.Debug(fmt.Sprintf("[%s] addTransactionsToMiningBlock Successful", logPrefix), "sender", from, "nonce", txn.GetNonce(), "payload", payloadId)
			coalescedLogs = append(coalescedLogs, logs...)
			tcount++
			txs.Shift()
		} else {
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug(fmt.Sprintf("[%s] Skipping transaction", logPrefix), "hash", txn.Hash(), "sender", from, "err", err)
			txs.Shift()
		}
	}

	/*
		// Notify resubmit loop to decrease resubmitting interval if env interval is larger
		// than the user-specified one.
		if interrupt != nil {
			w.resubmitAdjustCh <- &intervalAdjust{inc: false}
		}
	*/
	return coalescedLogs, done, nil

}

func UnwindSequenceExecutionStage(u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context, cfg SequenceBlockCfg, initialCycle bool) (err error) {
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err = unwindExecutionStage(u, s, tx, ctx, cfg, initialCycle); err != nil {
		return err
	}
	if err = u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context, cfg SequenceBlockCfg, initialCycle bool) error {
	logPrefix := s.LogPrefix()
	stateBucket := kv.PlainState
	storageKeyLength := length.Addr + length.Incarnation + length.Hash

	var accumulator *shards.Accumulator
	if !initialCycle && cfg.stateStream && s.BlockNumber-u.UnwindPoint < stateStreamLimit {
		accumulator = cfg.accumulator

		hash, err := rawdb.ReadCanonicalHash(tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("read canonical hash of unwind point: %w", err)
		}
		txs, err := rawdb.RawTransactionsRange(tx, u.UnwindPoint, s.BlockNumber)
		if err != nil {
			return err
		}
		accumulator.StartChange(u.UnwindPoint, hash, txs, true)
	}

	changes := etl.NewCollector(logPrefix, cfg.dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize))
	defer changes.Close()
	errRewind := changeset.RewindData(tx, s.BlockNumber, u.UnwindPoint, changes, ctx.Done())
	if errRewind != nil {
		return fmt.Errorf("getting rewind data: %w", errRewind)
	}

	if err := changes.Load(tx, stateBucket, func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(k) == 20 {
			if len(v) > 0 {
				var acc accounts.Account
				if err := acc.DecodeForStorage(v); err != nil {
					return err
				}

				// Fetch the code hash
				recoverCodeHashPlain(&acc, tx, k)
				var address common.Address
				copy(address[:], k)

				// cleanup contract code bucket
				original, err := state.NewPlainStateReader(tx).ReadAccountData(address)
				if err != nil {
					return fmt.Errorf("read account for %x: %w", address, err)
				}
				if original != nil {
					// clean up all the code incarnations original incarnation and the new one
					for incarnation := original.Incarnation; incarnation > acc.Incarnation && incarnation > 0; incarnation-- {
						err = tx.Delete(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], incarnation))
						if err != nil {
							return fmt.Errorf("writeAccountPlain for %x: %w", address, err)
						}
					}
				}

				newV := make([]byte, acc.EncodingLengthForStorage())
				acc.EncodeForStorage(newV)
				if accumulator != nil {
					accumulator.ChangeAccount(address, acc.Incarnation, newV)
				}
				if err := next(k, k, newV); err != nil {
					return err
				}
			} else {
				if accumulator != nil {
					var address common.Address
					copy(address[:], k)
					accumulator.DeleteAccount(address)
				}
				if err := next(k, k, nil); err != nil {
					return err
				}
			}
			return nil
		}
		if accumulator != nil {
			var address common.Address
			var incarnation uint64
			var location common.Hash
			copy(address[:], k[:length.Addr])
			incarnation = binary.BigEndian.Uint64(k[length.Addr:])
			copy(location[:], k[length.Addr+length.Incarnation:])
			log.Debug(fmt.Sprintf("un ch st: %x, %d, %x, %x\n", address, incarnation, location, common.Copy(v)))
			accumulator.ChangeStorage(address, incarnation, location, common.Copy(v))
		}
		if len(v) > 0 {
			if err := next(k, k[:storageKeyLength], v); err != nil {
				return err
			}
		} else {
			if err := next(k, k[:storageKeyLength], nil); err != nil {
				return err
			}
		}
		return nil

	}, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	if err := historyv2.Truncate(tx, u.UnwindPoint+1); err != nil {
		return err
	}

	if err := rawdb.TruncateReceipts(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate receipts: %w", err)
	}
	if err := rawdb.TruncateBorReceipts(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate bor receipts: %w", err)
	}
	if err := rawdb.DeleteNewerEpochs(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete newer epochs: %w", err)
	}

	// Truncate CallTraceSet
	keyStart := hexutility.EncodeTs(u.UnwindPoint + 1)
	c, err := tx.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return err
		}
		err = c.DeleteCurrentDuplicates()
		if err != nil {
			return err
		}
	}

	return nil
}

func recoverCodeHashPlain(acc *accounts.Account, db kv.Tx, key []byte) {
	var address common.Address
	copy(address[:], key)
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		if codeHash, err2 := db.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
			copy(acc.CodeHash[:], codeHash)
		}
	}
}

func PruneSequenceExecutionStage(s *stagedsync.PruneState, tx kv.RwTx, cfg SequenceBlockCfg, ctx context.Context, initialCycle bool) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if cfg.historyV3 {
		cfg.agg.SetTx(tx)
		if initialCycle {
			if err = cfg.agg.Prune(ctx, ethconfig.HistoryV3AggregationStep/10); err != nil { // prune part of retired data, before commit
				return err
			}
		} else {
			if err = cfg.agg.PruneWithTiemout(ctx, 1*time.Second); err != nil { // prune part of retired data, before commit
				return err
			}
		}
	} else {
		if cfg.prune.History.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.AccountChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
			if err = rawdb.PruneTableDupSort(tx, kv.StorageChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}

		if cfg.prune.Receipts.Enabled() {
			if err = rawdb.PruneTable(tx, kv.Receipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
			if err = rawdb.PruneTable(tx, kv.BorReceipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxUint32); err != nil {
				return err
			}
			// LogIndex.Prune will read everything what not pruned here
			if err = rawdb.PruneTable(tx, kv.Log, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
		}
		if cfg.prune.CallTraces.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.CallTraceSet, logPrefix, cfg.prune.CallTraces.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
