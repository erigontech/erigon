package stages

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/gateway-fm/cdk-erigon-lib/common/length"
	"github.com/gateway-fm/cdk-erigon-lib/etl"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/temporal/historyv2"
	libstate "github.com/gateway-fm/cdk-erigon-lib/state"
	"github.com/ledgerwatch/log/v3"

	"bytes"
	"io"
	"math/big"

	"errors"

	mapset "github.com/deckarep/golang-set/v2"
	types2 "github.com/gateway-fm/cdk-erigon-lib/types"
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
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/smt/pkg/blockinfo"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/txpool"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/secp256k1"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
)

const (
	logInterval = 20 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000

	transactionGasLimit = 30000000

	yieldSize = 100 // arbitrary number defining how many transactions to yield from the pool at once
)

var (
	noop            = state.NewNoopWriter()
	blockDifficulty = new(big.Int).SetUint64(0)
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
	zkVmConfig    *vm.ZkConfig
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
	vmConfig *vm.ZkConfig,
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
		zkVmConfig:    vmConfig,
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

	l1Recovery := cfg.zk.L1SyncStartBlock > 0

	hermezDb := hermez_db.NewHermezDb(tx)

	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	parentBlock, err := rawdb.ReadBlockByNumber(tx, executionAt)
	if err != nil {
		return err
	}

	lastBatch, err := stages.GetStageProgress(tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	var forkId uint64 = 0

	if executionAt == 0 {
		// capture the initial sequencer fork id for the first batch
		forkId = cfg.zk.SequencerInitialForkId
		if err := hermezDb.WriteForkId(1, forkId); err != nil {
			return err
		}
		if err := hermezDb.WriteForkIdBlockOnce(uint64(forkId), 1); err != nil {
			return err
		}
	} else {
		forkId, err = hermezDb.GetForkId(lastBatch)
		if err != nil {
			return err
		}
		if forkId == 0 {
			return errors.New("the network cannot have a 0 fork id")
		}
	}

	if err := utils.UpdateZkEVMBlockCfg(cfg.chainConfig, hermezDb, logPrefix); err != nil {
		return err
	}

	nextBlockNum := executionAt + 1
	thisBatch := lastBatch + 1
	newBlockTimestamp := uint64(time.Now().Unix())
	coinbase := cfg.zk.AddressSequencer

	var decodedBlock zktx.DecodedBatchL2Data
	var transactions []types.Transaction
	var effectiveGases []uint8
	workRemaining := true
	if l1Recovery {
		decodedBlock, coinbase, workRemaining, err = getNextL1BatchData(thisBatch, forkId, hermezDb)
		if err != nil {
			return err
		}
		transactions = decodedBlock.Transactions
		newBlockTimestamp = parentBlock.Time() + uint64(decodedBlock.DeltaTimestamp)
		effectiveGases = decodedBlock.EffectiveGasPricePercentages
	}

	header := &types.Header{
		ParentHash: parentBlock.Hash(),
		Coinbase:   coinbase,
		Difficulty: blockDifficulty,
		Number:     new(big.Int).SetUint64(nextBlockNum),
		GasLimit:   getGasLimit(uint16(forkId)),
		Time:       newBlockTimestamp,
	}

	stateReader := state.NewPlainStateReader(tx)
	ibs := state.New(stateReader)

	eridb := db2.NewEriDb(tx)
	smt := smt.NewSMT(eridb)

	// here we have a special case and need to inject in the initial batch on the network before
	// we can continue accepting transactions from the pool
	if executionAt == 0 {
		err = processInjectedInitialBatch(hermezDb, ibs, tx, cfg, header, parentBlock, stateReader, forkId, smt)
		if err != nil {
			return err
		}
		if freshTx {
			if err := tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	}

	batchCounters := vm.NewBatchCounterCollector(smt.GetDepth()-1, uint16(forkId))

	// whilst in the 1 batch = 1 block = 1 tx flow we can immediately add in the changeL2BlockTx calculation
	// as this is the first tx we can skip the overflow check
	batchCounters.StartNewBlock()

	// calculate and store the l1 info tree index used for this block
	infoTreeIndexProgress, err := stages.GetStageProgress(tx, stages.HighestUsedL1InfoIndex)
	if err != nil {
		return err
	}

	// if we are in a recovery state and recognise that a l1 info tree index has been reused
	// then we need to not include the GER and L1 block hash into the block info root calculation, so
	// we keep track of this here
	includeGerInBlockInfoRoot := true

	var l1TreeUpdateIndex uint64
	var l1TreeUpdate *zktypes.L1InfoTreeUpdate
	if l1Recovery {
		l1TreeUpdateIndex = uint64(decodedBlock.L1InfoTreeIndex)
		l1TreeUpdate, err = hermezDb.GetL1InfoTreeUpdate(l1TreeUpdateIndex)
		if err != nil {
			return err
		}
		if infoTreeIndexProgress >= l1TreeUpdateIndex {
			includeGerInBlockInfoRoot = false
		}
	} else {
		l1TreeUpdateIndex, l1TreeUpdate, err = calculateNextL1TreeUpdateToUse(infoTreeIndexProgress, hermezDb)
		if err != nil {
			return err
		}
		if l1TreeUpdateIndex > 0 {
			infoTreeIndexProgress = l1TreeUpdateIndex
		}
	}

	if err = hermezDb.WriteBlockL1InfoTreeIndex(nextBlockNum, l1TreeUpdateIndex); err != nil {
		return err
	}

	parentRoot := parentBlock.Root()
	if err = handleStateForNewBlockStarting(cfg.chainConfig, nextBlockNum, newBlockTimestamp, &parentRoot, l1TreeUpdate, ibs, hermezDb); err != nil {
		return err
	}

	// start waiting for a new transaction to arrive
	ticker := time.NewTicker(10 * time.Second)
	log.Info(fmt.Sprintf("[%s] Waiting for txs from the pool...", logPrefix))
	var addedTransactions []types.Transaction
	var addedReceipts []*types.Receipt
	yielded := mapset.NewSet[[32]byte]()
	lastTxTime := time.Now()
	overflow := false

	// start to wait for transactions to come in from the pool and attempt to add them to the current batch.  Once we detect a counter
	// overflow we revert the IBS back to the previous snapshot and don't add the transaction/receipt to the collection that will
	// end up in the finalised block
LOOP:
	for {
		select {
		case <-ticker.C:
			log.Info(fmt.Sprintf("[%s] Waiting some more for txs from the pool...", logPrefix))
		default:
			if !l1Recovery {
				transactions = make([]types.Transaction, 0)
				cfg.txPool.LockFlusher()
				transactions, err = getNextPoolTransactions(cfg, executionAt, forkId, yielded)
				if err != nil {
					return err
				}
				cfg.txPool.UnlockFlusher()
			}

			for _, t := range transactions {
				txHash := t.Hash().String()
				_ = txHash
			}

			for idx, transaction := range transactions {
				var receipt *types.Receipt

				var effectiveGas uint8
				if l1Recovery {
					effectiveGas = effectiveGases[idx]
				} else {
					effectiveGas = DeriveEffectiveGasPrice(cfg, transaction)
				}

				receipt, overflow, err = attemptAddTransaction(tx, cfg, batchCounters, header, parentBlock.Header(), transaction, ibs, hermezDb, smt, effectiveGas)
				if err != nil {
					return err
				}
				if overflow {
					log.Info(fmt.Sprintf("[%s] overflowed adding transaction to batch", logPrefix), "batch", thisBatch, "tx-hash", transaction.Hash())
					break LOOP
				}

				addedTransactions = append(addedTransactions, transaction)
				addedReceipts = append(addedReceipts, receipt)
			}

			if l1Recovery {
				// just go into the normal loop waiting for new transactions to signal that the recovery
				// has finished as far as it can go
				if len(transactions) == 0 && !workRemaining {
					continue
				}

				// if we had transactions then break the loop because we don't need to wait for more
				// because we got them from the DB
				break LOOP
			}

			// if there were no transactions in this check, and we have some transactions to process, and we've waited long enough for
			// more to arrive then close the batch
			sinceLastTx := time.Since(lastTxTime)
			if len(transactions) > 0 {
				lastTxTime = time.Now()
			} else if len(addedTransactions) > 0 && sinceLastTx > 250*time.Millisecond {
				log.Info(fmt.Sprintf("[%s] No new transactions, closing block at %v transactions", logPrefix, len(addedTransactions)))
				break LOOP
			}
		}
	}

	// todo: can we handle this scenario without needing to re-process the transactions?  We're doing this currently because the IBS can't be reverted once a tx has been
	// finalised within it - it causes a panic
	if overflow {
		// we know now that we have a list of good transactions, so we need to get a fresh intra block state and re-run the known good ones
		// before continuing on
		batchCounters.ClearTransactionCounters()
		ibs = state.New(stateReader)

		header = &types.Header{
			ParentHash: parentBlock.Hash(),
			Coinbase:   cfg.zk.AddressSequencer,
			Difficulty: blockDifficulty,
			Number:     new(big.Int).SetUint64(nextBlockNum),
			GasLimit:   getGasLimit(uint16(forkId)),
			Time:       newBlockTimestamp,
		}

		for idx, transaction := range addedTransactions {
			effectiveGas := DeriveEffectiveGasPrice(cfg, transaction)
			receipt, innerOverflow, err := attemptAddTransaction(tx, cfg, batchCounters, header, parentBlock.Header(), transaction, ibs, hermezDb, smt, effectiveGas)
			if err != nil {
				return err
			}
			if innerOverflow {
				// kill the node at this stage to prevent a batch being created that can't be proven
				panic("overflowed twice during execution")
			}
			addedReceipts[idx] = receipt
		}
	}

	counters, err := batchCounters.CombineCollectors()
	if err != nil {
		return err
	}
	log.Info("counters consumed", "counts", counters.UsedAsString())
	err = hermezDb.WriteBatchCounters(thisBatch, counters.UsedAsMap())
	if err != nil {
		return err
	}

	l1BlockHash := common.Hash{}
	ger := common.Hash{}
	if l1TreeUpdate != nil && includeGerInBlockInfoRoot {
		l1BlockHash = l1TreeUpdate.ParentHash
		ger = l1TreeUpdate.GER
	}

	if err = finaliseBlock(cfg, tx, hermezDb, ibs, stateReader, header, parentBlock, addedTransactions, addedReceipts, thisBatch, ger, l1BlockHash, forkId); err != nil {
		return err
	}

	if err = updateSequencerProgress(tx, nextBlockNum, thisBatch, infoTreeIndexProgress); err != nil {
		return err
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func getNextPoolTransactions(cfg SequenceBlockCfg, executionAt, forkId uint64, alreadyYielded mapset.Set[[32]byte]) ([]types.Transaction, error) {
	var transactions []types.Transaction
	var err error
	var count int
	killer := time.NewTicker(50 * time.Millisecond)
LOOP:
	for {
		// ensure we don't spin forever looking for transactions, attempt for a while then exit up to the caller
		select {
		case <-killer.C:
			break LOOP
		default:
		}
		if err := cfg.txPoolDb.View(context.Background(), func(poolTx kv.Tx) error {
			slots := types2.TxsRlp{}
			_, count, err = cfg.txPool.YieldBest(yieldSize, &slots, poolTx, executionAt, getGasLimit(uint16(forkId)), alreadyYielded)
			if err != nil {
				return err
			}
			if count == 0 {
				time.Sleep(500 * time.Microsecond)
				return nil
			}
			transactions, err = extractTransactionsFromSlot(slots)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}

		if len(transactions) > 0 {
			break
		}
	}

	return transactions, err
}

func getNextL1BatchData(batchNumber uint64, forkId uint64, hermezDb *hermez_db.HermezDb) (zktx.DecodedBatchL2Data, common.Address, bool, error) {
	// we expect that the batch we're going to load in next should be in the db already because of the l1 block sync
	// stage, if it is not there we need to panic as we're in a bad state
	batchL2Data, err := hermezDb.GetL1BatchData(batchNumber)
	if err != nil {
		return zktx.DecodedBatchL2Data{}, common.Address{}, true, err
	}

	if len(batchL2Data) == 0 {
		// end of the line for batch recovery so return empty
		return zktx.DecodedBatchL2Data{}, common.Address{}, false, nil
	}

	coinbase := common.BytesToAddress(batchL2Data[:length.Addr])
	batchL2Data = batchL2Data[length.Addr:]

	decodedData, err := zktx.DecodeBatchL2Blocks(batchL2Data, forkId)
	if err != nil {
		return zktx.DecodedBatchL2Data{}, common.Address{}, true, err
	}

	if len(decodedData) == 0 {
		return zktx.DecodedBatchL2Data{}, common.Address{}, false, nil
	}

	// todo - right now we only handle single block batches until multi block sequencing is in place so always
	// take the first element
	firstDecoded := decodedData[0]

	isWorkRemaining := true
	if len(firstDecoded.Transactions) == 0 {
		// we need to check if this batch should simply be empty or not so we need to check against the
		// highest known batch number to see if we have work to do still
		highestKnown, err := hermezDb.GetLastL1BatchData()
		if err != nil {
			return zktx.DecodedBatchL2Data{}, common.Address{}, true, err
		}
		if batchNumber >= highestKnown {
			isWorkRemaining = false
		}
	}

	return firstDecoded, coinbase, isWorkRemaining, err
}

const (
	injectedBatchNumber      = 1
	injectedBatchBlockNumber = 1
)

func processInjectedInitialBatch(
	hermezDb *hermez_db.HermezDb,
	ibs *state.IntraBlockState,
	tx kv.RwTx,
	cfg SequenceBlockCfg,
	header *types.Header,
	parentBlock *types.Block,
	stateReader *state.PlainStateReader,
	forkId uint64,
	smt *smt.SMT,
) error {
	injected, err := hermezDb.GetL1InjectedBatch(0)
	if err != nil {
		return err
	}
	fakeL1TreeUpdate := &zktypes.L1InfoTreeUpdate{
		GER:        injected.LastGlobalExitRoot,
		ParentHash: injected.L1ParentHash,
		Timestamp:  injected.Timestamp,
	}

	// the injected batch block timestamp should also match that of the injected batch
	header.Time = injected.Timestamp

	parentRoot := parentBlock.Root()
	if err = handleStateForNewBlockStarting(cfg.chainConfig, 1, injected.Timestamp, &parentRoot, fakeL1TreeUpdate, ibs, hermezDb); err != nil {
		return err
	}

	txn, receipt, err := handleInjectedBatch(cfg, tx, ibs, hermezDb, injected, header, parentBlock, forkId, smt)
	if err != nil {
		return err
	}
	txns := types.Transactions{*txn}
	receipts := types.Receipts{receipt}
	if err = finaliseBlock(cfg, tx, hermezDb, ibs, stateReader, header, parentBlock, txns, receipts, injectedBatchNumber, injected.LastGlobalExitRoot, injected.L1ParentHash, forkId); err != nil {
		return err
	}

	if err = updateSequencerProgress(tx, injectedBatchBlockNumber, injectedBatchNumber, 0); err != nil {
		return err
	}

	return nil
}

func postBlockStateHandling(
	cfg SequenceBlockCfg,
	ibs *state.IntraBlockState,
	hermezDb *hermez_db.HermezDb,
	header *types.Header,
	ger common.Hash,
	l1BlockHash common.Hash,
	parentHash common.Hash,
	transactions types.Transactions,
	receipts []*types.Receipt,
) error {
	infoTree := blockinfo.NewBlockInfoTree()
	coinbase := header.Coinbase
	if err := infoTree.InitBlockHeader(&parentHash, &coinbase, header.Number.Uint64(), header.GasLimit, header.Time, &ger, &l1BlockHash); err != nil {
		return err
	}
	var err error
	var logIndex int64 = 0
	for i := 0; i < len(transactions); i++ {
		receipt := receipts[i]
		t := transactions[i]

		var from common.Address
		sender, ok := t.GetSender()
		if ok {
			from = sender
		} else {
			signer := types.MakeSigner(cfg.chainConfig, header.Number.Uint64())
			from, err = t.Sender(*signer)
			if err != nil {
				return err
			}
		}

		l2TxHash, err := zktx.ComputeL2TxHash(
			t.GetChainID().ToBig(),
			t.GetValue(),
			t.GetPrice(),
			t.GetNonce(),
			t.GetGas(),
			t.GetTo(),
			&from,
			t.GetData(),
		)
		if err != nil {
			return err
		}

		// TODO: calculate l2 tx hash
		effectiveGasPrice := DeriveEffectiveGasPrice(cfg, t)
		_, err = infoTree.SetBlockTx(&l2TxHash, i, receipt, logIndex, receipt.CumulativeGasUsed, effectiveGasPrice)
		if err != nil {
			return err
		}
		logIndex += int64(len(receipt.Logs))
	}

	root, err := infoTree.SetBlockGasUsed(header.GasUsed)
	if err != nil {
		return err
	}

	rootHash := common.BigToHash(root)
	ibs.PostExecuteStateSet(cfg.chainConfig, header.Number.Uint64(), &rootHash)

	// store a reference to this block info root against the block number
	return hermezDb.WriteBlockInfoRoot(header.Number.Uint64(), rootHash)
}

func handleInjectedBatch(
	cfg SequenceBlockCfg,
	dbTx kv.RwTx,
	ibs *state.IntraBlockState,
	hermezDb *hermez_db.HermezDb,
	injected *zktypes.L1InjectedBatch,
	header *types.Header,
	parentBlock *types.Block,
	forkId uint64,
	smt *smt.SMT,
) (*types.Transaction, *types.Receipt, error) {
	decodedBlocks, err := zktx.DecodeBatchL2Blocks(injected.Transaction, 5)
	if err != nil {
		return nil, nil, err
	}
	if len(decodedBlocks) == 0 || len(decodedBlocks) > 1 {
		return nil, nil, errors.New("expected 1 block for the injected batch")
	}
	if len(decodedBlocks[0].Transactions) == 0 {
		return nil, nil, errors.New("expected 1 transaction in the injected batch")
	}

	batchCounters := vm.NewBatchCounterCollector(smt.GetDepth()-1, uint16(forkId))

	// process the tx and we can ignore the counters as an overflow at this stage means no network anyway
	effectiveGas := DeriveEffectiveGasPrice(cfg, decodedBlocks[0].Transactions[0])
	receipt, _, err := attemptAddTransaction(dbTx, cfg, batchCounters, header, parentBlock.Header(), decodedBlocks[0].Transactions[0], ibs, hermezDb, smt, effectiveGas)
	if err != nil {
		return nil, nil, err
	}

	return &decodedBlocks[0].Transactions[0], receipt, nil
}

func finaliseBlock(
	cfg SequenceBlockCfg,
	tx kv.RwTx,
	hermezDb *hermez_db.HermezDb,
	ibs *state.IntraBlockState,
	stateReader *state.PlainStateReader,
	newHeader *types.Header,
	parentBlock *types.Block,
	transactions []types.Transaction,
	receipts types.Receipts,
	batch uint64,
	ger common.Hash,
	l1BlockHash common.Hash,
	forkId uint64,
) error {

	stateWriter := state.NewPlainStateWriter(tx, tx, newHeader.Number.Uint64())
	chainReader := stagedsync.ChainReader{
		Cfg: *cfg.chainConfig,
		Db:  tx,
	}

	var excessDataGas *big.Int
	if parentBlock != nil {
		excessDataGas = parentBlock.ExcessDataGas()
	}

	if err := postBlockStateHandling(cfg, ibs, hermezDb, newHeader, ger, l1BlockHash, parentBlock.Root(), transactions, receipts); err != nil {
		return err
	}

	finalBlock, finalTransactions, finalReceipts, err := core.FinalizeBlockExecution(
		cfg.engine,
		stateReader,
		newHeader,
		transactions,
		[]*types.Header{}, // no uncles
		stateWriter,
		cfg.chainConfig,
		ibs,
		receipts,
		nil, // no withdrawals
		chainReader,
		true,
		excessDataGas,
	)
	if err != nil {
		return err
	}

	finalHeader := finalBlock.Header()
	finalHeader.Coinbase = cfg.zk.AddressSequencer
	finalHeader.GasLimit = getGasLimit(uint16(forkId))
	finalHeader.ReceiptHash = types.DeriveSha(receipts)
	newNum := finalBlock.Number()

	rawdb.WriteHeader(tx, finalHeader)
	err = rawdb.WriteCanonicalHash(tx, finalHeader.Hash(), newNum.Uint64())
	if err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	erigonDB := erigon_db.NewErigonDb(tx)
	err = erigonDB.WriteBody(newNum, finalHeader.Hash(), finalTransactions)
	if err != nil {
		return fmt.Errorf("failed to write body: %v", err)
	}

	// write the new block lookup entries
	rawdb.WriteTxLookupEntries(tx, finalBlock)

	if err = rawdb.WriteReceipts(tx, newNum.Uint64(), finalReceipts); err != nil {
		return err
	}

	if err = hermezDb.WriteForkId(batch, forkId); err != nil {
		return err
	}

	// now process the senders to avoid a stage by itself
	if err := addSenders(cfg, newNum, finalTransactions, tx, finalHeader); err != nil {
		return err
	}

	// now add in the zk batch to block references
	if err := hermezDb.WriteBlockBatch(newNum.Uint64(), batch); err != nil {
		return fmt.Errorf("write block batch error: %v", err)
	}

	return nil
}

func handleStateForNewBlockStarting(
	chainConfig *chain.Config,
	blockNumber uint64,
	timestamp uint64,
	stateRoot *common.Hash,
	l1info *zktypes.L1InfoTreeUpdate,
	ibs *state.IntraBlockState,
	hermezDb *hermez_db.HermezDb,
) error {
	ibs.PreExecuteStateSet(chainConfig, blockNumber, timestamp, stateRoot)

	// handle writing to the ger manager contract
	if l1info != nil {
		// first check if this ger has already been written
		l1BlockHash := ibs.ReadGerManagerL1BlockHash(l1info.GER)
		if l1BlockHash == (common.Hash{}) {
			// not in the contract so let's write it!
			ibs.WriteGerManagerL1BlockHash(l1info.GER, l1info.ParentHash)

			// store it so we can retrieve for the data stream
			if err := hermezDb.WriteBlockGlobalExitRoot(blockNumber, l1info.GER); err != nil {
				return err
			}
			if err := hermezDb.WriteBlockL1BlockHash(blockNumber, l1info.ParentHash); err != nil {
				return err
			}
		}
	}

	return nil
}

func addSenders(
	cfg SequenceBlockCfg,
	newNum *big.Int,
	finalTransactions types.Transactions,
	tx kv.RwTx,
	finalHeader *types.Header,
) error {
	signer := types.MakeSigner(cfg.chainConfig, newNum.Uint64())
	cryptoContext := secp256k1.ContextForThread(1)
	senders := make([]common.Address, 0, len(finalTransactions))
	for _, transaction := range finalTransactions {
		from, err := signer.SenderWithContext(cryptoContext, transaction)
		if err != nil {
			return err
		}
		senders = append(senders, from)
	}

	return rawdb.WriteSenders(tx, finalHeader.Hash(), newNum.Uint64(), senders)
}

func extractTransactionsFromSlot(slot types2.TxsRlp) ([]types.Transaction, error) {
	transactions := make([]types.Transaction, 0, len(slot.Txs))
	reader := bytes.NewReader([]byte{})
	stream := new(rlp.Stream)
	for idx, txBytes := range slot.Txs {
		reader.Reset(txBytes)
		stream.Reset(reader, uint64(len(txBytes)))
		transaction, err := types.DecodeTransaction(stream)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, err
		}
		var sender common.Address
		copy(sender[:], slot.Senders.At(idx))
		transaction.SetSender(sender)
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}

func attemptAddTransaction(
	tx kv.RwTx,
	cfg SequenceBlockCfg,
	batchCounters *vm.BatchCounterCollector,
	header *types.Header,
	parentHeader *types.Header,
	transaction types.Transaction,
	ibs *state.IntraBlockState,
	hermezDb *hermez_db.HermezDb,
	smt *smt.SMT,
	effectiveGasPrice uint8,
) (*types.Receipt, bool, error) {
	txCounters := vm.NewTransactionCounter(transaction, smt.GetDepth()-1, cfg.zk.ShouldCountersBeUnlimited())
	overflow, err := batchCounters.AddNewTransactionCounters(txCounters)
	if err != nil {
		return nil, false, err
	}
	if overflow {
		return nil, true, nil
	}

	gasPool := new(core.GasPool).AddGas(transactionGasLimit)
	getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }

	// set the counter collector on the config so that we can gather info during the execution
	cfg.zkVmConfig.CounterCollector = txCounters.ExecutionCounters()

	// TODO: possibly inject zero tracer here!

	ibs.Prepare(transaction.Hash(), common.Hash{}, 0)

	receipt, execResult, err := core.ApplyTransaction_zkevm(
		cfg.chainConfig,
		core.GetHashFn(header, getHeader),
		cfg.engine,
		&cfg.zk.AddressSequencer,
		gasPool,
		ibs,
		noop,
		header,
		transaction,
		&header.GasUsed,
		*cfg.zkVmConfig,
		parentHeader.ExcessDataGas,
		effectiveGasPrice)

	if err != nil {
		return nil, false, err
	}

	// we need to keep hold of the effective percentage used
	// todo [zkevm] for now we're hard coding to the max value but we need to calc this properly
	if err = hermezDb.WriteEffectiveGasPricePercentage(transaction.Hash(), effectiveGasPrice); err != nil {
		return nil, false, err
	}

	err = txCounters.ProcessTx(ibs, execResult.ReturnData)
	if err != nil {
		return nil, false, err
	}

	// now that we have executed we can check again for an overflow
	overflow, err = batchCounters.CheckForOverflow()

	return receipt, overflow, err
}

// will be called at the start of every new block created within a batch to figure out if there is a new GER
// we can use or not.  In the special case that this is the first block we just return 0 as we need to use the
// 0 index first before we can use 1+
func calculateNextL1TreeUpdateToUse(lastInfoIndex uint64, hermezDb *hermez_db.HermezDb) (uint64, *zktypes.L1InfoTreeUpdate, error) {
	// always default to 0 and only update this if the next available index has reached finality
	var nextL1Index uint64 = 0

	// check which was the last used index

	// check if the next index is there and if it has reached finality or not
	l1Info, err := hermezDb.GetL1InfoTreeUpdate(lastInfoIndex + 1)
	if err != nil {
		return 0, nil, err
	}

	// check that we have reached finality on the l1 info tree event before using it
	// todo: [zkevm] think of a better way to handle finality
	now := time.Now()
	target := now.Add(-(12 * time.Minute))
	if l1Info != nil && l1Info.Timestamp < uint64(target.Unix()) {
		nextL1Index = l1Info.Index
	}

	return nextL1Index, l1Info, nil
}

func updateSequencerProgress(tx kv.RwTx, newHeight uint64, newBatch uint64, l1InfoIndex uint64) error {
	// now update stages that will be used later on in stageloop.go and other stages. As we're the sequencer
	// we won't have headers stage for example as we're already writing them here
	if err := stages.SaveStageProgress(tx, stages.Execution, newHeight); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, newHeight); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, newBatch); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.HighestUsedL1InfoIndex, l1InfoIndex); err != nil {
		return err
	}

	return nil
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
