package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/core/rawdb/rawdbhelpers"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/wrap"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/consensuschain"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	tracelogger "github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
)

const (
	logInterval = 30 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000
)

type HasChangeSetWriter interface {
	ChangeSetWriter() *state.ChangeSetWriter
}

type ChangeSetHook func(blockNum uint64, wr *state.ChangeSetWriter)

type headerDownloader interface {
	ReportBadHeaderPoS(badHeader, lastValidAncestor common.Hash)
}

type ExecuteBlockCfg struct {
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
	hd            headerDownloader
	author        *common.Address
	// last valid number of the stage

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis
	agg       *libstate.Aggregator

	silkworm        *silkworm.Silkworm
	blockProduction bool
}

func StageExecuteBlocksCfg(
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

	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	hd headerDownloader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	agg *libstate.Aggregator,
	silkworm *silkworm.Silkworm,
) ExecuteBlockCfg {
	return ExecuteBlockCfg{
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
		hd:            hd,
		genesis:       genesis,
		historyV3:     true,
		syncCfg:       syncCfg,
		agg:           agg,
		silkworm:      silkworm,
	}
}

func executeBlock(
	block *types.Block,
	tx kv.RwTx,
	batch kv.StatelessRwTx,
	cfg ExecuteBlockCfg,
	vmConfig vm.Config, // emit copy, because will modify it
	writeChangesets bool,
	writeReceipts bool,
	writeCallTraces bool,
	stateStream bool,
	logger log.Logger,
) (err error) {
	blockNum := block.NumberU64()
	stateReader, stateWriter, err := newStateReaderWriter(batch, tx, block, writeChangesets, cfg.accumulator, cfg.blockReader, stateStream)
	if err != nil {
		return err
	}

	// where the magic happens
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, _ := cfg.blockReader.Header(context.Background(), tx, hash, number)
		return h
	}

	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		return tracelogger.NewStructLogger(&tracelogger.LogConfig{}), nil
	}

	callTracer := calltracer.NewCallTracer()
	vmConfig.Debug = true
	vmConfig.Tracer = callTracer

	var receipts types.Receipts
	var stateSyncReceipt *types.Receipt
	var execRs *core.EphemeralExecResult
	getHashFn := core.GetHashFn(block.Header(), getHeader)

	execRs, err = core.ExecuteBlockEphemerally(cfg.chainConfig, &vmConfig, getHashFn, cfg.engine, block, stateReader, stateWriter, consensuschain.NewReader(cfg.chainConfig, tx, cfg.blockReader, logger), getTracer, logger)
	if err != nil {
		return fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, err)
	}
	receipts = execRs.Receipts
	stateSyncReceipt = execRs.StateSyncReceipt

	// If writeReceipts is false here, append the not to be pruned receipts anyways
	if writeReceipts || gatherNoPruneReceipts(&receipts, cfg.chainConfig) {
		if err = rawdb.AppendReceipts(tx, blockNum, receipts); err != nil {
			return err
		}

		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(tx, block.NumberU64(), stateSyncReceipt); err != nil {
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

// Filters out and keeps receipts of the contracts that may be needed by CL, namely of the deposit contract.
func gatherNoPruneReceipts(receipts *types.Receipts, chainCfg *chain.Config) bool {
	cr := types.Receipts{}
	for _, r := range *receipts {
		toStore := false
		if chainCfg.DepositContract == r.ContractAddress {
			toStore = true
		} else {
			for _, l := range r.Logs {
				if chainCfg.DepositContract == l.Address {
					toStore = true
					break
				}
			}
		}

		if toStore {
			cr = append(cr, r)
		}
	}
	receipts = &cr
	return receipts.Len() > 0
}

func newStateReaderWriter(
	batch kv.StatelessRwTx,
	tx kv.RwTx,
	block *types.Block,
	writeChangesets bool,
	accumulator *shards.Accumulator,
	br services.FullBlockReader,
	stateStream bool,
) (state.StateReader, state.WriterWithChangeSets, error) {
	var stateReader state.StateReader
	var stateWriter state.WriterWithChangeSets

	stateReader = state.NewPlainStateReader(batch)

	if stateStream {
		txs, err := br.RawTransactions(context.Background(), tx, block.NumberU64(), block.NumberU64())
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

// ================ Erigon3 ================

func ExecBlockV3(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger) (err error) {
	workersCount := cfg.syncCfg.ExecWorkerCount
	if !initialCycle {
		workersCount = 1
	}

	prevStageProgress, err := senderStageProgress(txc.Tx, cfg.db)
	if err != nil {
		return err
	}

	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}
	if to < s.BlockNumber {
		return nil
	}

	parallel := txc.Tx == nil
	if err := ExecV3(ctx, s, u, workersCount, cfg, txc, parallel, to, logger, initialCycle); err != nil {
		return err
	}
	return nil
}

// reconstituteBlock - First block which is not covered by the history snapshot files
func reconstituteBlock(agg *libstate.Aggregator, db kv.RoDB, tx kv.Tx) (n uint64, ok bool, err error) {
	sendersProgress, err := senderStageProgress(tx, db)
	if err != nil {
		return 0, false, err
	}
	reconToBlock := min(sendersProgress, agg.EndTxNumDomainsFrozen())
	if tx == nil {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
			ok, n, err = rawdbv3.TxNums.FindBlockNum(tx, reconToBlock)
			return err
		}); err != nil {
			return
		}
	} else {
		ok, n, err = rawdbv3.TxNums.FindBlockNum(tx, reconToBlock)
	}
	return
}

var ErrTooDeepUnwind = fmt.Errorf("too deep unwind")

func unwindExec3(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, accumulator *shards.Accumulator, logger log.Logger) (err error) {
	//fmt.Printf("unwindv3: %d -> %d\n", u.CurrentBlockNumber, u.UnwindPoint)
	//start := time.Now()

	unwindToLimit, err := txc.Tx.(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).CanUnwindToBlockNum(txc.Tx)
	if err != nil {
		return err
	}
	if u.UnwindPoint < unwindToLimit {
		return fmt.Errorf("%w: %d < %d", ErrTooDeepUnwind, u.UnwindPoint, unwindToLimit)
	}

	var domains *libstate.SharedDomains
	if txc.Doms == nil {
		domains, err = libstate.NewSharedDomains(txc.Tx, logger)
		if err != nil {
			return err
		}
		defer domains.Close()
	} else {
		domains = txc.Doms
	}
	rs := state.NewStateV3(domains, logger)
	// unwind all txs of u.UnwindPoint block. 1 txn in begin/end of block - system txs
	txNum, err := rawdbv3.TxNums.Min(txc.Tx, u.UnwindPoint+1)
	if err != nil {
		return err
	}
	t := time.Now()
	var changeset *[kv.DomainLen][]libstate.DomainEntryDiff
	for currentBlock := u.CurrentBlockNumber; currentBlock > u.UnwindPoint; currentBlock-- {
		currentHash, err := rawdb.ReadCanonicalHash(txc.Tx, currentBlock)
		if err != nil {
			return err
		}
		var ok bool
		var currentKeys [kv.DomainLen][]libstate.DomainEntryDiff
		currentKeys, ok, err = domains.GetDiffset(txc.Tx, currentHash, currentBlock)
		if !ok {
			return fmt.Errorf("domains.GetDiffset(%d, %s): not found", currentBlock, currentHash)
		}
		if err != nil {
			return err
		}
		if changeset == nil {
			changeset = &currentKeys
		} else {
			for i := range currentKeys {
				changeset[i] = libstate.MergeDiffSets(changeset[i], currentKeys[i])
			}
		}
	}
	if err := rs.Unwind(ctx, txc.Tx, u.UnwindPoint, txNum, accumulator, changeset); err != nil {
		return fmt.Errorf("StateV3.Unwind(%d->%d): %w, took %s", s.BlockNumber, u.UnwindPoint, err, time.Since(t))
	}
	if err := rawdb.TruncateReceipts(txc.Tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate receipts: %w", err)
	}
	if err := rawdb.TruncateBorReceipts(txc.Tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("truncate bor receipts: %w", err)
	}
	if err := rawdb.DeleteNewerEpochs(txc.Tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete newer epochs: %w", err)
	}
	//fmt.Printf("unwindv3: %d -> %d done within %s\n", s.BlockNumber, u.UnwindPoint, time.Since(start))
	return nil
}

func senderStageProgress(tx kv.Tx, db kv.RoDB) (prevStageProgress uint64, err error) {
	if tx != nil {
		prevStageProgress, err = stages.GetStageProgress(tx, stages.Senders)
		if err != nil {
			return prevStageProgress, err
		}
	} else {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
			prevStageProgress, err = stages.GetStageProgress(tx, stages.Senders)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return prevStageProgress, err
		}
	}
	return prevStageProgress, nil
}

// ================ Erigon3 End ================

func SpawnExecuteBlocksStage(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	if dbg.StagesOnlyBlocks {
		return nil
	}
	if err = ExecBlockV3(s, u, txc, toBlock, ctx, cfg, s.CurrentSyncCycle.IsInitialCycle, logger); err != nil {
		return err
	}
	return nil
}

func blocksReadAhead(ctx context.Context, cfg *ExecuteBlockCfg, workers int, engine consensus.Engine, histV3 bool) (chan uint64, context.CancelFunc) {
	const readAheadBlocks = 100
	readAhead := make(chan uint64, readAheadBlocks)
	g, gCtx := errgroup.WithContext(ctx)
	for workerNum := 0; workerNum < workers; workerNum++ {
		g.Go(func() (err error) {
			var bn uint64
			var ok bool
			var tx kv.Tx
			defer func() {
				if tx != nil {
					tx.Rollback()
				}
			}()

			for i := 0; ; i++ {
				select {
				case bn, ok = <-readAhead:
					if !ok {
						return
					}
				case <-gCtx.Done():
					return gCtx.Err()
				}

				if i%100 == 0 {
					if tx != nil {
						tx.Rollback()
					}
					tx, err = cfg.db.BeginRo(ctx)
					if err != nil {
						return err
					}
				}

				if err := blocksReadAheadFunc(gCtx, tx, cfg, bn+readAheadBlocks, engine, histV3); err != nil {
					return err
				}
			}
		})
	}
	return readAhead, func() {
		close(readAhead)
		_ = g.Wait()
	}
}
func blocksReadAheadFunc(ctx context.Context, tx kv.Tx, cfg *ExecuteBlockCfg, blockNum uint64, engine consensus.Engine, histV3 bool) error {
	block, err := cfg.blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}
	_, _ = cfg.engine.Author(block.HeaderNoCopy()) // Bor consensus: this calc is heavy and has cache
	if histV3 {
		return nil
	}

	senders := block.Body().SendersFromTxs()     //TODO: BlockByNumber can return senders
	stateReader := state.NewPlainStateReader(tx) //TODO: can do on batch! if make batch thread-safe
	for _, sender := range senders {
		a, _ := stateReader.ReadAccountData(sender)
		if a == nil || a.Incarnation == 0 {
			continue
		}
		if code, _ := stateReader.ReadAccountCode(sender, a.Incarnation, a.CodeHash); len(code) > 0 {
			_, _ = code[0], code[len(code)-1]
		}
	}

	for _, txn := range block.Transactions() {
		to := txn.GetTo()
		if to == nil {
			continue
		}
		a, _ := stateReader.ReadAccountData(*to)
		if a == nil || a.Incarnation == 0 {
			continue
		}
		if code, _ := stateReader.ReadAccountCode(*to, a.Incarnation, a.CodeHash); len(code) > 0 {
			_, _ = code[0], code[len(code)-1]
		}
	}
	_, _ = stateReader.ReadAccountData(block.Coinbase())
	_, _ = block, senders
	return nil
}

func UnwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	//fmt.Printf("unwind: %d -> %d\n", u.CurrentBlockNumber, u.UnwindPoint)
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := txc.Tx != nil
	if !useExternalTx {
		txc.Tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer txc.Tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	logger.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err = unwindExecutionStage(u, s, txc, ctx, cfg, logger); err != nil {
		return err
	}
	if err = u.Done(txc.Tx); err != nil {
		return err
	}
	//dumpPlainStateDebug(tx, nil)

	if !useExternalTx {
		if err = txc.Tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) error {
	var accumulator *shards.Accumulator
	if cfg.stateStream && s.BlockNumber-u.UnwindPoint < stateStreamLimit {
		accumulator = cfg.accumulator

		hash, err := cfg.blockReader.CanonicalHash(ctx, txc.Tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("read canonical hash of unwind point: %w", err)
		}
		txs, err := cfg.blockReader.RawTransactions(ctx, txc.Tx, u.UnwindPoint, s.BlockNumber)
		if err != nil {
			return err
		}
		accumulator.StartChange(u.UnwindPoint, hash, txs, true)
	}

	//TODO: why we don't call accumulator.ChangeCode???
	return unwindExec3(u, s, txc, ctx, accumulator, logger)
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

func PruneExecutionStage(s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if s.ForwardProgress > config3.MaxReorgDepthV3 {
		// (chunkLen is 8Kb) * (1_000 chunks) = 8mb
		// Some blocks on bor-mainnet have 400 chunks of diff = 3mb
		var pruneDiffsLimitOnChainTip = 1_000
		if s.CurrentSyncCycle.IsInitialCycle {
			pruneDiffsLimitOnChainTip *= 10
		}
		if err := rawdb.PruneTable(tx, kv.ChangeSets3, s.ForwardProgress-config3.MaxReorgDepthV3, ctx, pruneDiffsLimitOnChainTip); err != nil {
			return err
		}
	}

	execStepsInDB.Set(rawdbhelpers.IdxStepsCountV3(tx) * 100)

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	pruneTimeout := 3 * time.Second
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneTimeout = 12 * time.Hour
	}
	if _, err = tx.(*temporal.Tx).AggTx().(*libstate.AggregatorRoTx).PruneSmallBatches(ctx, pruneTimeout, tx); err != nil { // prune part of retired data, before commit
		return err
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
