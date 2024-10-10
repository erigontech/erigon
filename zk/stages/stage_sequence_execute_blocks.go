package stages

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"math/big"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/smt/pkg/blockinfo"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/secp256k1"
)

func handleStateForNewBlockStarting(
	batchContext *BatchContext,
	ibs *state.IntraBlockState,
	blockNumber uint64,
	batchNumber uint64,
	timestamp uint64,
	stateRoot *common.Hash,
	l1info *zktypes.L1InfoTreeUpdate,
	shouldWriteGerToContract bool,
) error {
	chainConfig := batchContext.cfg.chainConfig
	hermezDb := batchContext.sdb.hermezDb

	ibs.PreExecuteStateSet(chainConfig, blockNumber, timestamp, stateRoot)

	// handle writing to the ger manager contract but only if the index is above 0
	// block 1 is a special case as it's the injected batch, so we always need to check the GER/L1 block hash
	// as these will be force-fed from the event from L1
	if l1info != nil && l1info.Index > 0 || blockNumber == 1 {
		// store it so we can retrieve for the data stream
		if err := hermezDb.WriteBlockGlobalExitRoot(blockNumber, l1info.GER); err != nil {
			return err
		}
		if err := hermezDb.WriteBlockL1BlockHash(blockNumber, l1info.ParentHash); err != nil {
			return err
		}

		// in the case of a re-used l1 info tree index we don't want to write the ger to the contract
		if shouldWriteGerToContract {
			// first check if this ger has already been written
			l1BlockHash := ibs.ReadGerManagerL1BlockHash(l1info.GER)
			if l1BlockHash == (common.Hash{}) {
				// not in the contract so let's write it!
				ibs.WriteGerManagerL1BlockHash(l1info.GER, l1info.ParentHash)
				if err := hermezDb.WriteLatestUsedGer(blockNumber, l1info.GER); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func doFinishBlockAndUpdateState(
	batchContext *BatchContext,
	ibs *state.IntraBlockState,
	header *types.Header,
	parentBlock *types.Block,
	batchState *BatchState,
	ger common.Hash,
	l1BlockHash common.Hash,
	l1TreeUpdateIndex uint64,
	infoTreeIndexProgress uint64,
	batchCounters *vm.BatchCounterCollector,
) (*types.Block, error) {
	thisBlockNumber := header.Number.Uint64()

	if batchContext.cfg.accumulator != nil {
		batchContext.cfg.accumulator.StartChange(thisBlockNumber, header.Hash(), nil, false)
	}

	block, err := finaliseBlock(batchContext, ibs, header, parentBlock, batchState, ger, l1BlockHash, l1TreeUpdateIndex, infoTreeIndexProgress, batchCounters)
	if err != nil {
		return nil, err
	}

	if err := updateSequencerProgress(batchContext.sdb.tx, thisBlockNumber, batchState.batchNumber, false); err != nil {
		return nil, err
	}

	if batchContext.cfg.accumulator != nil {
		txs, err := rawdb.RawTransactionsRange(batchContext.sdb.tx, thisBlockNumber, thisBlockNumber)
		if err != nil {
			return nil, err
		}
		batchContext.cfg.accumulator.ChangeTransactions(txs)
	}

	return block, nil
}

func finaliseBlock(
	batchContext *BatchContext,
	ibs *state.IntraBlockState,
	newHeader *types.Header,
	parentBlock *types.Block,
	batchState *BatchState,
	ger common.Hash,
	l1BlockHash common.Hash,
	l1TreeUpdateIndex uint64,
	infoTreeIndexProgress uint64,
	batchCounters *vm.BatchCounterCollector,
) (*types.Block, error) {
	thisBlockNumber := newHeader.Number.Uint64()
	if err := batchContext.sdb.hermezDb.WriteBlockL1InfoTreeIndex(thisBlockNumber, l1TreeUpdateIndex); err != nil {
		return nil, err
	}
	if err := batchContext.sdb.hermezDb.WriteBlockL1InfoTreeIndexProgress(thisBlockNumber, infoTreeIndexProgress); err != nil {
		return nil, err
	}

	stateWriter := state.NewPlainStateWriter(batchContext.sdb.tx, batchContext.sdb.tx, newHeader.Number.Uint64()).SetAccumulator(batchContext.cfg.accumulator)
	chainReader := stagedsync.ChainReader{
		Cfg: *batchContext.cfg.chainConfig,
		Db:  batchContext.sdb.tx,
	}

	txInfos := []blockinfo.ExecutedTxInfo{}
	builtBlockElements := batchState.blockState.builtBlockElements
	for i, tx := range builtBlockElements.transactions {
		var from common.Address
		var err error
		sender, ok := tx.GetSender()
		if ok {
			from = sender
		} else {
			signer := types.MakeSigner(batchContext.cfg.chainConfig, newHeader.Number.Uint64(), newHeader.Time)
			from, err = tx.Sender(*signer)
			if err != nil {
				return nil, err
			}
		}
		localReceipt := core.CreateReceiptForBlockInfoTree(builtBlockElements.receipts[i], batchContext.cfg.chainConfig, newHeader.Number.Uint64(), builtBlockElements.executionResults[i])
		txInfos = append(txInfos, blockinfo.ExecutedTxInfo{
			Tx:                tx,
			EffectiveGasPrice: builtBlockElements.effectiveGases[i],
			Receipt:           localReceipt,
			Signer:            &from,
		})
	}

	if err := postBlockStateHandling(*batchContext.cfg, ibs, batchContext.sdb.hermezDb, newHeader, ger, l1BlockHash, parentBlock.Root(), txInfos); err != nil {
		return nil, err
	}

	var withdrawals []*types.Withdrawal
	if batchContext.cfg.chainConfig.IsShanghai(newHeader.Number.Uint64()) {
		withdrawals = []*types.Withdrawal{}
	}

	finalBlock, finalTransactions, finalReceipts, err := core.FinalizeBlockExecution(
		batchContext.cfg.engine,
		batchContext.sdb.stateReader,
		newHeader,
		builtBlockElements.transactions,
		[]*types.Header{}, // no uncles
		stateWriter,
		batchContext.cfg.chainConfig,
		ibs,
		builtBlockElements.receipts,
		withdrawals,
		chainReader,
		true,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// this is actually the interhashes stage
	newRoot, err := zkIncrementIntermediateHashes(batchContext.ctx, batchContext.s.LogPrefix(), batchContext.s, batchContext.sdb.tx, batchContext.sdb.eridb, batchContext.sdb.smt, newHeader.Number.Uint64()-1, newHeader.Number.Uint64())
	if err != nil {
		return nil, err
	}

	finalHeader := finalBlock.HeaderNoCopy()
	finalHeader.Root = newRoot
	finalHeader.Coinbase = batchState.getCoinbase(batchContext.cfg)
	finalHeader.ReceiptHash = types.DeriveSha(builtBlockElements.receipts)
	finalHeader.Bloom = types.CreateBloom(builtBlockElements.receipts)
	newNum := finalBlock.Number()

	err = rawdb.WriteHeader_zkEvm(batchContext.sdb.tx, finalHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %v", err)
	}
	if err := rawdb.WriteHeadHeaderHash(batchContext.sdb.tx, finalHeader.Hash()); err != nil {
		return nil, err
	}
	err = rawdb.WriteCanonicalHash(batchContext.sdb.tx, finalHeader.Hash(), newNum.Uint64())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %v", err)
	}

	erigonDB := erigon_db.NewErigonDb(batchContext.sdb.tx)
	err = erigonDB.WriteBody(newNum, finalHeader.Hash(), finalTransactions)
	if err != nil {
		return nil, fmt.Errorf("failed to write body: %v", err)
	}

	// write the new block lookup entries
	rawdb.WriteTxLookupEntries(batchContext.sdb.tx, finalBlock)

	if err = rawdb.WriteReceipts(batchContext.sdb.tx, newNum.Uint64(), finalReceipts); err != nil {
		return nil, err
	}

	if err = batchContext.sdb.hermezDb.WriteForkId(batchState.batchNumber, batchState.forkId); err != nil {
		return nil, err
	}

	// now process the senders to avoid a stage by itself
	if err := addSenders(*batchContext.cfg, newNum, finalTransactions, batchContext.sdb.tx, finalHeader); err != nil {
		return nil, err
	}

	// now add in the zk batch to block references
	if err := batchContext.sdb.hermezDb.WriteBlockBatch(newNum.Uint64(), batchState.batchNumber); err != nil {
		return nil, fmt.Errorf("write block batch error: %v", err)
	}

	// write batch counters
	err = batchContext.sdb.hermezDb.WriteBatchCounters(newNum.Uint64(), batchCounters.CombineCollectorsNoChanges().UsedAsArray())
	if err != nil {
		return nil, err
	}

	// this is actually account + storage indices stages
	quitCh := batchContext.ctx.Done()
	from := newNum.Uint64()
	if from == 1 {
		from = 0
	}
	to := newNum.Uint64() + 1
	if err = stagedsync.PromoteHistory(batchContext.s.LogPrefix(), batchContext.sdb.tx, kv.AccountChangeSet, from, to, *batchContext.historyCfg, quitCh); err != nil {
		return nil, err
	}
	if err = stagedsync.PromoteHistory(batchContext.s.LogPrefix(), batchContext.sdb.tx, kv.StorageChangeSet, from, to, *batchContext.historyCfg, quitCh); err != nil {
		return nil, err
	}

	return finalBlock, nil
}

func postBlockStateHandling(
	cfg SequenceBlockCfg,
	ibs *state.IntraBlockState,
	hermezDb *hermez_db.HermezDb,
	header *types.Header,
	ger common.Hash,
	l1BlockHash common.Hash,
	parentHash common.Hash,
	txInfos []blockinfo.ExecutedTxInfo,
) error {
	blockNumber := header.Number.Uint64()

	blockInfoRootHash, err := blockinfo.BuildBlockInfoTree(
		&header.Coinbase,
		blockNumber,
		header.Time,
		header.GasLimit,
		header.GasUsed,
		ger,
		l1BlockHash,
		parentHash,
		&txInfos,
	)
	if err != nil {
		return err
	}

	ibs.PostExecuteStateSet(cfg.chainConfig, header.Number.Uint64(), blockInfoRootHash)

	// store a reference to this block info root against the block number
	return hermezDb.WriteBlockInfoRoot(header.Number.Uint64(), *blockInfoRootHash)
}

func addSenders(
	cfg SequenceBlockCfg,
	newNum *big.Int,
	finalTransactions types.Transactions,
	tx kv.RwTx,
	finalHeader *types.Header,
) error {
	signer := types.MakeSigner(cfg.chainConfig, newNum.Uint64(), 0)
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
