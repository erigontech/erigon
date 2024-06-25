package stages

import (
	"context"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"math/big"

	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/smt/pkg/blockinfo"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/secp256k1"
	"github.com/ledgerwatch/erigon/zk/utils"
)

func handleStateForNewBlockStarting(
	chainConfig *chain.Config,
	hermezDb *hermez_db.HermezDb,
	ibs *state.IntraBlockState,
	blockNumber uint64,
	batchNumber uint64,
	timestamp uint64,
	stateRoot *common.Hash,
	l1info *zktypes.L1InfoTreeUpdate,
	shouldWriteGerToContract bool,
) error {
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
				if err := hermezDb.WriteLatestUsedGer(batchNumber, l1info.GER); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func finaliseBlock(
	ctx context.Context,
	cfg SequenceBlockCfg,
	s *stagedsync.StageState,
	sdb *stageDb,
	ibs *state.IntraBlockState,
	newHeader *types.Header,
	parentBlock *types.Block,
	forkId uint64,
	batch uint64,
	accumulator *shards.Accumulator,
	ger common.Hash,
	l1BlockHash common.Hash,
	transactions []types.Transaction,
	receipts types.Receipts,
	effectiveGases []uint8,
) (*types.Block, error) {
	stateWriter := state.NewPlainStateWriter(sdb.tx, sdb.tx, newHeader.Number.Uint64()).SetAccumulator(accumulator)
	chainReader := stagedsync.ChainReader{
		Cfg: *cfg.chainConfig,
		Db:  sdb.tx,
	}

	var excessDataGas *big.Int
	if parentBlock != nil {
		excessDataGas = parentBlock.ExcessDataGas()
	}

	txInfos := []blockinfo.ExecutedTxInfo{}
	for i, tx := range transactions {
		var from common.Address
		var err error
		sender, ok := tx.GetSender()
		if ok {
			from = sender
		} else {
			signer := types.MakeSigner(cfg.chainConfig, newHeader.Number.Uint64())
			from, err = tx.Sender(*signer)
			if err != nil {
				return nil, err
			}
		}
		txInfos = append(txInfos, blockinfo.ExecutedTxInfo{
			Tx:                tx,
			EffectiveGasPrice: effectiveGases[i],
			Receipt:           receipts[i],
			Signer:            &from,
		})
	}
	if err := postBlockStateHandling(cfg, ibs, sdb.hermezDb, newHeader, ger, l1BlockHash, parentBlock.Root(), txInfos); err != nil {
		return nil, err
	}

	finalBlock, finalTransactions, finalReceipts, err := core.FinalizeBlockExecutionWithHistoryWrite(
		cfg.engine,
		sdb.stateReader,
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
		return nil, err
	}

	newRoot, err := zkIncrementIntermediateHashes(ctx, s.LogPrefix(), s, sdb.tx, sdb.eridb, sdb.smt, newHeader.Number.Uint64()-1, newHeader.Number.Uint64())
	if err != nil {
		return nil, err
	}

	finalHeader := finalBlock.HeaderNoCopy()
	finalHeader.Root = newRoot
	finalHeader.Coinbase = cfg.zk.AddressSequencer
	finalHeader.GasLimit = utils.GetBlockGasLimitForFork(forkId)
	finalHeader.ReceiptHash = types.DeriveSha(receipts)
	finalHeader.Bloom = types.CreateBloom(receipts)
	newNum := finalBlock.Number()

	err = rawdb.WriteHeader_zkEvm(sdb.tx, finalHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %v", err)
	}
	if err := rawdb.WriteHeadHeaderHash(sdb.tx, finalHeader.Hash()); err != nil {
		return nil, err
	}
	err = rawdb.WriteCanonicalHash(sdb.tx, finalHeader.Hash(), newNum.Uint64())
	if err != nil {
		return nil, fmt.Errorf("failed to write header: %v", err)
	}

	erigonDB := erigon_db.NewErigonDb(sdb.tx)
	err = erigonDB.WriteBody(newNum, finalHeader.Hash(), finalTransactions)
	if err != nil {
		return nil, fmt.Errorf("failed to write body: %v", err)
	}

	// write the new block lookup entries
	rawdb.WriteTxLookupEntries(sdb.tx, finalBlock)

	if err = rawdb.WriteReceipts(sdb.tx, newNum.Uint64(), finalReceipts); err != nil {
		return nil, err
	}

	if err = sdb.hermezDb.WriteForkId(batch, forkId); err != nil {
		return nil, err
	}

	// now process the senders to avoid a stage by itself
	if err := addSenders(cfg, newNum, finalTransactions, sdb.tx, finalHeader); err != nil {
		return nil, err
	}

	// now add in the zk batch to block references
	if err := sdb.hermezDb.WriteBlockBatch(newNum.Uint64(), batch); err != nil {
		return nil, fmt.Errorf("write block batch error: %v", err)
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

	blokInfoRootHash, err := blockinfo.BuildBlockInfoTree(
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

	ibs.PostExecuteStateSet(cfg.chainConfig, header.Number.Uint64(), blokInfoRootHash)

	// store a reference to this block info root against the block number
	return hermezDb.WriteBlockInfoRoot(header.Number.Uint64(), *blokInfoRootHash)
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
