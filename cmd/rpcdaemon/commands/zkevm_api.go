package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	jsoniter "github.com/json-iterator/go"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/log/v3"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	smtDb "github.com/ledgerwatch/erigon/smt/pkg/db"
	smt "github.com/ledgerwatch/erigon/smt/pkg/smt"
	smtUtils "github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/zk/constants"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	types "github.com/ledgerwatch/erigon/zk/rpcdaemon"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	zkStages "github.com/ledgerwatch/erigon/zk/stages"
	"github.com/ledgerwatch/erigon/zk/syncer"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/utils"
	zkUtils "github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/erigon/zk/witness"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
)

var sha3UncleHash = common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")

// ZkEvmAPI is a collection of functions that are exposed in the
type ZkEvmAPI interface {
	ConsolidatedBlockNumber(ctx context.Context) (hexutil.Uint64, error)
	IsBlockConsolidated(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error)
	IsBlockVirtualized(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error)
	BatchNumberByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error)
	BatchNumber(ctx context.Context) (hexutil.Uint64, error)
	VirtualBatchNumber(ctx context.Context) (hexutil.Uint64, error)
	VerifiedBatchNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBatchByNumber(ctx context.Context, batchNumber rpc.BlockNumber, fullTx *bool) (json.RawMessage, error)
	GetFullBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (types.Block, error)
	GetFullBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (types.Block, error)
	// GetBroadcastURI(ctx context.Context) (string, error)
	GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error)
	GetBlockRangeWitness(ctx context.Context, startBlockNrOrHash rpc.BlockNumberOrHash, endBlockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error)
	GetBatchWitness(ctx context.Context, batchNumber uint64, mode *WitnessMode) (interface{}, error)
	GetProverInput(ctx context.Context, batchNumber uint64, mode *WitnessMode, debug *bool) (*legacy_executor_verifier.RpcPayload, error)
	GetLatestGlobalExitRoot(ctx context.Context) (common.Hash, error)
	GetExitRootsByGER(ctx context.Context, globalExitRoot common.Hash) (*ZkExitRoots, error)
	GetL2BlockInfoTree(ctx context.Context, blockNum rpc.BlockNumberOrHash) (json.RawMessage, error)
	EstimateCounters(ctx context.Context, argsOrNil *zkevmRPCTransaction) (json.RawMessage, error)
	TraceTransactionCounters(ctx context.Context, hash common.Hash, config *tracers.TraceConfig_ZkEvm, stream *jsoniter.Stream) error
	GetBatchCountersByNumber(ctx context.Context, batchNumRpc rpc.BlockNumber) (res json.RawMessage, err error)
	GetExitRootTable(ctx context.Context) ([]l1InfoTreeData, error)
	GetVersionHistory(ctx context.Context) (json.RawMessage, error)
	GetForkId(ctx context.Context) (hexutil.Uint64, error)
	GetForkById(ctx context.Context, forkId hexutil.Uint64) (res json.RawMessage, err error)
	GetForkIdByBatchNumber(ctx context.Context, batchNumber rpc.BlockNumber) (hexutil.Uint64, error)
	GetForks(ctx context.Context) (res json.RawMessage, err error)
}

const getBatchWitness = "getBatchWitness"

// APIImpl is implementation of the ZkEvmAPI interface based on remote Db access
type ZkEvmAPIImpl struct {
	ethApi *APIImpl

	db              kv.RoDB
	ReturnDataLimit int
	config          *ethconfig.Config
	l1Syncer        *syncer.L1Syncer
	l2SequencerUrl  string
	semaphores      map[string]chan struct{}
}

func (api *ZkEvmAPIImpl) initializeSemaphores(functionLimits map[string]int) {
	api.semaphores = make(map[string]chan struct{})

	for funcName, limit := range functionLimits {
		if limit != 0 {
			api.semaphores[funcName] = make(chan struct{}, limit)
		}
	}
}

// NewEthAPI returns ZkEvmAPIImpl instance
func NewZkEvmAPI(
	base *APIImpl,
	db kv.RoDB,
	returnDataLimit int,
	zkConfig *ethconfig.Config,
	l1Syncer *syncer.L1Syncer,
	l2SequencerUrl string,
) *ZkEvmAPIImpl {

	a := &ZkEvmAPIImpl{
		ethApi:          base,
		db:              db,
		ReturnDataLimit: returnDataLimit,
		config:          zkConfig,
		l1Syncer:        l1Syncer,
		l2SequencerUrl:  l2SequencerUrl,
	}

	a.initializeSemaphores(map[string]int{
		getBatchWitness: zkConfig.Zk.RpcGetBatchWitnessConcurrencyLimit,
	})

	return a
}

// ConsolidatedBlockNumber returns the latest consolidated block number
// Once a batch is verified, it is connected to the blockchain, and the block number of the most recent block in that batch
// becomes the "consolidated block number.”
func (api *ZkEvmAPIImpl) ConsolidatedBlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return hexutil.Uint64(0), err
	}

	blockNum, err := getLastBlockInBatchNumber(tx, highestVerifiedBatchNo)
	if err != nil {
		return hexutil.Uint64(0), err
	}

	return hexutil.Uint64(blockNum), nil
}

// IsBlockConsolidated returns true if the block is consolidated
func (api *ZkEvmAPIImpl) IsBlockConsolidated(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	batchNum, err := getBatchNoByL2Block(tx, uint64(blockNumber.Int64()))
	if errors.Is(err, hermez_db.ErrorNotStored) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return false, err
	}

	return batchNum <= highestVerifiedBatchNo, nil
}

// IsBlockVirtualized returns true if the block is virtualized (not confirmed on the L1 but exists in the L1 smart contract i.e. sequenced)
func (api *ZkEvmAPIImpl) IsBlockVirtualized(ctx context.Context, blockNumber rpc.BlockNumber) (bool, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	batchNum, err := getBatchNoByL2Block(tx, uint64(blockNumber.Int64()))
	if errors.Is(err, hermez_db.ErrorNotStored) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	hermezDb := hermez_db.NewHermezDbReader(tx)
	latestSequencedBatch, err := hermezDb.GetLatestSequence()
	if err != nil {
		return false, err
	}

	// if the batch is lower than the latest sequenced then it must be virtualized
	return batchNum <= latestSequencedBatch.BatchNo, nil
}

// BatchNumberByBlockNumber returns the batch number of the block
func (api *ZkEvmAPIImpl) BatchNumberByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	batchNum, err := getBatchNoByL2Block(tx, uint64(blockNumber.Int64()))
	if err != nil {
		return hexutil.Uint64(0), err
	}

	return hexutil.Uint64(batchNum), err
}

// BatchNumber returns the latest batch number
func (api *ZkEvmAPIImpl) BatchNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	currentBatchNumber, err := getLatestBatchNumber(tx)
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(currentBatchNumber), err
}

// VirtualBatchNumber returns the latest virtual batch number
// A virtual batch is a batch that is in the process of being created and has not yet been verified.
// The virtual batch number represents the next batch to be verified using zero-knowledge proofs.
func (api *ZkEvmAPIImpl) VirtualBatchNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	latestSequencedBatch, err := hermezDb.GetLatestSequence()
	if err != nil {
		return hexutil.Uint64(0), err
	}

	if latestSequencedBatch == nil {
		return hexutil.Uint64(0), nil
	}

	// todo: what if this number is the same as the last verified batch number?  do we return 0?

	return hexutil.Uint64(latestSequencedBatch.BatchNo), nil
}

// VerifiedBatchNumber returns the latest verified batch number
// A batch is considered verified once its proof has been validated and accepted by the network.
func (api *ZkEvmAPIImpl) VerifiedBatchNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	highestVerifiedBatchNo, err := stages.GetStageProgress(tx, stages.L1VerificationsBatchNo)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	return hexutil.Uint64(highestVerifiedBatchNo), nil
}

// GetBatchDataByNumbers returns the batch data for the given batch numbers
func (api *ZkEvmAPIImpl) GetBatchDataByNumbers(ctx context.Context, batchNumbers rpc.RpcNumberArray) (json.RawMessage, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)

	// use inbuilt rpc.BlockNumber type to implement the 'latest' behaviour
	// highest block/batch tied to last block synced
	// unless the node is still syncing - in which case 'current block' is used
	// this is the batch number of stage progress of the Finish stage

	highestBlock, err := rawdb.ReadLastBlockSynced(tx)
	if err != nil {
		return nil, err
	}

	highestBatchNo, err := hermezDb.GetBatchNoByL2Block(highestBlock.NumberU64())
	if err != nil {
		return nil, err
	}

	// check sync status of node
	syncing, err := api.ethApi.Syncing(ctx)
	if err != nil {
		return nil, err
	}
	if syncing != nil && syncing != false {
		bn := syncing.(map[string]interface{})["currentBlock"]
		highestBatchNo, err = hermezDb.GetBatchNoByL2Block(uint64(bn.(hexutil.Uint64)))
		if err != nil {
			return nil, err
		}
	}

	bds := make([]*types.BatchDataSlim, 0, len(batchNumbers.Numbers))

	for _, batchNumber := range batchNumbers.Numbers {
		bd := &types.BatchDataSlim{
			Number: types.ArgUint64(batchNumber.Int64()),
			Empty:  false,
		}

		// return null if we're not at this block height yet
		if batchNumber > rpc.BlockNumber(highestBatchNo) {
			bd.Empty = true
			bds = append(bds, bd)
			continue
		}

		// try to find the BatchData in db to avoid calculate it when it is possible
		batchData, err := hermezDb.GetL1BatchData(batchNumber.Uint64())
		if err != nil {
			return nil, err
		} else if len(batchData) != 0 {
			bd.BatchL2Data = batchData
			bds = append(bds, bd)
			continue
		}

		// looks weird but we're using the rpc.BlockNumber type to represent the batch number, LatestBlockNumber represents latest batch
		if batchNumber == rpc.LatestBlockNumber {
			batchNumber = rpc.BlockNumber(highestBatchNo)
		}

		batchNo := uint64(batchNumber.Int64())

		_, found, err := hermezDb.GetLowestBlockInBatch(batchNo)
		if err != nil {
			return nil, err
		}
		if !found {
			// not found - set to empty and append
			bd.Empty = true
			bds = append(bds, bd)
			continue
		}

		// block numbers in batch
		blocksInBatch, err := hermezDb.GetL2BlockNosByBatch(batchNo)
		if err != nil {
			return nil, err
		}

		// collect blocks in batch
		var batchBlocks []*eritypes.Block
		// handle genesis - not in the hermez tables so requires special treament
		if batchNumber == 0 {
			blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, 0)
			if err != nil {
				return nil, err
			}
			batchBlocks = append(batchBlocks, blk)
			// no txs in genesis
		}
		for _, blkNo := range blocksInBatch {
			blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, blkNo)
			if err != nil {
				return nil, err
			}
			batchBlocks = append(batchBlocks, blk)
		}

		// batch l2 data - must build on the fly
		forkId, err := hermezDb.GetForkId(batchNo)
		if err != nil {
			return nil, err
		}

		batchL2Data, err := utils.GenerateBatchData(tx, hermezDb, batchBlocks, forkId)
		if err != nil {
			return nil, err
		}

		bd.BatchL2Data = batchL2Data
		bds = append(bds, bd)
	}

	return populateBatchDataSlimDetails(bds)
}

func generateBatchData(
	tx kv.Tx,
	hermezDb *hermez_db.HermezDbReader,
	batchBlocks []*eritypes.Block,
	forkId uint64,
) (batchL2Data []byte, err error) {
	if len(batchBlocks) == 0 {
		return batchL2Data, nil
	}

	lastBlockNoInPreviousBatch := uint64(0)
	if batchBlocks[0].NumberU64() != 0 {
		lastBlockNoInPreviousBatch = batchBlocks[0].NumberU64() - 1
	}

	lastBlockInPreviousBatch, err := rawdb.ReadBlockByNumber(tx, lastBlockNoInPreviousBatch)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(batchBlocks); i++ {
		var dTs uint32
		if i == 0 {
			dTs = uint32(batchBlocks[i].Time() - lastBlockInPreviousBatch.Time())
		} else {
			dTs = uint32(batchBlocks[i].Time() - batchBlocks[i-1].Time())
		}
		iti, err := hermezDb.GetBlockL1InfoTreeIndex(batchBlocks[i].NumberU64())
		if err != nil {
			return nil, err
		}
		egTx := make(map[common.Hash]uint8)
		for _, txn := range batchBlocks[i].Transactions() {
			eg, err := hermezDb.GetEffectiveGasPricePercentage(txn.Hash())
			if err != nil {
				return nil, err
			}
			egTx[txn.Hash()] = eg
		}

		// block 0 does not geenrate any data (special case)
		var bl2d []byte
		if batchBlocks[i].NumberU64() != 0 {
			if bl2d, err = zktx.GenerateBlockBatchL2Data(uint16(forkId), dTs, uint32(iti), batchBlocks[i].Transactions(), egTx); err != nil {
				return nil, err
			}
		}
		batchL2Data = append(batchL2Data, bl2d...)
	}

	return batchL2Data, err
}

// GetBatchByNumber returns a batch from the current canonical chain. If number is nil, the
// latest known batch is returned.
func (api *ZkEvmAPIImpl) GetBatchByNumber(ctx context.Context, batchNumber rpc.BlockNumber, fullTx *bool) (json.RawMessage, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	hermezDb := hermez_db.NewHermezDbReader(tx)
	// use inbuilt rpc.BlockNumber type to implement the 'latest' behaviour
	// the highest block/batch is tied to last block synced
	// unless the node is still syncing - in which case 'current block' is used
	// this is the batch number of stage progress of the Finish stage

	highestBlock, err := rawdb.ReadLastBlockSynced(tx)
	if err != nil {
		return nil, err
	}

	highestBatchNo, err := hermezDb.GetBatchNoByL2Block(highestBlock.NumberU64())
	if err != nil {
		return nil, err
	}

	// check sync status of node
	syncStatus, err := api.ethApi.Syncing(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := syncStatus.(bool); !ok {
		bn := syncStatus.(map[string]interface{})["currentBlock"]
		if highestBatchNo, err = hermezDb.GetBatchNoByL2Block(uint64(bn.(hexutil.Uint64))); err != nil {
			return nil, err
		}

	}
	if batchNumber > rpc.BlockNumber(highestBatchNo) {
		return nil, nil
	}

	// looks weird but we're using the rpc.BlockNumber type to represent the batch number, LatestBlockNumber represents latest batch
	if batchNumber == rpc.LatestBlockNumber {
		batchNumber = rpc.BlockNumber(highestBatchNo)
	}

	batchNo := uint64(batchNumber.Int64())

	batch := &types.Batch{
		Number: types.ArgUint64(batchNo),
	}

	// loop until we find a block in the batch
	var found bool
	var blockNo, counter uint64
	for !found {
		// highest block in batch
		blockNo, found, err = hermezDb.GetHighestBlockInBatch(batchNo - counter)
		if err != nil {
			return nil, err
		}
		counter++
	}

	block, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, blockNo)
	if err != nil {
		return nil, err
	}

	// last block in batch data
	batch.Coinbase = block.Coinbase()
	batch.StateRoot = block.Root()

	// block numbers in batch
	blocksInBatch, err := hermezDb.GetL2BlockNosByBatch(batchNo)
	if err != nil {
		return nil, err
	}

	// collect blocks in batch
	batch.Blocks = []interface{}{}
	batch.Transactions = []interface{}{}
	var batchBlocks []*eritypes.Block
	var batchTxs []eritypes.Transaction
	// handle genesis - not in the hermez tables so requires special treament
	if batchNumber == 0 {
		blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, 0)
		if err != nil {
			return nil, err
		}
		batchBlocks = append(batchBlocks, blk)
		batch.Blocks = append(batch.Blocks, blk.Hash())
		// no txs in genesis
	}
	for _, blkNo := range blocksInBatch {
		blk, err := api.ethApi.BaseAPI.blockByNumberWithSenders(tx, blkNo)
		if err != nil {
			return nil, err
		}
		batchBlocks = append(batchBlocks, blk)
		batch.Blocks = append(batch.Blocks, blk.Hash())
		for _, btx := range blk.Transactions() {
			batchTxs = append(batchTxs, btx)
			batch.Transactions = append(batch.Transactions, btx.Hash())
		}
	}

	if fullTx != nil && *fullTx {
		batchBlocksJson := make([]interface{}, 0, len(blocksInBatch))
		batchTransactionsJson := make([]interface{}, 0, len(batchTxs))
		for _, blk := range batchBlocks {
			bbj, err := api.populateBlockDetail(tx, ctx, blk, true)
			if err != nil {
				return nil, err
			}

			bir, err := hermezDb.GetBlockInfoRoot(blk.NumberU64())
			if err != nil {
				return nil, err
			}

			ger, err := hermezDb.GetBlockGlobalExitRoot(blk.NumberU64())
			if err != nil {
				return nil, err
			}

			batchBlockExtra := &types.BlockWithInfoRootAndGer{
				Block:          &bbj,
				BlockInfoRoot:  bir,
				GlobalExitRoot: ger,
			}

			// txs
			hashes := make([]types.TransactionOrHash, len(bbj.Transactions))
			for i, txn := range bbj.Transactions {

				blkTx := blk.Transactions()[i]
				l2TxHash, err := zktx.ComputeL2TxHash(
					blkTx.GetChainID().ToBig(),
					blkTx.GetValue(),
					blkTx.GetPrice(),
					blkTx.GetNonce(),
					blkTx.GetGas(),
					blkTx.GetTo(),
					&txn.Tx.From,
					blkTx.GetData(),
				)
				if err != nil {
					return nil, err
				}

				txn.Tx.L2Hash = l2TxHash
				txn.Tx.Receipt.TransactionL2Hash = l2TxHash

				batchTransactionsJson = append(batchTransactionsJson, txn)
				txn.Hash = &txn.Tx.Hash
				txn.Tx = nil
				hashes[i] = txn
			}

			// after collecting transactions, reduce them to them hash only on the block
			bbj.Transactions = hashes

			batchBlocksJson = append(batchBlocksJson, batchBlockExtra)
		}

		batch.Blocks = batchBlocksJson
		batch.Transactions = batchTransactionsJson
	}

	// for consistency with legacy node, return nil if no transactions
	if len(batch.Transactions) == 0 {
		batch.Transactions = nil
	}

	if len(batch.Blocks) == 0 {
		batch.Blocks = nil
	}

	// batch l2 data - must build on the fly
	forkId, err := hermezDb.GetForkId(batchNo)
	if err != nil {
		return nil, err
	}

	// global exit root of batch
	batchGer, _, err := hermezDb.GetLastBlockGlobalExitRoot(blockNo)
	if err != nil {
		return nil, err
	}

	batch.GlobalExitRoot = batchGer

	// sequence
	seq, err := hermezDb.GetSequenceByBatchNoOrHighest(batchNo)
	if err != nil {
		return nil, err
	}
	if batchNo == 0 {
		batch.SendSequencesTxHash = &common.Hash{0}
	} else if seq != nil {
		batch.SendSequencesTxHash = &seq.L1TxHash
	}

	// timestamp - ts of highest block in the batch always
	if block != nil {
		batch.Timestamp = types.ArgUint64(block.Time())
	}

	if _, found, err = hermezDb.GetLowestBlockInBatch(batchNo + 1); err != nil {
		return nil, err
	}
	// sequenced, genesis or injected batch 1 - special batches 0,1 will always be closed, if next batch has blocks, bn must be closed
	batch.Closed = seq != nil || batchNo == 0 || batchNo == 1 || found

	// verification - if we can't find one, maybe this batch was verified along with a higher batch number
	ver, err := hermezDb.GetVerificationByBatchNoOrHighest(batchNo)
	if err != nil {
		return nil, err
	}

	if batchNo == 0 {
		batch.VerifyBatchTxHash = &common.Hash{0}
	} else if ver != nil {
		batch.VerifyBatchTxHash = &ver.L1TxHash
	}

	itu, err := hermezDb.GetL1InfoTreeUpdateByGer(batchGer)
	if err != nil {
		return nil, err
	}
	if itu != nil {
		batch.MainnetExitRoot = itu.MainnetExitRoot
		batch.RollupExitRoot = itu.RollupExitRoot
	}

	// local exit root
	localExitRoot, err := utils.GetBatchLocalExitRootFromSCStorageForLatestBlock(batchNo, hermezDb, tx)
	if err != nil {
		return nil, err
	}
	batch.LocalExitRoot = localExitRoot

	batchL2Data, err := generateBatchData(tx, hermezDb, batchBlocks, forkId)
	if err != nil {
		return nil, err
	}
	batch.BatchL2Data = batchL2Data

	if api.l1Syncer != nil {
		accInputHash, err := api.getAccInputHash(ctx, hermezDb, batchNo)
		if err != nil {
			log.Error(fmt.Sprintf("failed to get acc input hash for batch %d: %v", batchNo, err))
		}
		if accInputHash == nil {
			accInputHash = &common.Hash{}
		}
		batch.AccInputHash = *accInputHash
	}

	// forkid exit roots logic
	// if forkid < 12 then we should only set the exit roots if they have changed, otherwise 0x00..00
	// if forkid >= 12 then we should always set the exit roots
	if forkId < uint64(constants.ForkID12Banana) {
		// get the previous batches exit roots
		prevBatchNo := batchNo - 1
		prevBatchHighestBlock, _, err := hermezDb.GetHighestBlockInBatch(prevBatchNo)
		if err != nil {
			return nil, err
		}
		prevBatchGer, _, err := hermezDb.GetLastBlockGlobalExitRoot(prevBatchHighestBlock)
		if err != nil {
			return nil, err
		}

		if batchGer == prevBatchGer {
			batch.GlobalExitRoot = common.Hash{}
			batch.MainnetExitRoot = common.Hash{}
			batch.RollupExitRoot = common.Hash{}
		}
	}

	return populateBatchDetails(batch)
}

type SequenceReader interface {
	GetRangeSequencesByBatch(batchNo uint64) (*zktypes.L1BatchInfo, *zktypes.L1BatchInfo, error)
	GetForkId(batchNo uint64) (uint64, error)
}

func (api *ZkEvmAPIImpl) getAccInputHash(ctx context.Context, db SequenceReader, batchNum uint64) (accInputHash *common.Hash, err error) {
	// get batch sequence
	prevSequence, batchSequence, err := db.GetRangeSequencesByBatch(batchNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get sequence range data for batch %d: %w", batchNum, err)
	}

	if prevSequence == nil || batchSequence == nil {
		return nil, fmt.Errorf("failed to get sequence data for batch %d", batchNum)
	}

	// get batch range for sequence
	prevSequenceBatch, currentSequenceBatch := prevSequence.BatchNo, batchSequence.BatchNo
	// get call data for tx
	l1Transaction, _, err := api.l1Syncer.GetTransaction(batchSequence.L1TxHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction data for tx %s: %w", batchSequence.L1TxHash, err)
	}
	sequenceBatchesCalldata := l1Transaction.GetData()
	if len(sequenceBatchesCalldata) < 10 {
		return nil, fmt.Errorf("calldata for tx %s is too short", batchSequence.L1TxHash)
	}

	currentBatchForkId, err := db.GetForkId(currentSequenceBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to get fork id for batch %d: %w", currentSequenceBatch, err)
	}

	prevSequenceAccinputHash, err := api.GetccInputHash(ctx, currentBatchForkId, prevSequenceBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to get old acc input hash for batch %d: %w", prevSequenceBatch, err)
	}

	decodedSequenceInteerface, err := syncer.DecodeSequenceBatchesCalldata(sequenceBatchesCalldata)
	if err != nil {
		return nil, fmt.Errorf("failed to decode calldata for tx %s: %w", batchSequence.L1TxHash, err)
	}

	accInputHashCalcFn, totalSequenceBatches, err := syncer.GetAccInputDataCalcFunction(batchSequence.L1InfoRoot, decodedSequenceInteerface)
	if err != nil {
		return nil, fmt.Errorf("failed to get accInputHash calculation func: %w", err)
	}

	if totalSequenceBatches == 0 || batchNum-prevSequenceBatch > uint64(totalSequenceBatches) {
		return nil, fmt.Errorf("batch %d is out of range of sequence calldata", batchNum)
	}

	accInputHash = &prevSequenceAccinputHash
	// calculate acc input hash
	for i := 0; i < int(batchNum-prevSequenceBatch); i++ {
		accInputHash = accInputHashCalcFn(prevSequenceAccinputHash, i)
	}

	return
}

func (api *ZkEvmAPIImpl) GetccInputHash(ctx context.Context, currentBatchForkId, lastSequenceBatchNumber uint64) (accInputHash common.Hash, err error) {
	if currentBatchForkId < uint64(constants.ForkID8Elderberry) {
		accInputHash, err = api.l1Syncer.GetPreElderberryAccInputHash(ctx, &api.config.AddressRollup, lastSequenceBatchNumber)
	} else {
		accInputHash, err = api.l1Syncer.GetElderberryAccInputHash(ctx, &api.config.AddressRollup, api.config.L1RollupId, lastSequenceBatchNumber)
	}

	if err != nil {
		err = fmt.Errorf("failed to get accInputHash batch %d: %w", lastSequenceBatchNumber, err)
	}

	return
}

// GetFullBlockByNumber returns a full block from the current canonical chain. If number is nil, the
// latest known block is returned.
func (api *ZkEvmAPIImpl) GetFullBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (types.Block, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return types.Block{}, err
	}
	defer tx.Rollback()

	baseBlock, err := api.ethApi.BaseAPI.blockByRPCNumber(number, tx)
	if err != nil {
		return types.Block{}, err
	}
	if baseBlock == nil {
		return types.Block{}, errors.New("could not find block")
	}

	return api.populateBlockDetail(tx, ctx, baseBlock, fullTx)
}

// GetFullBlockByHash returns a full block from the current canonical chain. If number is nil, the
// latest known block is returned.
func (api *ZkEvmAPIImpl) GetFullBlockByHash(ctx context.Context, hash libcommon.Hash, fullTx bool) (types.Block, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return types.Block{}, err
	}
	defer tx.Rollback()

	baseBlock, err := api.ethApi.BaseAPI.blockByHashWithSenders(tx, hash)
	if err != nil {
		return types.Block{}, err
	}
	if baseBlock == nil {
		return types.Block{}, fmt.Errorf("block not found")
	}

	return api.populateBlockDetail(tx, ctx, baseBlock, fullTx)
}

// zkevm_getExitRootsByGER returns the exit roots accordingly to the provided Global Exit Root
func (api *ZkEvmAPIImpl) GetExitRootsByGER(ctx context.Context, globalExitRoot common.Hash) (*ZkExitRoots, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	infoTreeUpdate, err := hermezDb.GetL1InfoTreeUpdateByGer(globalExitRoot)
	if err != nil {
		return nil, err
	}

	if infoTreeUpdate == nil {
		return nil, nil
	}

	return &ZkExitRoots{
		BlockNumber:     types.ArgUint64(infoTreeUpdate.BlockNumber),
		Timestamp:       types.ArgUint64(infoTreeUpdate.Timestamp),
		MainnetExitRoot: infoTreeUpdate.MainnetExitRoot,
		RollupExitRoot:  infoTreeUpdate.RollupExitRoot,
	}, nil
}

func (api *ZkEvmAPIImpl) populateBlockDetail(
	tx kv.Tx,
	ctx context.Context,
	baseBlock *eritypes.Block,
	fullTx bool,
) (types.Block, error) {
	cc, err := api.ethApi.chainConfig(tx)
	if err != nil {
		return types.Block{}, err
	}

	// doing this here seems stragne, and it is.  But because we change the header hash in execution
	// to populate details we don't have in the batches stage, the senders are held against the wrong hash.
	// the call later to `getReceipts` sets the incorrect sender because of this so we need to calc and hold
	// these ahead of time.  TODO: fix senders stage to avoid this or update them with the new hash in execution
	number := baseBlock.NumberU64()
	hermezReader := hermez_db.NewHermezDbReader(tx)

	sendersFixed := false
	var senders []common.Address
	var signer *eritypes.Signer
	if sendersFixed {
		senders = baseBlock.Body().SendersFromTxs()
	} else {
		signer = eritypes.MakeSigner(cc, number)
	}

	var effectiveGasPricePercentages []uint8
	if fullTx {
		for _, txn := range baseBlock.Transactions() {
			if signer != nil {
				sender, err := txn.Sender(*signer)
				if err != nil {
					return types.Block{}, err
				}
				senders = append(senders, sender)
			}
			effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(txn.Hash())
			if err != nil {
				return types.Block{}, err
			}
			effectiveGasPricePercentages = append(effectiveGasPricePercentages, effectiveGasPricePercentage)
		}
	}

	receipts, err := api.ethApi.BaseAPI.getReceipts(ctx, tx, cc, baseBlock, baseBlock.Body().SendersFromTxs())
	if err != nil {
		return types.Block{}, err
	}

	return convertBlockToRpcBlock(baseBlock, receipts, senders, effectiveGasPricePercentages, fullTx)
}

// GetBroadcastURI returns the URI of the broadcaster - the trusted sequencer
// func (api *ZkEvmAPIImpl) GetBroadcastURI(ctx context.Context) (string, error) {
// 	return api.ethApi.ZkRpcUrl, nil
// }

func (api *ZkEvmAPIImpl) GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error) {
	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	dbg := false
	if debug != nil {
		dbg = *debug
	}
	return api.getBlockRangeWitness(ctx, api.db, blockNrOrHash, blockNrOrHash, dbg, checkedMode)
}

func (api *ZkEvmAPIImpl) GetBlockRangeWitness(ctx context.Context, startBlockNrOrHash rpc.BlockNumberOrHash, endBlockNrOrHash rpc.BlockNumberOrHash, mode *WitnessMode, debug *bool) (hexutility.Bytes, error) {
	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	dbg := false
	if debug != nil {
		dbg = *debug
	}
	return api.getBlockRangeWitness(ctx, api.db, startBlockNrOrHash, endBlockNrOrHash, dbg, checkedMode)
}

func (api *ZkEvmAPIImpl) getBatchWitness(ctx context.Context, tx kv.Tx, batchNum uint64, debug bool, mode WitnessMode) (hexutility.Bytes, error) {

	// limit in-flight requests by name
	semaphore := api.semaphores[getBatchWitness]
	if semaphore != nil {
		select {
		case semaphore <- struct{}{}:
			defer func() { <-semaphore }()
		default:
			return nil, fmt.Errorf("busy")
		}
	}

	if api.ethApi.historyV3(tx) {
		return nil, fmt.Errorf("not supported by Erigon3")
	}

	generator, fullWitness, err := api.buildGenerator(tx, mode)
	if err != nil {
		return nil, err
	}

	return generator.GetWitnessByBatch(tx, ctx, batchNum, debug, fullWitness)

}

func (api *ZkEvmAPIImpl) buildGenerator(tx kv.Tx, witnessMode WitnessMode) (*witness.Generator, bool, error) {
	chainConfig, err := api.ethApi.chainConfig(tx)
	if err != nil {
		return nil, false, err
	}

	generator := witness.NewGenerator(
		api.ethApi.dirs,
		api.ethApi.historyV3(tx),
		api.ethApi._agg,
		api.ethApi._blockReader,
		chainConfig,
		api.config.Zk,
		api.ethApi._engine,
	)

	fullWitness := false
	if witnessMode == WitnessModeNone {
		fullWitness = api.config.WitnessFull
	} else if witnessMode == WitnessModeFull {
		fullWitness = true
	}

	return generator, fullWitness, nil
}

// Get witness for a range of blocks [startBlockNrOrHash, endBlockNrOrHash] (inclusive)
func (api *ZkEvmAPIImpl) getBlockRangeWitness(ctx context.Context, db kv.RoDB, startBlockNrOrHash rpc.BlockNumberOrHash, endBlockNrOrHash rpc.BlockNumberOrHash, debug bool, witnessMode WitnessMode) (hexutility.Bytes, error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if api.ethApi.historyV3(tx) {
		return nil, fmt.Errorf("not supported by Erigon3")
	}

	blockNr, _, _, err := rpchelper.GetCanonicalBlockNumber(startBlockNrOrHash, tx, api.ethApi.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}

	endBlockNr, _, _, err := rpchelper.GetCanonicalBlockNumber(endBlockNrOrHash, tx, api.ethApi.filters) // DoCall cannot be executed on non-canonical blocks

	if err != nil {
		return nil, err
	}

	if blockNr > endBlockNr {
		return nil, fmt.Errorf("start block number must be less than or equal to end block number, start=%d end=%d", blockNr, endBlockNr)
	}

	generator, fullWitness, err := api.buildGenerator(tx, witnessMode)
	if err != nil {
		return nil, err
	}

	return generator.GetWitnessByBlockRange(tx, ctx, blockNr, endBlockNr, debug, fullWitness)
}

type WitnessMode string

const (
	WitnessModeNone         WitnessMode = "none"
	WitnessModeFull         WitnessMode = "full"          // if the node mode is "full witness" - will return witness from cache
	WitnessModeTrimmed      WitnessMode = "trimmed"       // if the node mode is "partial witness" - will return witness from cache
	WitnessModeFullRegen    WitnessMode = "full_regen"    // forces regenerate no matter the node mode
	WitnessModeTrimmedRegen WitnessMode = "trimmed_regen" // forces regenerate no matter the node mode
)

func (api *ZkEvmAPIImpl) GetBatchWitness(ctx context.Context, batchNumber uint64, mode *WitnessMode) (interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	badBatch, err := hermezDb.GetInvalidBatch(batchNumber)
	if err != nil {
		return nil, err
	}

	if badBatch && !sequencer.IsSequencer() {
		// we won't have the details in our db if the batch is marked as invalid so we need to check this
		// here
		return api.sendGetBatchWitness(api.l2SequencerUrl, batchNumber, mode)
	}

	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	isWitnessModeNone := checkedMode == WitnessModeNone
	rpcModeMatchesNodeMode :=
		checkedMode == WitnessModeFull && api.config.WitnessFull ||
			checkedMode == WitnessModeTrimmed && !api.config.WitnessFull
	// we only want to check the cache if no special run mode has been supplied.
	// or if requested mode matches the node mode
	// otherwise regenerate it
	if isWitnessModeNone || rpcModeMatchesNodeMode {
		hermezDb := hermez_db.NewHermezDbReader(tx)
		witnessCached, err := hermezDb.GetWitness(batchNumber)
		if err != nil {
			return nil, err
		}
		if witnessCached != nil {
			return witnessCached, nil
		}
	}

	return api.getBatchWitness(ctx, tx, batchNumber, false, checkedMode)
}

func (api *ZkEvmAPIImpl) GetProverInput(ctx context.Context, batchNumber uint64, mode *WitnessMode, debug *bool) (*legacy_executor_verifier.RpcPayload, error) {
	if !sequencer.IsSequencer() {
		return nil, errors.New("method only supported from a sequencer node")
	}

	checkedMode := WitnessModeNone
	if mode != nil && *mode != WitnessModeFull && *mode != WitnessModeTrimmed {
		return nil, errors.New("invalid mode, must be full or trimmed")
	} else if mode != nil {
		checkedMode = *mode
	}

	useDebug := false
	if debug != nil {
		useDebug = *debug
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hDb := hermez_db.NewHermezDbReader(tx)

	blockNumbers, err := hDb.GetL2BlockNosByBatch(batchNumber)
	if err != nil {
		return nil, err
	}

	lastBlock, err := rawdb.ReadBlockByNumber(tx, blockNumbers[len(blockNumbers)-1])
	if err != nil {
		return nil, err
	}

	start := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumbers[0]))
	end := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumbers[len(blockNumbers)-1]))

	rangeWitness, err := api.getBlockRangeWitness(ctx, api.db, start, end, useDebug, checkedMode)
	if err != nil {
		return nil, err
	}

	var oldAccInputHash common.Hash
	if batchNumber > 0 {
		oaih, err := api.getAccInputHash(ctx, hDb, batchNumber-1)
		if err != nil {
			return nil, err
		}
		oldAccInputHash = *oaih
	} else {
		oldAccInputHash = common.Hash{}
	}

	timestampLimit := lastBlock.Time()

	return &legacy_executor_verifier.RpcPayload{
		Witness:           hex.EncodeToHex(rangeWitness),
		Coinbase:          api.config.AddressSequencer.String(),
		OldAccInputHash:   oldAccInputHash.String(),
		TimestampLimit:    timestampLimit,
		ForcedBlockhashL1: "",
	}, nil
}

func (api *ZkEvmAPIImpl) GetLatestGlobalExitRoot(ctx context.Context) (common.Hash, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	_, ger, err := hermezDb.GetLatestUsedGer()
	if err != nil {
		return common.Hash{}, err
	}

	return ger, nil
}

func (api *ZkEvmAPIImpl) GetVersionHistory(ctx context.Context) (json.RawMessage, error) {
	// get values from the db
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	versions, err := hermezDb.GetVersionHistory()
	if err != nil {
		return nil, err
	}

	versionsJson, err := json.Marshal(versions)
	if err != nil {
		return nil, err
	}

	return versionsJson, nil
}

type l1InfoTreeData struct {
	Index           uint64      `json:"index"`
	Ger             common.Hash `json:"ger"`
	InfoRoot        common.Hash `json:"info_root"`
	MainnetExitRoot common.Hash `json:"mainnet_exit_root"`
	RollupExitRoot  common.Hash `json:"rollup_exit_root"`
	ParentHash      common.Hash `json:"parent_hash"`
	MinTimestamp    uint64      `json:"min_timestamp"`
	BlockNumber     uint64      `json:"block_number"`
}

func (api *ZkEvmAPIImpl) GetExitRootTable(ctx context.Context) ([]l1InfoTreeData, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)

	indexToRoots, err := hermezDb.GetL1InfoTreeIndexToRoots()
	if err != nil {
		return nil, err
	}

	var result []l1InfoTreeData

	var idx uint64 = 1
	for {
		info, err := hermezDb.GetL1InfoTreeUpdate(idx)
		if err != nil {
			return nil, err
		}
		if info == nil || info.Index == 0 {
			break
		}
		data := l1InfoTreeData{
			Index:           info.Index,
			Ger:             info.GER,
			MainnetExitRoot: info.MainnetExitRoot,
			RollupExitRoot:  info.RollupExitRoot,
			ParentHash:      info.ParentHash,
			MinTimestamp:    info.Timestamp,
			BlockNumber:     info.BlockNumber,
			InfoRoot:        indexToRoots[info.Index],
		}
		result = append(result, data)
		idx++
	}

	return result, nil
}

func (api *ZkEvmAPIImpl) sendGetBatchWitness(rpcUrl string, batchNumber uint64, mode *WitnessMode) (json.RawMessage, error) {
	res, err := client.JSONRPCCall(rpcUrl, "zkevm_getBatchWitness", batchNumber, mode)
	if err != nil {
		return nil, err
	}

	return res.Result, nil
}

func getLastBlockInBatchNumber(tx kv.Tx, batchNumber uint64) (uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)

	if batchNumber == 0 {
		return 0, nil
	}

	blocks, err := reader.GetL2BlockNosByBatch(batchNumber)
	if err != nil {
		return 0, err
	}
	return blocks[len(blocks)-1], nil
}

func getAllBlocksInBatchNumber(tx kv.Tx, batchNumber uint64) ([]uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)
	return reader.GetL2BlockNosByBatch(batchNumber)
}

func getLatestBatchNumber(tx kv.Tx) (uint64, error) {
	c, err := tx.Cursor(hermez_db.BLOCKBATCHES)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	// get the last entry from the table
	k, v, err := c.Last()
	if err != nil {
		return 0, err
	}
	if k == nil {
		return 0, nil
	}

	return hermez_db.BytesToUint64(v), nil
}

func getBatchNoByL2Block(tx kv.Tx, l2BlockNo uint64) (uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)
	return reader.GetBatchNoByL2Block(l2BlockNo)
}

func getForkIdByBatchNo(tx kv.Tx, batchNo uint64) (uint64, error) {
	reader := hermez_db.NewHermezDbReader(tx)
	return reader.GetForkId(batchNo)
}

func getForkInterval(tx kv.Tx, forkId uint64) (*rpc.ForkInterval, error) {
	reader := hermez_db.NewHermezDbReader(tx)

	forkInterval, found, err := reader.GetForkInterval(forkId)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, nil
	}

	result := rpc.ForkInterval{
		ForkId:          hexutil.Uint64(forkInterval.ForkID),
		FromBatchNumber: hexutil.Uint64(forkInterval.FromBatchNumber),
		ToBatchNumber:   hexutil.Uint64(forkInterval.ToBatchNumber),
		Version:         "",
		BlockNumber:     hexutil.Uint64(forkInterval.BlockNumber),
	}

	return &result, nil
}

func getForkIntervals(tx kv.Tx) ([]rpc.ForkInterval, error) {
	reader := hermez_db.NewHermezDbReader(tx)

	forkIntervals, err := reader.GetAllForkIntervals()
	if err != nil {
		return nil, err
	}

	result := make([]rpc.ForkInterval, 0, len(forkIntervals))

	for _, forkInterval := range forkIntervals {
		result = append(result, rpc.ForkInterval{
			ForkId:          hexutil.Uint64(forkInterval.ForkID),
			FromBatchNumber: hexutil.Uint64(forkInterval.FromBatchNumber),
			ToBatchNumber:   hexutil.Uint64(forkInterval.ToBatchNumber),
			Version:         "",
			BlockNumber:     hexutil.Uint64(forkInterval.BlockNumber),
		})
	}

	return result, nil
}

func convertTransactionsReceipts(
	txs []eritypes.Transaction,
	receipts eritypes.Receipts,
	hermezReader hermez_db.HermezDbReader,
	block eritypes.Block) ([]types.Transaction, error) {
	if len(txs) != len(receipts) {
		return nil, errors.New("transactions and receipts length mismatch")
	}

	result := make([]types.Transaction, 0, len(txs))

	for idx, tx := range txs {
		effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(tx.Hash())
		if err != nil {
			return nil, err
		}
		gasPrice := tx.GetPrice()
		v, r, s := tx.RawSignatureValues()
		var sender common.Address

		// TODO: senders!

		var receipt *types.Receipt
		if len(receipts) > idx {
			receipt = convertReceipt(receipts[idx], sender, tx.GetTo(), gasPrice, effectiveGasPricePercentage)
		}

		bh := block.Hash()
		blockNumber := block.NumberU64()

		tran := types.Transaction{
			Nonce:       types.ArgUint64(tx.GetNonce()),
			GasPrice:    types.ArgBig(*gasPrice.ToBig()),
			Gas:         types.ArgUint64(tx.GetGas()),
			To:          tx.GetTo(),
			Value:       types.ArgBig(*tx.GetValue().ToBig()),
			Input:       tx.GetData(),
			V:           types.ArgBig(*v.ToBig()),
			R:           types.ArgBig(*r.ToBig()),
			S:           types.ArgBig(*s.ToBig()),
			Hash:        tx.Hash(),
			From:        sender,
			BlockHash:   &bh,
			BlockNumber: types.ArgUint64Ptr(types.ArgUint64(blockNumber)),
			TxIndex:     types.ArgUint64Ptr(types.ArgUint64(idx)),
			Type:        types.ArgUint64(tx.Type()),
			Receipt:     receipt,
		}

		cid := tx.GetChainID()
		var cidAB *types.ArgBig
		if cid.Cmp(uint256.NewInt(0)) != 0 {
			cidAB = (*types.ArgBig)(cid.ToBig())
			tran.ChainID = cidAB
		}

		result = append(result, tran)
	}

	return result, nil
}

func convertBlockToRpcBlock(
	orig *eritypes.Block,
	receipts eritypes.Receipts,
	senders []common.Address,
	effectiveGasPricePercentages []uint8,
	full bool,
) (types.Block, error) {
	header := orig.Header()

	var difficulty uint64
	if header.Difficulty != nil {
		difficulty = header.Difficulty.Uint64()
	} else {
		difficulty = uint64(0)
	}

	n := big.NewInt(0).SetUint64(header.Nonce.Uint64())
	nonce := types.LeftPadBytes(n.Bytes(), 8) //nolint:gomnd
	blockHash := orig.Hash()
	blockNumber := orig.NumberU64()

	result := types.Block{
		ParentHash:      header.ParentHash,
		Sha3Uncles:      sha3UncleHash,
		Miner:           header.Coinbase,
		StateRoot:       header.Root,
		TxRoot:          header.TxHash,
		ReceiptsRoot:    header.ReceiptHash,
		LogsBloom:       header.Bloom,
		Difficulty:      types.ArgUint64(difficulty),
		TotalDifficulty: types.ArgUint64(difficulty),
		Size:            types.ArgUint64(orig.Size()),
		Number:          types.ArgUint64(blockNumber),
		GasLimit:        types.ArgUint64(header.GasLimit),
		GasUsed:         types.ArgUint64(header.GasUsed),
		Timestamp:       types.ArgUint64(header.Time),
		ExtraData:       types.ArgBytes(header.Extra),
		MixHash:         header.MixDigest,
		Nonce:           nonce,
		Hash:            blockHash,
		Transactions:    []types.TransactionOrHash{},
		Uncles:          []common.Hash{},
	}

	if full {
		for idx, tx := range orig.Transactions() {
			gasPrice := tx.GetPrice()
			v, r, s := tx.RawSignatureValues()
			var sender common.Address
			if len(senders) > idx {
				sender = senders[idx]
			}
			var effectiveGasPricePercentage uint8 = 0
			if len(effectiveGasPricePercentages) > idx {
				effectiveGasPricePercentage = effectiveGasPricePercentages[idx]
			}
			var receipt *types.Receipt
			if len(receipts) > idx {
				receipt = convertReceipt(receipts[idx], sender, tx.GetTo(), gasPrice, effectiveGasPricePercentage)
			}

			tran := types.Transaction{
				Nonce:       types.ArgUint64(tx.GetNonce()),
				GasPrice:    types.ArgBig(*gasPrice.ToBig()),
				Gas:         types.ArgUint64(tx.GetGas()),
				To:          tx.GetTo(),
				Value:       types.ArgBig(*tx.GetValue().ToBig()),
				Input:       tx.GetData(),
				V:           types.ArgBig(*v.ToBig()),
				R:           types.ArgBig(*r.ToBig()),
				S:           types.ArgBig(*s.ToBig()),
				Hash:        tx.Hash(),
				From:        sender,
				BlockHash:   &blockHash,
				BlockNumber: types.ArgUint64Ptr(types.ArgUint64(blockNumber)),
				TxIndex:     types.ArgUint64Ptr(types.ArgUint64(idx)),
				Type:        types.ArgUint64(tx.Type()),
				Receipt:     receipt,
			}

			cid := tx.GetChainID()
			if cid == nil {
				cid = uint256.NewInt(0)
			}
			tran.ChainID = (*types.ArgBig)(cid.ToBig())

			t := types.TransactionOrHash{Tx: &tran}
			result.Transactions = append(result.Transactions, t)
		}
	} else {
		for _, tx := range orig.Transactions() {
			h := tx.Hash()
			th := types.TransactionOrHash{Hash: &h}
			result.Transactions = append(result.Transactions, th)
		}
	}

	return result, nil
}

func convertReceipt(
	r *eritypes.Receipt,
	from common.Address,
	to *common.Address,
	gasPrice *uint256.Int,
	effectiveGasPricePercentage uint8,
) *types.Receipt {
	var cAddr *common.Address
	if r.ContractAddress != (common.Address{}) {
		cAddr = &r.ContractAddress
	}

	// ensure logs is always an empty array rather than nil in the response
	logs := make([]*eritypes.Log, 0)
	if len(r.Logs) > 0 {
		logs = r.Logs
	}

	var effectiveGasPrice *types.ArgBig
	if gasPrice != nil {
		gas := core.CalculateEffectiveGas(gasPrice.Clone(), effectiveGasPricePercentage)
		asBig := types.ArgBig(*gas.ToBig())
		effectiveGasPrice = &asBig
	}

	return &types.Receipt{
		CumulativeGasUsed: types.ArgUint64(r.CumulativeGasUsed),
		LogsBloom:         eritypes.CreateBloom(eritypes.Receipts{r}),
		Logs:              logs,
		Status:            types.ArgUint64(r.Status),
		TxHash:            r.TxHash,
		TxIndex:           types.ArgUint64(r.TransactionIndex),
		BlockHash:         r.BlockHash,
		BlockNumber:       types.ArgUint64(r.BlockNumber.Uint64()),
		GasUsed:           types.ArgUint64(r.GasUsed),
		FromAddr:          from,
		ToAddr:            to,
		ContractAddress:   cAddr,
		Type:              types.ArgUint64(r.Type),
		EffectiveGasPrice: effectiveGasPrice,
	}
}

func populateBatchDetails(batch *types.Batch) (json.RawMessage, error) {
	jBatch := map[string]interface{}{}
	jBatch["number"] = batch.Number
	jBatch["coinbase"] = batch.Coinbase
	jBatch["stateRoot"] = batch.StateRoot
	jBatch["timestamp"] = batch.Timestamp
	jBatch["blocks"] = batch.Blocks
	jBatch["transactions"] = batch.Transactions
	jBatch["globalExitRoot"] = batch.GlobalExitRoot
	jBatch["mainnetExitRoot"] = batch.MainnetExitRoot
	jBatch["rollupExitRoot"] = batch.RollupExitRoot
	jBatch["localExitRoot"] = batch.LocalExitRoot
	jBatch["sendSequencesTxHash"] = batch.SendSequencesTxHash
	jBatch["verifyBatchTxHash"] = batch.VerifyBatchTxHash
	jBatch["accInputHash"] = batch.AccInputHash

	if batch.ForcedBatchNumber != nil {
		jBatch["forcedBatchNumber"] = batch.ForcedBatchNumber
	}
	jBatch["closed"] = batch.Closed
	jBatch["batchL2Data"] = batch.BatchL2Data

	return json.Marshal(jBatch)
}

func populateBatchDataSlimDetails(batches []*types.BatchDataSlim) (json.RawMessage, error) {
	jBatches := make([]map[string]interface{}, 0, len(batches))
	for _, b := range batches {
		jBatch := map[string]interface{}{}
		jBatch["number"] = b.Number.Hex()
		jBatch["empty"] = b.Empty
		if !b.Empty {
			jBatch["batchL2Data"] = b.BatchL2Data
		}
		jBatches = append(jBatches, jBatch)
	}

	data := map[string]interface{}{
		"data": jBatches,
	}

	return json.Marshal(data)
}

// GetProof
func (zkapi *ZkEvmAPIImpl) GetProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*accounts.SMTAccProofResult, error) {
	api := zkapi.ethApi

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if api.historyV3(tx) {
		return nil, fmt.Errorf("not supported by Erigon3")
	}

	blockNr, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return nil, err
	}

	latestBlock, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return nil, err
	}

	if latestBlock < blockNr {
		// shouldn't happen, but check anyway
		return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, blockNr)
	}

	batch := memdb.NewMemoryBatch(tx, api.dirs.Tmp)
	defer batch.Rollback()
	if err = zkUtils.PopulateMemoryMutationTables(batch); err != nil {
		return nil, err
	}

	if blockNr < latestBlock {
		if latestBlock-blockNr > maxGetProofRewindBlockCount {
			return nil, fmt.Errorf("requested block is too old, block must be within %d blocks of the head block number (currently %d)", maxGetProofRewindBlockCount, latestBlock)
		}
		unwindState := &stagedsync.UnwindState{UnwindPoint: blockNr}
		stageState := &stagedsync.StageState{BlockNumber: latestBlock}

		interHashStageCfg := zkStages.StageZkInterHashesCfg(nil, true, true, false, api.dirs.Tmp, api._blockReader, nil, api.historyV3(tx), api._agg, nil)

		if err = zkStages.UnwindZkIntermediateHashesStage(unwindState, stageState, batch, interHashStageCfg, ctx, true); err != nil {
			return nil, fmt.Errorf("unwind intermediate hashes: %w", err)
		}

		if err != nil {
			return nil, err
		}
		tx = batch
	}

	reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(tx), "")
	if err != nil {
		return nil, err
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNr)
	if err != nil {
		return nil, err
	}

	tds := state.NewTrieDbState(header.Root, tx, blockNr, nil)
	tds.SetResolveReads(true)
	tds.StartNewBuffer()
	tds.SetStateReader(reader)

	ibs := state.New(tds)

	ibs.GetBalance(address)

	for _, key := range storageKeys {
		value := new(uint256.Int)
		ibs.GetState(address, &key, value)
	}

	rl, err := tds.ResolveSMTRetainList()
	if err != nil {
		return nil, err
	}

	smtTrie := smt.NewRoSMT(smtDb.NewRoEriDb(tx))

	proofs, err := smt.BuildProofs(smtTrie, rl, ctx)
	if err != nil {
		return nil, err
	}

	stateRootNode := smtUtils.ScalarToRoot(new(big.Int).SetBytes(header.Root.Bytes()))

	if err != nil {
		return nil, err
	}

	balanceKey := smtUtils.KeyEthAddrBalance(address.String())
	nonceKey := smtUtils.KeyEthAddrNonce(address.String())
	codeHashKey := smtUtils.KeyContractCode(address.String())
	codeLengthKey := smtUtils.KeyContractLength(address.String())

	balanceProofs := smt.FilterProofs(proofs, balanceKey)
	balanceBytes, err := smt.VerifyAndGetVal(stateRootNode, balanceProofs, balanceKey)
	if err != nil {
		return nil, fmt.Errorf("balance proof verification failed: %w", err)
	}

	balance := new(big.Int).SetBytes(balanceBytes)

	nonceProofs := smt.FilterProofs(proofs, nonceKey)
	nonceBytes, err := smt.VerifyAndGetVal(stateRootNode, nonceProofs, nonceKey)
	if err != nil {
		return nil, fmt.Errorf("nonce proof verification failed: %w", err)
	}
	nonce := new(big.Int).SetBytes(nonceBytes).Uint64()

	codeHashProofs := smt.FilterProofs(proofs, codeHashKey)
	codeHashBytes, err := smt.VerifyAndGetVal(stateRootNode, codeHashProofs, codeHashKey)
	if err != nil {
		return nil, fmt.Errorf("code hash proof verification failed: %w", err)
	}
	codeHash := codeHashBytes

	codeLengthProofs := smt.FilterProofs(proofs, codeLengthKey)
	codeLengthBytes, err := smt.VerifyAndGetVal(stateRootNode, codeLengthProofs, codeLengthKey)
	if err != nil {
		return nil, fmt.Errorf("code length proof verification failed: %w", err)
	}
	codeLength := new(big.Int).SetBytes(codeLengthBytes).Uint64()

	accProof := &accounts.SMTAccProofResult{
		Address:         address,
		Balance:         (*hexutil.Big)(balance),
		CodeHash:        libcommon.BytesToHash(codeHash),
		CodeLength:      hexutil.Uint64(codeLength),
		Nonce:           hexutil.Uint64(nonce),
		BalanceProof:    balanceProofs,
		NonceProof:      nonceProofs,
		CodeHashProof:   codeHashProofs,
		CodeLengthProof: codeLengthProofs,
		StorageProof:    make([]accounts.SMTStorageProofResult, 0),
	}

	addressArrayBig := smtUtils.ScalarToArrayBig(smtUtils.ConvertHexToBigInt(address.String()))
	for _, k := range storageKeys {
		storageKey := smtUtils.KeyContractStorage(addressArrayBig, k.String())
		storageProofs := smt.FilterProofs(proofs, storageKey)

		valueBytes, err := smt.VerifyAndGetVal(stateRootNode, storageProofs, storageKey)
		if err != nil {
			return nil, fmt.Errorf("storage proof verification failed: %w", err)
		}

		value := new(big.Int).SetBytes(valueBytes)

		accProof.StorageProof = append(accProof.StorageProof, accounts.SMTStorageProofResult{
			Key:   k,
			Value: (*hexutil.Big)(value),
			Proof: storageProofs,
		})
	}

	return accProof, nil
}

// ForkId returns the network's current fork ID
func (api *ZkEvmAPIImpl) GetForkId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	currentBatchNumber, err := getLatestBatchNumber(tx)
	if err != nil {
		return 0, err
	}

	currentForkId, err := getForkIdByBatchNo(tx, currentBatchNumber)
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(currentForkId), err
}

// GetForkById returns the network fork interval given the provided fork id
func (api *ZkEvmAPIImpl) GetForkById(ctx context.Context, forkId hexutil.Uint64) (res json.RawMessage, err error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	forkInterval, err := getForkInterval(tx, uint64(forkId))
	if err != nil {
		return nil, err
	}

	if forkInterval == nil {
		return nil, nil
	}

	forkJson, err := json.Marshal(forkInterval)
	if err != nil {
		return nil, err
	}

	return forkJson, err
}

// GetForkIdByBatchNumber returns the fork ID given the provided batch number
func (api *ZkEvmAPIImpl) GetForkIdByBatchNumber(ctx context.Context, batchNumber rpc.BlockNumber) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return hexutil.Uint64(0), err
	}
	defer tx.Rollback()

	currentForkId, err := getForkIdByBatchNo(tx, uint64(batchNumber))
	if err != nil {
		return 0, err
	}

	return hexutil.Uint64(currentForkId), err
}

// GetForks returns the network fork intervals
func (api *ZkEvmAPIImpl) GetForks(ctx context.Context) (res json.RawMessage, err error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	forkIntervals, err := getForkIntervals(tx)
	if err != nil {
		return nil, err
	}

	forksJson, err := json.Marshal(forkIntervals)
	if err != nil {
		return nil, err
	}

	return forksJson, err
}
