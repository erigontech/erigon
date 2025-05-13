package hermez_db

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"

	"encoding/json"

	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	dstypes "github.com/erigontech/erigon/zk/datastream/types"
	"github.com/erigontech/erigon/zk/types"
)

const L1VERIFICATIONS = "hermez_l1Verifications"                        // l1blockno, batchno -> l1txhash
const L1SEQUENCES = "hermez_l1Sequences"                                // l1blockno, batchno -> l1txhash
const FORKIDS = "hermez_forkIds"                                        // batchNo -> forkId
const FORKID_BLOCK = "hermez_forkIdBlock"                               // forkId -> startBlock
const BLOCKBATCHES = "hermez_blockBatches"                              // l2blockno -> batchno
const GLOBAL_EXIT_ROOTS = "hermez_globalExitRootsSaved"                 // GER -> true
const BLOCK_GLOBAL_EXIT_ROOTS = "hermez_globalExitRoots"                // l2blockno -> GER
const GLOBAL_EXIT_ROOTS_BATCHES = "hermez_globalExitRoots_batches"      // batchkno -> GER
const TX_PRICE_PERCENTAGE = "hermez_txPricePercentage"                  // txHash -> txPricePercentage
const STATE_ROOTS = "hermez_stateRoots"                                 // l2blockno -> stateRoot
const L1_INFO_TREE_UPDATES = "l1_info_tree_updates"                     // index -> L1InfoTreeUpdate
const L1_INFO_TREE_UPDATES_BY_GER = "l1_info_tree_updates_by_ger"       // GER -> L1InfoTreeUpdate
const BLOCK_L1_INFO_TREE_INDEX = "block_l1_info_tree_index"             // block number -> l1 info tree index
const BLOCK_L1_INFO_TREE_INDEX_PROGRESS = "block_l1_info_tree_progress" // block number -> l1 info tree progress
const L1_INJECTED_BATCHES = "l1_injected_batches"                       // index increasing by 1 -> injected batch for the start of the chain
const BLOCK_INFO_ROOTS = "block_info_roots"                             // block number -> block info root hash
const BLOCK_L1_BLOCK_HASHES = "block_l1_block_hashes"                   // block number -> l1 block hash
const INTERMEDIATE_TX_STATEROOTS = "hermez_intermediate_tx_stateRoots"  // l2blockno -> stateRoot
const BATCH_WITNESSES = "hermez_batch_witnesses"                        // batch number -> witness
const BATCH_COUNTERS = "hermez_batch_counters"                          // block number -> counters
const L1_BATCH_DATA = "l1_batch_data"                                   // batch number -> l1 batch data from transaction call data
const REUSED_L1_INFO_TREE_INDEX = "reused_l1_info_tree_index"           // block number => const 1
const LATEST_USED_GER = "latest_used_ger"                               // batch number -> GER latest used GER
const BATCH_BLOCKS = "batch_blocks"                                     // batch number -> block numbers (concatenated together)
const SMT_DEPTHS = "smt_depths"                                         // block number -> smt depth
const L1_INFO_LEAVES = "l1_info_leaves"                                 // l1 info tree index -> l1 info tree leaf
const L1_INFO_ROOTS = "l1_info_roots"                                   // root hash -> l1 info tree index
const INVALID_BATCHES = "invalid_batches"                               // batch number -> true
const ROllUP_TYPES_FORKS = "rollup_types_forks"                         // rollup type id -> fork id
const FORK_HISTORY = "fork_history"                                     // index -> fork id + last verified batch
const JUST_UNWOUND = "just_unwound"                                     // batch number -> true
const PLAIN_STATE_VERSION = "plain_state_version"                       // batch number -> true
const ERIGON_VERSIONS = "erigon_versions"                               // erigon version -> timestamp of startup
const BATCH_ENDS = "batch_ends"                                         // batch number -> true
const WITNESS_CACHE = "witness_cache"                                   // block number -> witness for 1 block
const BAD_TX_HASHES = "bad_tx_hashes"                                   // tx hash -> integer counter
const BAD_TX_HASHES_LOOKUP = "bad_tx_hashes_lookup"                     // timestamp -> tx hash
const CONFIRMED_L1_INFO_TREE_UPDATE = "confirmed_l1_info_tree_update"   // 1 - > confirmed l1 info tree index information (fork 12 only)

var HermezDbTables = []string{
	L1VERIFICATIONS,
	L1SEQUENCES,
	FORKIDS,
	FORKID_BLOCK,
	BLOCKBATCHES,
	GLOBAL_EXIT_ROOTS,
	BLOCK_GLOBAL_EXIT_ROOTS,
	GLOBAL_EXIT_ROOTS_BATCHES,
	TX_PRICE_PERCENTAGE,
	STATE_ROOTS,
	L1_INFO_TREE_UPDATES,
	L1_INFO_TREE_UPDATES_BY_GER,
	BLOCK_L1_INFO_TREE_INDEX,
	BLOCK_L1_INFO_TREE_INDEX_PROGRESS,
	L1_INJECTED_BATCHES,
	BLOCK_INFO_ROOTS,
	BLOCK_L1_BLOCK_HASHES,
	INTERMEDIATE_TX_STATEROOTS,
	BATCH_WITNESSES,
	BATCH_COUNTERS,
	L1_BATCH_DATA,
	REUSED_L1_INFO_TREE_INDEX,
	LATEST_USED_GER,
	BATCH_BLOCKS,
	SMT_DEPTHS,
	L1_INFO_LEAVES,
	L1_INFO_ROOTS,
	INVALID_BATCHES,
	ROllUP_TYPES_FORKS,
	FORK_HISTORY,
	JUST_UNWOUND,
	PLAIN_STATE_VERSION,
	ERIGON_VERSIONS,
	BATCH_ENDS,
	BAD_TX_HASHES,
	BAD_TX_HASHES_LOOKUP,
	WITNESS_CACHE,
	CONFIRMED_L1_INFO_TREE_UPDATE,
}

type HermezDb struct {
	tx kv.RwTx
	*HermezDbReader
}

// HermezDbReader represents a reader for the HermezDb database.  It has no write functions and is embedded into the
// HermezDb type for read operations.
type HermezDbReader struct {
	tx kv.Tx
}

func NewHermezDbReader(tx kv.Tx) *HermezDbReader {
	return &HermezDbReader{tx}
}

func NewHermezDb(tx kv.RwTx) *HermezDb {
	db := &HermezDb{tx: tx}
	db.HermezDbReader = NewHermezDbReader(tx)

	return db
}

func (db *HermezDb) SetNewTx(tx kv.RwTx) {
	db.tx = tx
	db.HermezDbReader.tx = tx
}

func CreateHermezBuckets(tx kv.RwTx) error {
	for _, t := range HermezDbTables {
		if err := tx.CreateBucket(t); err != nil {
			return err
		}
	}
	return nil
}

func (db *HermezDbReader) GetBatchNoByL2Block(l2BlockNo uint64) (uint64, error) {
	c, err := db.tx.Cursor(BLOCKBATCHES)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	k, v, err := c.Seek(Uint64ToBytes(l2BlockNo))
	if err != nil {
		return 0, err
	}

	if k == nil {
		return 0, ErrorNotStored
	}

	if BytesToUint64(k) != l2BlockNo {
		return 0, nil
	}

	return BytesToUint64(v), nil
}

func (db *HermezDbReader) CheckBatchNoByL2Block(l2BlockNo uint64) (uint64, bool, error) {
	c, err := db.tx.Cursor(BLOCKBATCHES)
	if err != nil {
		return 0, false, err
	}
	defer c.Close()

	k, v, err := c.Seek(Uint64ToBytes(l2BlockNo))
	if err != nil {
		return 0, false, err
	}
	if k == nil {
		return 0, false, nil
	}
	if BytesToUint64(k) != l2BlockNo {
		return 0, false, nil
	}
	return BytesToUint64(v), true, nil
}

func (db *HermezDbReader) GetL2BlockNosByBatch(batchNo uint64) ([]uint64, error) {
	v, err := db.tx.GetOne(BATCH_BLOCKS, Uint64ToBytes(batchNo))
	if err != nil {
		return nil, err
	}

	blocks := parseConcatenatedBlockNumbers(v)

	return blocks, nil
}

func concatenateBlockNumbers(blocks []uint64) []byte {
	v := make([]byte, len(blocks)*8)
	for i, block := range blocks {
		copy(v[i*8:(i+1)*8], Uint64ToBytes(block))
	}
	return v
}

func parseConcatenatedBlockNumbers(v []byte) []uint64 {
	count := len(v) / 8
	blocks := make([]uint64, count)
	for i := 0; i < count; i++ {
		blocks[i] = BytesToUint64(v[i*8 : (i+1)*8])
	}
	return blocks
}

func (db *HermezDbReader) GetLatestDownloadedBatchNo() (uint64, error) {
	c, err := db.tx.Cursor(BLOCKBATCHES)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	_, v, err := c.Last()
	if err != nil {
		return 0, err
	}
	return BytesToUint64(v), nil
}

// returns 0 and true for batch 0 (custom case) even thou no block in the db for taht batch
// returns 0 and false if no blocks found in the DB for that batch
func (db *HermezDbReader) GetHighestBlockInBatch(batchNo uint64) (uint64, bool, error) {
	// custom case for batch 0
	if batchNo == 0 {
		return 0, true, nil
	}
	blocks, err := db.GetL2BlockNosByBatch(batchNo)
	if err != nil {
		return 0, false, err
	}

	if len(blocks) == 0 {
		return 0, false, nil
	}

	max := uint64(0)
	for _, block := range blocks {
		if block > max {
			max = block
		}
	}

	return max, true, nil
}

// returns 0 and true for batch 0 (custom case) even thou no block in the db for taht batch
// returns 0 and false if no blocks found in the DB for that batch
func (db *HermezDbReader) GetLowestBlockInBatch(batchNo uint64) (blockNo uint64, found bool, err error) {
	// custom case for batch 0
	if batchNo == 0 {
		return 0, true, nil
	}

	blocks, err := db.GetL2BlockNosByBatch(batchNo)
	if err != nil {
		return 0, false, err
	}

	if len(blocks) == 0 {
		return 0, false, nil
	}

	min := uint64(0)
	for _, block := range blocks {
		if block < min || min == 0 {
			min = block
		}
	}

	return min, true, nil
}

func (db *HermezDbReader) GetHighestVerifiedBlockNo() (uint64, error) {
	v, err := db.GetLatestVerification()
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, nil
	}

	blockNo, _, err := db.GetHighestBlockInBatch(v.BatchNo)
	if err != nil {
		return 0, err
	}

	return blockNo, nil
}

func (db *HermezDbReader) GetVerificationByL2BlockNo(blockNo uint64) (*types.L1BatchInfo, error) {
	batchNo, err := db.GetBatchNoByL2Block(blockNo)
	if err != nil {
		return nil, err
	}
	log.Debug(fmt.Sprintf("[HermezDbReader] GetVerificationByL2BlockNo: blockNo %d, batchNo %d", blockNo, batchNo))

	return db.GetVerificationByBatchNo(batchNo)
}

func (db *HermezDbReader) GetSequenceByL1Block(l1BlockNo uint64) (*types.L1BatchInfo, error) {
	return db.getByL1Block(L1SEQUENCES, l1BlockNo)
}

func (db *HermezDbReader) GetSequenceByBatchNo(batchNo uint64) (*types.L1BatchInfo, error) {
	return db.getByBatchNo(L1SEQUENCES, batchNo)
}

func (db *HermezDbReader) GetRangeSequencesByBatch(batchNo uint64) (*types.L1BatchInfo, *types.L1BatchInfo, error) {
	return db.getPrevAndCurrentForBatch(L1SEQUENCES, batchNo)
}

func (db *HermezDbReader) GetSequenceByBatchNoOrHighest(batchNo uint64) (*types.L1BatchInfo, error) {
	seq, err := db.GetSequenceByBatchNo(batchNo)
	if err != nil {
		return nil, err
	}

	if seq != nil {
		return seq, nil
	}

	// start a cursor at the current batch no and then call .next to find the next highest sequence
	c, err := db.tx.Cursor(L1SEQUENCES)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var k, v []byte
	for k, v, err = c.Seek(Uint64ToBytes(batchNo)); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		l1Block, batch, err := SplitKey(k)
		if err != nil {
			return nil, err
		}

		if batch > batchNo {
			return parseL1BatchInfo(l1Block, batch, v)
		}
	}

	return nil, nil
}
func parseL1BatchInfo(l1BlockN, batchN uint64, v []byte) (*types.L1BatchInfo, error) {
	if len(v) != 96 && len(v) != 64 {
		return nil, fmt.Errorf("invalid hash length")
	}

	l1TxHash := common.BytesToHash(v[:32])
	stateRoot := common.BytesToHash(v[32:64])
	l1InfoRoot := common.BytesToHash(v[64:])

	return &types.L1BatchInfo{
		BatchNo:    batchN,
		L1BlockNo:  l1BlockN,
		StateRoot:  stateRoot,
		L1TxHash:   l1TxHash,
		L1InfoRoot: l1InfoRoot,
	}, nil
}
func (db *HermezDbReader) GetVerificationByL1Block(l1BlockNo uint64) (*types.L1BatchInfo, error) {
	return db.getByL1Block(L1VERIFICATIONS, l1BlockNo)
}

func (db *HermezDbReader) GetVerificationByBatchNo(batchNo uint64) (*types.L1BatchInfo, error) {
	return db.getByBatchNo(L1VERIFICATIONS, batchNo)
}

func (db *HermezDbReader) GetVerificationByBatchNoOrHighest(batchNo uint64) (*types.L1BatchInfo, error) {
	batchInfo, err := db.GetVerificationByBatchNo(batchNo)
	if err != nil {
		return nil, err
	}

	if batchInfo != nil {
		return batchInfo, nil
	}

	// start a cursor at the current batch no and then call .next to find the next highest verification
	c, err := db.tx.Cursor(L1VERIFICATIONS)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var k, v []byte
	for k, v, err = c.Seek(Uint64ToBytes(batchNo)); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		l1Block, batch, err := SplitKey(k)
		if err != nil {
			return nil, err
		}

		if batch > batchNo {
			if len(v) != 96 && len(v) != 64 {
				return nil, fmt.Errorf("invalid hash length")
			}

			l1TxHash := common.BytesToHash(v[:32])
			stateRoot := common.BytesToHash(v[32:64])
			var l1InfoRoot common.Hash
			if len(v) > 64 {
				l1InfoRoot = common.BytesToHash(v[64:])
			}

			return &types.L1BatchInfo{
				BatchNo:    batch,
				L1BlockNo:  l1Block,
				StateRoot:  stateRoot,
				L1TxHash:   l1TxHash,
				L1InfoRoot: l1InfoRoot,
			}, nil
		}
	}

	return nil, nil
}

func (db *HermezDbReader) getByL1Block(table string, l1BlockNo uint64) (*types.L1BatchInfo, error) {
	c, err := db.tx.Cursor(table)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var k, v []byte
	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		l1Block, batchNo, err := SplitKey(k)
		if err != nil {
			return nil, err
		}

		if l1Block == l1BlockNo {
			return parseL1BatchInfo(l1Block, batchNo, v)
		}
	}

	return nil, nil
}

func (db *HermezDbReader) getPrevAndCurrentForBatch(table string, batchNo uint64) (prev *types.L1BatchInfo, current *types.L1BatchInfo, err error) {
	c, err := db.tx.Cursor(table)
	if err != nil {
		return
	}
	defer c.Close()

	var k, v []byte
	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return
		}

		l1Block, batch, err1 := SplitKey(k)
		if err1 != nil {
			err = err1
			return
		}

		// found the current one
		if batch >= batchNo {
			current, err = parseL1BatchInfo(l1Block, batch, v)
			if err != nil {
				return
			}
			break
		}
	}

	k, v, err = c.Prev()
	if err != nil {
		return
	}
	if len(v) == 0 {
		prev = &types.L1BatchInfo{}
		return
	}

	l1Block, prevBatch, err := SplitKey(k)
	if err != nil {
		return
	}

	prev, err = parseL1BatchInfo(l1Block, prevBatch, v)
	if err != nil {
		return
	}

	return
}

func (db *HermezDbReader) getByBatchNo(table string, batchNo uint64) (*types.L1BatchInfo, error) {
	c, err := db.tx.Cursor(table)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var k, v []byte
	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		l1Block, batch, err := SplitKey(k)
		if err != nil {
			return nil, err
		}

		if batch == batchNo {
			return parseL1BatchInfo(l1Block, batch, v)
		}
	}

	return nil, nil
}

func (db *HermezDbReader) GetLatestSequence() (*types.L1BatchInfo, error) {
	return db.getLatest(L1SEQUENCES)
}

func (db *HermezDbReader) GetLatestVerification() (*types.L1BatchInfo, error) {
	return db.getLatest(L1VERIFICATIONS)
}

func (db *HermezDbReader) getLatest(table string) (*types.L1BatchInfo, error) {
	c, err := db.tx.Cursor(table)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var l1BlockNo, batchNo uint64
	var value []byte
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		tmpL1BlockNo, tmpBatchNo, err := SplitKey(k)
		if err != nil {
			return nil, err
		}

		if tmpBatchNo > batchNo {
			l1BlockNo = tmpL1BlockNo
			batchNo = tmpBatchNo
			value = v
		}
	}

	if len(value) == 0 {
		return nil, nil
	}
	return parseL1BatchInfo(l1BlockNo, batchNo, value)
}

func (db *HermezDb) WriteSequence(l1BlockNo, batchNo uint64, l1TxHash, stateRoot, l1InfoRoot common.Hash) error {
	val := make([]byte, 0, 96)
	val = append(val, l1TxHash.Bytes()...)
	val = append(val, stateRoot.Bytes()...)
	val = append(val, l1InfoRoot.Bytes()...)
	return db.tx.Put(L1SEQUENCES, ConcatKey(l1BlockNo, batchNo), val)
}

// RollbackSequences deletes the sequences up to the given batch number
func (db *HermezDb) RollbackSequences(batchNo uint64) error {
	for {
		latestSequence, err := db.GetLatestSequence()
		if err != nil {
			return err
		}

		if latestSequence == nil || latestSequence.BatchNo <= batchNo {
			break
		}

		if err = db.tx.Delete(L1SEQUENCES, ConcatKey(latestSequence.L1BlockNo, latestSequence.BatchNo)); err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) TruncateSequences(l2BlockNo uint64) error {
	batchNo, err := db.GetBatchNoByL2Block(l2BlockNo)
	if err != nil && !errors.Is(err, ErrorNotStored) {
		return err
	}
	if batchNo == 0 {
		return nil
	}

	latestSeq, err := db.GetLatestSequence()
	if err != nil {
		return err
	}

	if latestSeq == nil {
		return nil
	}

	if latestSeq.BatchNo <= batchNo {
		return nil
	}

	for i := latestSeq.BatchNo; i > batchNo; i-- {
		seq, err := db.GetSequenceByBatchNo(i)
		if err != nil {
			return err
		}
		if seq == nil {
			continue
		}
		// delete seq
		if err = db.tx.Delete(L1SEQUENCES, ConcatKey(seq.L1BlockNo, seq.BatchNo)); err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteVerification(l1BlockNo, batchNo uint64, l1TxHash common.Hash, stateRoot common.Hash) error {
	return db.tx.Put(L1VERIFICATIONS, ConcatKey(l1BlockNo, batchNo), append(l1TxHash.Bytes(), stateRoot.Bytes()...))
}

func (db *HermezDb) TruncateVerifications(l2BlockNo uint64) error {
	batchNo, err := db.GetBatchNoByL2Block(l2BlockNo)
	if err != nil && !errors.Is(err, ErrorNotStored) {
		return err
	}
	if batchNo == 0 {
		return nil
	}

	latestSeq, err := db.GetLatestVerification()
	if err != nil {
		return err
	}

	if latestSeq == nil {
		return nil
	}

	if latestSeq.BatchNo <= batchNo {
		return nil
	}

	for i := latestSeq.BatchNo; i > batchNo; i-- {
		ver, err := db.GetVerificationByBatchNo(i)
		if err != nil {
			return err
		}
		if ver == nil {
			continue
		}
		// delete seq
		err = db.tx.Delete(L1VERIFICATIONS, ConcatKey(ver.L1BlockNo, ver.BatchNo))
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteBlockBatch(l2BlockNo, batchNo uint64) error {
	// first store the block -> batch record
	err := db.tx.Put(BLOCKBATCHES, Uint64ToBytes(l2BlockNo), Uint64ToBytes(batchNo))
	if err != nil {
		return err
	}

	// now write the batch -> block record
	v, err := db.tx.GetOne(BATCH_BLOCKS, Uint64ToBytes(batchNo))
	if err != nil {
		return err
	}

	// parse out the block numbers we already have
	blocks := parseConcatenatedBlockNumbers(v)

	// now check that we don't already have this block number stored
	for _, b := range blocks {
		if b == l2BlockNo {
			return nil
		}
	}

	// otherwise append it and store it
	v = append(v, Uint64ToBytes(l2BlockNo)...)
	return db.tx.Put(BATCH_BLOCKS, Uint64ToBytes(batchNo), v)
}

func (db *HermezDb) WriteGlobalExitRoot(ger common.Hash) error {
	return db.tx.Put(GLOBAL_EXIT_ROOTS, ger.Bytes(), []byte{1})
}

func (db *HermezDbReader) CheckGlobalExitRootWritten(ger common.Hash) (bool, error) {
	bytes, err := db.tx.GetOne(GLOBAL_EXIT_ROOTS, ger.Bytes())
	if err != nil {
		return false, err
	}
	return len(bytes) > 0, nil
}

func (db *HermezDb) DeleteGlobalExitRoots(gers *[]common.Hash) error {
	for _, ger := range *gers {
		err := db.tx.Delete(GLOBAL_EXIT_ROOTS, ger.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteReusedL1InfoTreeIndex(blockNo uint64) error {
	return db.tx.Put(REUSED_L1_INFO_TREE_INDEX, Uint64ToBytes(blockNo), []byte{1})
}

func (db *HermezDbReader) GetReusedL1InfoTreeIndex(blockNo uint64) (bool, error) {
	bytes, err := db.tx.GetOne(REUSED_L1_INFO_TREE_INDEX, Uint64ToBytes(blockNo))
	if err != nil {
		return false, err
	}
	return len(bytes) > 0, nil
}

func (db *HermezDb) DeleteReusedL1InfoTreeIndexes(fromBlock, toBlock uint64) error {
	for i := fromBlock; i <= toBlock; i++ {
		err := db.tx.Delete(REUSED_L1_INFO_TREE_INDEX, Uint64ToBytes(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteBlockGlobalExitRoot(l2BlockNo uint64, ger common.Hash) error {
	return db.tx.Put(BLOCK_GLOBAL_EXIT_ROOTS, Uint64ToBytes(l2BlockNo), ger.Bytes())
}

func (db *HermezDbReader) GetLastBlockGlobalExitRoot(l2BlockNo uint64) (common.Hash, uint64, error) {
	c, err := db.tx.Cursor(BLOCK_GLOBAL_EXIT_ROOTS)
	if err != nil {
		return common.Hash{}, 0, err
	}
	defer c.Close()

	var ger common.Hash
	var k, v []byte
	var currentBlockNumber, lastBlockNumber uint64
	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			break
		}
		currentBlockNumber = BytesToUint64(k)
		if currentBlockNumber > l2BlockNo {
			break
		}

		if len(v) > 0 && currentBlockNumber > lastBlockNumber && currentBlockNumber <= l2BlockNo {
			ger = common.BytesToHash(v)
			lastBlockNumber = currentBlockNumber
		}
	}

	return ger, lastBlockNumber, err
}

func (db *HermezDbReader) GetBlockGlobalExitRoot(l2BlockNo uint64) (common.Hash, error) {
	bytes, err := db.tx.GetOne(BLOCK_GLOBAL_EXIT_ROOTS, Uint64ToBytes(l2BlockNo))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(bytes), nil
}

// from and to are inclusive
func (db *HermezDbReader) GetBlockGlobalExitRoots(fromBlockNo, toBlockNo uint64) ([]common.Hash, error) {
	c, err := db.tx.Cursor(BLOCK_GLOBAL_EXIT_ROOTS)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var gers []common.Hash

	var k, v []byte

	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		CurrentBlockNumber := BytesToUint64(k)
		if CurrentBlockNumber >= fromBlockNo && CurrentBlockNumber <= toBlockNo {
			h := common.BytesToHash(v)
			gers = append(gers, h)
		}
	}

	return gers, nil
}

func (db *HermezDb) WriteBlockL1BlockHash(l2BlockNo uint64, l1BlockHash common.Hash) error {
	return db.tx.Put(BLOCK_L1_BLOCK_HASHES, Uint64ToBytes(l2BlockNo), l1BlockHash.Bytes())
}

func (db *HermezDbReader) GetBlockL1BlockHash(l2BlockNo uint64) (common.Hash, error) {
	bytes, err := db.tx.GetOne(BLOCK_L1_BLOCK_HASHES, Uint64ToBytes(l2BlockNo))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(bytes), nil
}

// from and to are inclusive
func (db *HermezDbReader) GetBlockL1BlockHashes(fromBlockNo, toBlockNo uint64) ([]common.Hash, error) {
	c, err := db.tx.Cursor(BLOCK_L1_BLOCK_HASHES)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var l1BlockHashes []common.Hash

	var k, v []byte

	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		CurrentBlockNumber := BytesToUint64(k)
		if CurrentBlockNumber >= fromBlockNo && CurrentBlockNumber <= toBlockNo {
			h := common.BytesToHash(v)
			l1BlockHashes = append(l1BlockHashes, h)
		}
	}

	return l1BlockHashes, nil
}

func (db *HermezDb) WriteBatchGlobalExitRoot(batchNumber uint64, ger *dstypes.GerUpdate) error {
	return db.tx.Put(GLOBAL_EXIT_ROOTS_BATCHES, Uint64ToBytes(batchNumber), ger.EncodeToBytes())
}

// deprecated: post etrog this will not work
func (db *HermezDbReader) GetBatchGlobalExitRoots(fromBatchNum, toBatchNum uint64) (*[]dstypes.GerUpdate, error) {
	c, err := db.tx.Cursor(GLOBAL_EXIT_ROOTS_BATCHES)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var gers []dstypes.GerUpdate
	var k, v []byte

	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			break
		}
		currentBatchNo := BytesToUint64(k)
		if currentBatchNo >= fromBatchNum && currentBatchNo <= toBatchNum {
			gerUpdate, err := dstypes.DecodeGerUpdate(v)
			if err != nil {
				return nil, err
			}
			gers = append(gers, *gerUpdate)
		}
	}

	return &gers, err
}

// GetLastBatchGlobalExitRoot deprecated: post etrog this will not work
func (db *HermezDbReader) GetLastBatchGlobalExitRoot(batchNum uint64) (*dstypes.GerUpdate, uint64, error) {
	c, err := db.tx.Cursor(GLOBAL_EXIT_ROOTS_BATCHES)
	if err != nil {
		return nil, 0, err
	}
	defer c.Close()

	var ger *dstypes.GerUpdate
	var k, v []byte
	var lastWrittenbatcNo, currentBatchNo uint64
	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			break
		}
		currentBatchNo := BytesToUint64(k)
		if len(v) > 0 && currentBatchNo > lastWrittenbatcNo && currentBatchNo <= batchNum {
			ger, err = dstypes.DecodeGerUpdate(v)
			if err != nil {
				return nil, 0, err
			}
			lastWrittenbatcNo = currentBatchNo
			if currentBatchNo == batchNum {
				continue
			}
		}
	}

	return ger, currentBatchNo, err
}

func (db *HermezDbReader) GetBatchGlobalExitRootsProto(fromBatchNum, toBatchNum uint64) ([]dstypes.GerUpdateProto, error) {
	gers, err := db.GetBatchGlobalExitRoots(fromBatchNum, toBatchNum)
	if err != nil {
		return nil, err
	}

	var gersProto []dstypes.GerUpdateProto
	for _, ger := range *gers {
		proto := dstypes.ConvertGerUpdateToProto(ger)
		gersProto = append(gersProto, proto)
	}

	return gersProto, nil
}

// GetBatchGlobalExitRoot deprecated: post etrog this will not work
func (db *HermezDbReader) GetBatchGlobalExitRoot(batchNum uint64) (*dstypes.GerUpdate, error) {
	gerUpdateBytes, err := db.tx.GetOne(GLOBAL_EXIT_ROOTS_BATCHES, Uint64ToBytes(batchNum))
	if err != nil {
		return nil, err
	}
	if len(gerUpdateBytes) == 0 {
		// no ger update for this batch
		return nil, nil
	}
	gerUpdate, err := dstypes.DecodeGerUpdate(gerUpdateBytes)
	if err != nil {
		return nil, err
	}
	return gerUpdate, nil
}

func (db *HermezDb) DeleteBatchGlobalExitRoots(fromBatchNum uint64) error {
	c, err := db.tx.Cursor(GLOBAL_EXIT_ROOTS_BATCHES)
	if err != nil {
		return err
	}
	defer c.Close()

	k, _, err := c.Last()
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	lastBatchNum := BytesToUint64(k)
	return db.deleteFromBucketWithUintKeysRange(GLOBAL_EXIT_ROOTS_BATCHES, fromBatchNum, lastBatchNum)
}

func (db *HermezDb) DeleteBlockGlobalExitRoots(fromBlockNum, toBlockNum uint64) error {
	return db.deleteFromBucketWithUintKeysRange(BLOCK_GLOBAL_EXIT_ROOTS, fromBlockNum, toBlockNum)
}

func (db *HermezDb) DeleteBlockL1BlockHashes(fromBlockNum, toBlockNum uint64) error {
	return db.deleteFromBucketWithUintKeysRange(BLOCK_L1_BLOCK_HASHES, fromBlockNum, toBlockNum)
}

func (db *HermezDb) DeleteBlockL1InfoTreeIndexes(fromBlockNum, toBlockNum uint64) error {
	return db.deleteFromBucketWithUintKeysRange(BLOCK_L1_INFO_TREE_INDEX, fromBlockNum, toBlockNum)
}

// from and to are inclusive
func (db *HermezDb) DeleteBlockBatches(fromBlockNum, toBlockNum uint64) error {
	// first, gather batch numbers related to the blocks we're about to delete
	batchNumbersMap := map[uint64]struct{}{}

	// find all the batches involved
	for i := fromBlockNum; i <= toBlockNum; i++ {
		batch, err := db.GetBatchNoByL2Block(i)
		if err != nil && !errors.Is(err, ErrorNotStored) {
			return err
		}
		batchNumbersMap[batch] = struct{}{}
	}

	// now for each batch go and get the block numbers and remove them from the batch to block records
	for batchNumber := range batchNumbersMap {
		data, err := db.tx.GetOne(BATCH_BLOCKS, Uint64ToBytes(batchNumber))
		if err != nil {
			return err
		}
		blockNos := parseConcatenatedBlockNumbers(data)

		// make a new list excluding the blocks in our range
		newBlockNos := make([]uint64, 0, len(blockNos))
		for _, blockNo := range blockNos {
			if blockNo < fromBlockNum || blockNo > toBlockNum {
				newBlockNos = append(newBlockNos, blockNo)
			}
		}

		// concatenate the block numbers back again
		newData := concatenateBlockNumbers(newBlockNos)

		// now delete/store it back
		if len(newData) == 0 {
			err = db.tx.Delete(BATCH_BLOCKS, Uint64ToBytes(batchNumber))
		} else {
			err = db.tx.Put(BATCH_BLOCKS, Uint64ToBytes(batchNumber), newData)
		}
		if err != nil {
			return err
		}
	}

	return db.deleteFromBucketWithUintKeysRange(BLOCKBATCHES, fromBlockNum, toBlockNum)
}

func (db *HermezDb) deleteFromBucketWithUintKeysRange(bucket string, fromBlockNum, toBlockNum uint64) error {
	for i := fromBlockNum; i <= toBlockNum; i++ {
		err := db.tx.Delete(bucket, Uint64ToBytes(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDbReader) GetForkId(batchNo uint64) (uint64, error) {
	if batchNo == 0 {
		batchNo = 1
	}
	v, err := db.tx.GetOne(FORKIDS, Uint64ToBytes(batchNo))
	if err != nil {
		return 0, err
	}
	return BytesToUint64(v), nil
}

func (db *HermezDb) WriteForkId(batchNo, forkId uint64) error {
	return db.tx.Put(FORKIDS, Uint64ToBytes(batchNo), Uint64ToBytes(forkId))
}

func (db *HermezDbReader) GetLowestBatchByFork(forkId uint64) (uint64, error) {
	forkIdBlock, err := db.tx.GetOne(FORKID_BLOCK, Uint64ToBytes(forkId))
	if err != nil {
		return 0, err
	}

	batchNo, err := db.tx.GetOne(BLOCKBATCHES, forkIdBlock)
	if err != nil {
		return 0, err
	}

	return BytesToUint64(batchNo), err

}

func (db *HermezDbReader) GetForkIdBlock(forkId uint64) (uint64, bool, error) {
	blkNum, err := db.tx.GetOne(FORKID_BLOCK, Uint64ToBytes(forkId))
	if err != nil {
		return 0, false, err
	}
	if blkNum == nil {
		return 0, false, nil
	}

	return BytesToUint64(blkNum), true, nil
}

func (db *HermezDbReader) GetAllForkBlocks() (map[uint64]uint64, error) {
	c, err := db.tx.Cursor(FORKID_BLOCK)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	forkBlocks := make(map[uint64]uint64)
	var k, v []byte

	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			break
		}
		currentForkId := BytesToUint64(k)
		blockNum := BytesToUint64(v)

		forkBlocks[currentForkId] = blockNum
	}

	return forkBlocks, err
}

func (db *HermezDb) DeleteForkIdBlock(fromBlockNo, toBlockNo uint64) error {
	return db.deleteFromBucketWithUintKeysRange(FORKID_BLOCK, fromBlockNo, toBlockNo)
}

func (db *HermezDb) WriteForkIdBlockOnce(forkId, blockNum uint64) error {
	tempBlockNum, found, err := db.GetForkIdBlock(forkId)
	if err != nil {
		log.Error(fmt.Sprintf("[HermezDb] Error getting forkIdBlock: %v", err))
		return err
	}
	if found {
		log.Debug(fmt.Sprintf("[HermezDb] Fork id block already exists: %d, block:%v, set db failed.", forkId, tempBlockNum))
		return nil
	}
	return db.tx.Put(FORKID_BLOCK, Uint64ToBytes(forkId), Uint64ToBytes(blockNum))
}

func (db *HermezDb) DeleteForkIds(fromBatchNum, toBatchNum uint64) error {
	return db.deleteFromBucketWithUintKeysRange(FORKIDS, fromBatchNum, toBatchNum)
}

func (db *HermezDb) WriteEffectiveGasPricePercentage(txHash common.Hash, txPricePercentage uint8) error {
	return db.tx.Put(TX_PRICE_PERCENTAGE, txHash.Bytes(), Uint8ToBytes(txPricePercentage))
}

func (db *HermezDbReader) GetEffectiveGasPricePercentage(txHash common.Hash) (uint8, error) {
	data, err := db.tx.GetOne(TX_PRICE_PERCENTAGE, txHash.Bytes())
	if err != nil {
		return 0, err
	}

	return BytesToUint8(data), nil
}

func (db *HermezDb) DeleteEffectiveGasPricePercentages(txHashes *[]common.Hash) error {
	for _, txHash := range *txHashes {
		err := db.tx.Delete(TX_PRICE_PERCENTAGE, txHash.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteStateRoot(l2BlockNo uint64, rpcRoot common.Hash) error {
	return db.tx.Put(STATE_ROOTS, Uint64ToBytes(l2BlockNo), rpcRoot.Bytes())
}

func (db *HermezDbReader) GetStateRoot(l2BlockNo uint64) (common.Hash, error) {
	data, err := db.tx.GetOne(STATE_ROOTS, Uint64ToBytes(l2BlockNo))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(data), nil
}

func (db *HermezDb) DeleteStateRoots(fromBlockNo, toBlockNo uint64) error {
	return db.deleteFromBucketWithUintKeysRange(STATE_ROOTS, fromBlockNo, toBlockNo)
}
func (db *HermezDb) WriteIntermediateTxStateRoot(l2BlockNo uint64, txHash common.Hash, rpcRoot common.Hash) error {
	numberBytes := Uint64ToBytes(l2BlockNo)
	key := append(numberBytes, txHash.Bytes()...)

	return db.tx.Put(INTERMEDIATE_TX_STATEROOTS, key, rpcRoot.Bytes())
}

func (db *HermezDbReader) GetIntermediateTxStateRoot(l2BlockNo uint64, txHash common.Hash) (common.Hash, error) {
	numberBytes := Uint64ToBytes(l2BlockNo)
	key := append(numberBytes, txHash.Bytes()...)
	data, err := db.tx.GetOne(INTERMEDIATE_TX_STATEROOTS, key)
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(data), nil
}

func (db *HermezDb) DeleteIntermediateTxStateRoots(fromBlockNo, toBlockNo uint64) error {
	c, err := db.tx.Cursor(INTERMEDIATE_TX_STATEROOTS)
	if err != nil {
		return err
	}
	defer c.Close()

	var k []byte
	for k, _, err = c.First(); k != nil; k, _, err = c.Next() {
		if err != nil {
			break
		}

		blockNum := BytesToUint64(k[:8])
		if blockNum >= fromBlockNo && blockNum <= toBlockNo {
			err := db.tx.Delete(INTERMEDIATE_TX_STATEROOTS, k)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *HermezDb) WriteL1InfoTreeUpdate(update *types.L1InfoTreeUpdate) error {
	marshalled := update.Marshall()
	idx := Uint64ToBytes(update.Index)
	return db.tx.Put(L1_INFO_TREE_UPDATES, idx, marshalled)
}

func (db *HermezDb) TruncateL1InfoTreeUpdates(fromIndex uint64) error {
	c, err := db.tx.Cursor(L1_INFO_TREE_UPDATES)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, _, err := c.Seek(Uint64ToBytes(fromIndex)); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}

		if err = db.tx.Delete(L1_INFO_TREE_UPDATES, k); err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteL1InfoTreeUpdateToGer(update *types.L1InfoTreeUpdate) error {
	marshalled := update.Marshall()
	return db.tx.Put(L1_INFO_TREE_UPDATES_BY_GER, update.GER.Bytes(), marshalled)
}

func (db *HermezDb) TruncateL1InfoTreeUpdatesByGer(fromIndex uint64) error {
	c, err := db.tx.Cursor(L1_INFO_TREE_UPDATES_BY_GER)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}

		update := &types.L1InfoTreeUpdate{}
		update.Unmarshall(v)

		if update.Index >= fromIndex {
			if err = db.tx.Delete(L1_INFO_TREE_UPDATES_BY_GER, k); err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *HermezDbReader) GetL1InfoTreeUpdateByGer(ger common.Hash) (*types.L1InfoTreeUpdate, error) {
	data, err := db.tx.GetOne(L1_INFO_TREE_UPDATES_BY_GER, ger.Bytes())
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	update := &types.L1InfoTreeUpdate{}
	update.Unmarshall(data)
	return update, nil
}

func (db *HermezDbReader) GetL1InfoTreeUpdate(idx uint64) (*types.L1InfoTreeUpdate, error) {
	data, err := db.tx.GetOne(L1_INFO_TREE_UPDATES, Uint64ToBytes(idx))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	update := &types.L1InfoTreeUpdate{}
	update.Unmarshall(data)
	return update, nil
}

func (db *HermezDbReader) GetLatestL1InfoTreeUpdate() (*types.L1InfoTreeUpdate, error) {
	cursor, err := db.tx.Cursor(L1_INFO_TREE_UPDATES)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	count, err := cursor.Count()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	_, v, err := cursor.Last()
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}

	result := &types.L1InfoTreeUpdate{}
	result.Unmarshall(v)
	return result, nil
}

func (db *HermezDb) WriteBlockL1InfoTreeIndex(blockNumber uint64, l1Index uint64) error {
	k := Uint64ToBytes(blockNumber)
	v := Uint64ToBytes(l1Index)
	return db.tx.Put(BLOCK_L1_INFO_TREE_INDEX, k, v)
}

func (db *HermezDbReader) GetBlockL1InfoTreeIndex(blockNumber uint64) (uint64, error) {
	v, err := db.tx.GetOne(BLOCK_L1_INFO_TREE_INDEX, Uint64ToBytes(blockNumber))
	if err != nil {
		return 0, err
	}
	return BytesToUint64(v), nil
}

// gets the previous saved index and block for that index
// uses current inex block as parameter
func (db *HermezDbReader) GetPreviousIndexBlock(currentIndexBlockNumber uint64) (blockNum uint64, index uint64, found bool, err error) {
	c, err := db.tx.Cursor(BLOCK_L1_INFO_TREE_INDEX)
	if err != nil {
		return
	}
	defer c.Close()

	k, _, err := c.SeekExact(Uint64ToBytes(currentIndexBlockNumber))
	if err != nil || k == nil {
		return
	}

	k, v, err := c.Prev()
	if err != nil || k == nil {
		return
	}

	blockNum = BytesToUint64(k)
	index = BytesToUint64(v)
	found = true

	return
}

func (db *HermezDb) WriteBlockL1InfoTreeIndexProgress(blockNumber uint64, l1Index uint64) error {
	latestBlockNumber, latestL1Index, err := db.GetLatestBlockL1InfoTreeIndexProgress()
	if err != nil {
		return err
	}
	if latestBlockNumber > blockNumber {
		return fmt.Errorf("unable to set l1index for block %d because it has already been set for block %d", blockNumber, latestBlockNumber)
	}
	if l1Index <= latestL1Index {
		return nil
	}

	k := Uint64ToBytes(blockNumber)
	v := Uint64ToBytes(l1Index)
	return db.tx.Put(BLOCK_L1_INFO_TREE_INDEX_PROGRESS, k, v)
}

func (db *HermezDbReader) GetLatestBlockL1InfoTreeIndexProgress() (uint64, uint64, error) {
	c, err := db.tx.Cursor(BLOCK_L1_INFO_TREE_INDEX_PROGRESS)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()

	k, v, err := c.Last()
	if err != nil {
		return 0, 0, err
	}
	return BytesToUint64(k), BytesToUint64(v), nil
}

func (db *HermezDb) DeleteBlockL1InfoTreeIndexesProgress(fromBlockNum, toBlockNum uint64) error {
	return db.deleteFromBucketWithUintKeysRange(BLOCK_L1_INFO_TREE_INDEX_PROGRESS, fromBlockNum, toBlockNum)
}

func (db *HermezDb) WriteL1InjectedBatch(batch *types.L1InjectedBatch) error {
	var nextIndex uint64 = 0

	// get the next index for the write
	cursor, err := db.tx.Cursor(L1_INJECTED_BATCHES)
	if err != nil {
		return err
	}
	defer cursor.Close()

	count, err := cursor.Count()
	if err != nil {
		return err
	}

	if count > 0 {
		nextIndex = count + 1
	}

	k := Uint64ToBytes(nextIndex)
	v := batch.Marshall()
	return db.tx.Put(L1_INJECTED_BATCHES, k, v)
}

func (db *HermezDbReader) GetL1InjectedBatch(index uint64) (*types.L1InjectedBatch, error) {
	k := Uint64ToBytes(index)
	v, err := db.tx.GetOne(L1_INJECTED_BATCHES, k)
	if err != nil {
		return nil, err
	}
	ib := new(types.L1InjectedBatch)
	err = ib.Unmarshall(v)
	if err != nil {
		return nil, err
	}
	return ib, nil
}

func (db *HermezDb) WriteBlockInfoRoot(blockNumber uint64, root common.Hash) error {
	k := Uint64ToBytes(blockNumber)
	return db.tx.Put(BLOCK_INFO_ROOTS, k, root.Bytes())
}

func (db *HermezDbReader) GetBlockInfoRoot(blockNumber uint64) (common.Hash, error) {
	k := Uint64ToBytes(blockNumber)
	data, err := db.tx.GetOne(BLOCK_INFO_ROOTS, k)
	if err != nil {
		return common.Hash{}, err
	}
	res := common.BytesToHash(data)
	return res, nil
}

func (db *HermezDb) DeleteBlockInfoRoots(fromBlock, toBlock uint64) error {
	return db.deleteFromBucketWithUintKeysRange(BLOCK_INFO_ROOTS, fromBlock, toBlock)
}

func (db *HermezDb) WriteWitness(batchNumber uint64, witness []byte) error {
	return db.tx.Put(BATCH_WITNESSES, Uint64ToBytes(batchNumber), witness)
}

func (db *HermezDbReader) GetWitness(batchNumber uint64) ([]byte, error) {
	v, err := db.tx.GetOne(BATCH_WITNESSES, Uint64ToBytes(batchNumber))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (db *HermezDb) WriteBatchCounters(blockNumber uint64, counters []int) error {
	countersJson, err := json.Marshal(counters)
	if err != nil {
		return err
	}
	return db.tx.Put(BATCH_COUNTERS, Uint64ToBytes(blockNumber), countersJson)
}

func (db *HermezDbReader) GetLatestBatchCounters(batchNumber uint64) (countersArray []int, found bool, err error) {
	batchBlockNumbers, err := db.GetL2BlockNosByBatch(batchNumber)
	if err != nil {
		return nil, false, err
	}

	v, err := db.tx.GetOne(BATCH_COUNTERS, Uint64ToBytes(batchBlockNumbers[len(batchBlockNumbers)-1]))
	if err != nil {
		return nil, false, err
	}
	found = len(v) > 0

	if found {
		if err = json.Unmarshal(v, &countersArray); err != nil {
			return nil, false, err
		}
	}

	return countersArray, found, nil
}

func (db *HermezDb) DeleteBatchCounters(fromBlockNum, toBlockNum uint64) error {
	return db.deleteFromBucketWithUintKeysRange(BATCH_COUNTERS, fromBlockNum, toBlockNum)
}

// WriteL1BatchData stores the data for a given L1 batch number
// coinbase = 20 bytes
// batchL2Data = remaining
func (db *HermezDb) WriteL1BatchData(batchNumber uint64, data []byte) error {
	k := Uint64ToBytes(batchNumber)
	return db.tx.Put(L1_BATCH_DATA, k, data)
}

// GetL1BatchData returns the data stored for a given L1 batch number
// coinbase = 20 bytes
// batchL2Data = remaining
func (db *HermezDbReader) GetL1BatchData(batchNumber uint64) ([]byte, error) {
	k := Uint64ToBytes(batchNumber)
	return db.tx.GetOne(L1_BATCH_DATA, k)
}

func (db *HermezDbReader) GetLastL1BatchData() (uint64, error) {
	c, err := db.tx.Cursor(L1_BATCH_DATA)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	k, _, err := c.Last()
	if err != nil {
		return 0, err
	}

	return BytesToUint64(k), nil
}

func (db *HermezDb) WriteLatestUsedGer(blockNumber uint64, ger common.Hash) error {
	return db.tx.Put(LATEST_USED_GER, Uint64ToBytes(blockNumber), ger.Bytes())
}

func (db *HermezDbReader) GetLatestUsedGer() (uint64, common.Hash, error) {
	c, err := db.tx.Cursor(LATEST_USED_GER)
	if err != nil {
		return 0, common.Hash{}, err
	}
	defer c.Close()

	k, v, err := c.Last()
	if err != nil {
		return 0, common.Hash{}, err
	}

	batchNo := BytesToUint64(k)
	ger := common.BytesToHash(v)

	return batchNo, ger, nil
}

func (db *HermezDb) DeleteLatestUsedGers(fromBlockNum, toBlockNum uint64) error {
	return db.deleteFromBucketWithUintKeysRange(LATEST_USED_GER, fromBlockNum, toBlockNum)
}

func (db *HermezDb) WriteSmtDepth(l2BlockNo, depth uint64) error {
	return db.tx.Put(SMT_DEPTHS, Uint64ToBytes(l2BlockNo), Uint64ToBytes(depth))
}

// get the closest to the given block smt depth
func (db *HermezDbReader) GetClosestSmtDepth(l2BlockNo uint64) (closestBlock uint64, depth uint64, err error) {
	c, err := db.tx.Cursor(SMT_DEPTHS)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()

	var k, v []byte
	var currentRange, currentBlock uint64
	closestRange := uint64(math.MaxUint64)
	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return 0, 0, err
		}

		currentBlock = BytesToUint64(k)

		if currentBlock > l2BlockNo {
			currentRange = currentBlock - l2BlockNo
		} else {
			currentRange = l2BlockNo - currentBlock
		}

		if currentRange < closestRange {
			closestBlock = currentBlock
			depth = BytesToUint64(v)

			if closestBlock > l2BlockNo {
				closestRange = closestBlock - l2BlockNo
			} else {
				closestRange = l2BlockNo - closestBlock
			}
		}
	}

	return closestBlock, depth, nil
}

// truncate smt depths from the given block onwards
func (db *HermezDb) TruncateSmtDepths(fromBlock uint64) error {
	c, err := db.tx.Cursor(SMT_DEPTHS)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, _, err := c.Seek(Uint64ToBytes(fromBlock + 1)); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}

		err := db.tx.Delete(SMT_DEPTHS, k)
		if err != nil {
			return err
		}

	}

	return nil
}

func (db *HermezDb) WriteL1InfoTreeLeaf(l1Index uint64, leaf common.Hash) error {
	return db.tx.Put(L1_INFO_LEAVES, Uint64ToBytes(l1Index), leaf.Bytes())
}

func (db *HermezDb) TruncateL1InfoTreeLeaves(fromIndex uint64) error {
	c, err := db.tx.Cursor(L1_INFO_LEAVES)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, _, err := c.Seek(Uint64ToBytes(fromIndex)); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}

		if err = db.tx.Delete(L1_INFO_LEAVES, k); err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDbReader) GetAllL1InfoTreeLeaves() ([]common.Hash, error) {
	c, err := db.tx.Cursor(L1_INFO_LEAVES)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var leaves []common.Hash
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		leaves = append(leaves, common.BytesToHash(v))
	}

	return leaves, nil
}

func (db *HermezDb) WriteL1InfoTreeRoot(hash common.Hash, index uint64) error {
	return db.tx.Put(L1_INFO_ROOTS, hash.Bytes(), Uint64ToBytes(index))
}

func (db *HermezDb) TruncateL1InfoTreeRoots(fromIndex uint64) error {
	c, err := db.tx.Cursor(L1_INFO_ROOTS)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		index := BytesToUint64(v)
		if index >= fromIndex {
			if err = db.tx.Delete(L1_INFO_ROOTS, k); err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *HermezDb) GetL1InfoTreeIndexByRoot(hash common.Hash) (uint64, bool, error) {
	data, err := db.tx.GetOne(L1_INFO_ROOTS, hash.Bytes())
	if err != nil {
		return 0, false, err
	}
	return BytesToUint64(data), data != nil, nil
}

func (db *HermezDbReader) GetL1InfoTreeIndexToRoots() (map[uint64]common.Hash, error) {
	c, err := db.tx.Cursor(L1_INFO_ROOTS)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	indexToRoot := make(map[uint64]common.Hash)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		index := BytesToUint64(v)
		root := common.BytesToHash(k)
		indexToRoot[index] = root
	}

	return indexToRoot, nil
}

func (db *HermezDbReader) GetForkIdByBlockNum(blockNum uint64) (uint64, error) {
	blockbatch, err := db.GetBatchNoByL2Block(blockNum)
	if err != nil {
		return 0, err
	}

	forkId, err := db.GetForkId(blockbatch)
	if err != nil {
		return 0, err
	}
	if forkId == 0 {
		return 0, errors.New("the network cannot have a 0 fork id")
	}

	return forkId, nil
}

func (db *HermezDb) WriteInvalidBatch(batchNo uint64) error {
	return db.tx.Put(INVALID_BATCHES, Uint64ToBytes(batchNo), []byte{1})
}

func (db *HermezDbReader) GetInvalidBatch(batchNo uint64) (bool, error) {
	v, err := db.tx.GetOne(INVALID_BATCHES, Uint64ToBytes(batchNo))
	if err != nil {
		return false, err
	}
	return len(v) > 0, nil
}

func (db *HermezDb) WriteRollupType(rollupType, forkId uint64) error {
	return db.tx.Put(ROllUP_TYPES_FORKS, Uint64ToBytes(rollupType), Uint64ToBytes(forkId))
}

func (db *HermezDbReader) GetForkFromRollupType(rollupType uint64) (uint64, error) {
	v, err := db.tx.GetOne(ROllUP_TYPES_FORKS, Uint64ToBytes(rollupType))
	if err != nil {
		return 0, err
	}
	return BytesToUint64(v), nil
}

func (db *HermezDb) WriteNewForkHistory(forkId, lastVerifiedBatch uint64) error {
	cursor, err := db.tx.Cursor(FORK_HISTORY)
	if err != nil {
		return err
	}
	defer cursor.Close()
	lastIndex, _, err := cursor.Last()
	if err != nil {
		return err
	}
	nextIndex := BytesToUint64(lastIndex) + 1
	k := Uint64ToBytes(nextIndex)
	forkBytes := Uint64ToBytes(forkId)
	batchBytes := Uint64ToBytes(lastVerifiedBatch)
	v := append(forkBytes, batchBytes...)
	return db.tx.Put(FORK_HISTORY, k, v)
}

func (db *HermezDbReader) GetLatestForkHistory() (uint64, uint64, error) {
	cursor, err := db.tx.Cursor(FORK_HISTORY)
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close()
	_, v, err := cursor.Last()
	if err != nil {
		return 0, 0, err
	}
	if len(v) == 0 {
		return 0, 0, nil
	}
	forkId := BytesToUint64(v[:8])
	lastVerifiedBatch := BytesToUint64(v[8:])

	return forkId, lastVerifiedBatch, nil
}

func (db *HermezDbReader) GetAllForkHistory() ([]uint64, []uint64, error) {
	var forks, batches []uint64
	cursor, err := db.tx.Cursor(FORK_HISTORY)
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close()
	for k, v, err := cursor.First(); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			return nil, nil, err
		}
		forkId := BytesToUint64(v[:8])
		lastVerifiedBatch := BytesToUint64(v[8:])
		forks = append(forks, forkId)
		batches = append(batches, lastVerifiedBatch)
	}

	return forks, batches, nil
}

func (db *HermezDbReader) GetVersionHistory() (map[string]time.Time, error) {
	c, err := db.tx.Cursor(ERIGON_VERSIONS)
	if err != nil {
		return nil, nil
	}
	defer c.Close()

	versions := make(map[string]time.Time)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		tsInt := BytesToUint64(v)
		versions[string(k)] = time.Unix(int64(tsInt), 0)
	}

	return versions, nil
}

// WriteErigonVersion adds the erigon version to the db, returning true if written, false if already exists
func (db *HermezDb) WriteErigonVersion(version string, timestamp time.Time) (bool, error) {
	// check if already exists
	v, err := db.tx.GetOne(ERIGON_VERSIONS, []byte(version))
	if err != nil {
		return false, err
	}
	if v != nil {
		return false, nil
	}

	// write new version
	return true, db.tx.Put(ERIGON_VERSIONS, []byte(version), Uint64ToBytes(uint64(timestamp.Unix())))
}

func (db *HermezDb) WriteBatchEnd(blockNo uint64) error {
	key := Uint64ToBytes(blockNo)
	return db.tx.Put(BATCH_ENDS, key, []byte{1})
}

func (db *HermezDbReader) GetBatchEnd(blockNo uint64) (bool, error) {
	v, err := db.tx.GetOne(BATCH_ENDS, Uint64ToBytes(blockNo))
	if err != nil {
		return false, err
	}
	return len(v) > 0, nil
}

func (db *HermezDbReader) GetLatestBatchEndBlock() (uint64, error) {
	c, err := db.tx.Cursor(BATCH_ENDS)
	if err != nil {
		return 0, nil
	}
	defer c.Close()

	k, _, err := c.Last()
	if err != nil {
		return 0, err
	}

	return BytesToUint64(k), nil
}

func (db *HermezDb) DeleteBatchEnds(from, to uint64) error {
	return db.deleteFromBucketWithUintKeysRange(BATCH_ENDS, from, to)
}

func (db *HermezDbReader) GetAllForkIntervals() ([]types.ForkInterval, error) {
	return db.getForkIntervals(nil)
}

func (db *HermezDbReader) GetForkInterval(forkID uint64) (*types.ForkInterval, bool, error) {
	forkIntervals, err := db.getForkIntervals(&forkID)
	if err != nil {
		return nil, false, err
	}

	if len(forkIntervals) == 0 {
		return nil, false, err
	}

	forkInterval := forkIntervals[0]
	return &forkInterval, true, nil
}

func (db *HermezDbReader) getForkIntervals(forkIdFilter *uint64) ([]types.ForkInterval, error) {
	mapForkIntervals := map[uint64]types.ForkInterval{}

	c, err := db.tx.Cursor(FORKIDS)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	lastForkId := uint64(0)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}

		batchNumber := BytesToUint64(k)
		forkID := BytesToUint64(v)

		if forkID > lastForkId {
			lastForkId = forkID
		}

		if forkIdFilter != nil && *forkIdFilter != forkID {
			continue
		}

		mapInterval, found := mapForkIntervals[forkID]
		if !found {
			mapInterval = types.ForkInterval{
				ForkID:          forkID,
				FromBatchNumber: batchNumber,
				ToBatchNumber:   batchNumber,
			}
		}

		if batchNumber < mapInterval.FromBatchNumber {
			mapInterval.FromBatchNumber = batchNumber
		}

		if batchNumber > mapInterval.ToBatchNumber {
			mapInterval.ToBatchNumber = batchNumber
		}

		mapForkIntervals[forkID] = mapInterval
	}

	forkIntervals := make([]types.ForkInterval, 0, len(mapForkIntervals))
	for forkId, forkInterval := range mapForkIntervals {
		blockNumber, found, err := db.GetForkIdBlock(forkInterval.ForkID)
		if err != nil {
			return nil, err
		} else if found {
			forkInterval.BlockNumber = blockNumber
		}

		if forkId == lastForkId {
			forkInterval.ToBatchNumber = math.MaxUint64
		}

		forkIntervals = append(forkIntervals, forkInterval)
	}

	sort.Slice(forkIntervals, func(i, j int) bool {
		return forkIntervals[i].FromBatchNumber < forkIntervals[j].FromBatchNumber
	})

	return forkIntervals, nil
}

func (db *HermezDb) PurgeBadTxHashes() error {
	if err := db.tx.ClearBucket(BAD_TX_HASHES); err != nil {
		return err
	}
	if err := db.tx.ClearBucket(BAD_TX_HASHES_LOOKUP); err != nil {
		return err
	}
	return nil
}

func (db *HermezDb) TruncateBadTxHashCounterBelow(below uint64) error {
	c, err := db.tx.Cursor(BAD_TX_HASHES_LOOKUP)
	if err != nil {
		return err
	}
	defer c.Close()

	var counter uint64 = 0
	for k, v, err := c.Last(); k != nil; k, v, err = c.Prev() {
		if err != nil {
			return err
		}
		counter++
		if counter <= below {
			continue
		}

		if err = db.tx.Delete(BAD_TX_HASHES, v); err != nil {
			return err
		}

		if err = db.tx.Delete(BAD_TX_HASHES_LOOKUP, k); err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteBadTxHashCounter(txHash common.Hash, counter uint64) error {
	if err := db.tx.Put(BAD_TX_HASHES, txHash.Bytes(), Uint64ToBytes(counter)); err != nil {
		return err
	}
	if err := db.tx.Put(BAD_TX_HASHES_LOOKUP, TimeToBytes(time.Now()), txHash.Bytes()); err != nil {
		return err
	}
	return nil
}

func (db *HermezDbReader) GetBadTxHashCounter(txHash common.Hash) (uint64, error) {
	v, err := db.tx.GetOne(BAD_TX_HASHES, txHash.Bytes())
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	return BytesToUint64(v), nil
}

func (db *HermezDb) WriteWitnessCache(blockNo uint64, witnessBytes []byte) error {
	key := Uint64ToBytes(blockNo)
	return db.tx.Put(WITNESS_CACHE, key, witnessBytes)
}

func (db *HermezDbReader) GetWitnessCache(batchNo uint64) ([]byte, error) {
	v, err := db.tx.GetOne(WITNESS_CACHE, Uint64ToBytes(batchNo))
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (db *HermezDb) DeleteWitnessCaches(from, to uint64) error {
	return db.deleteFromBucketWithUintKeysRange(WITNESS_CACHE, from, to)
}

func (db *HermezDb) PurgeWitnessCaches() error {
	return db.tx.ClearBucket(WITNESS_CACHE)
}

func (db *HermezDbReader) GetLatestCachedWitnessBatchNo() (uint64, error) {
	c, err := db.tx.Cursor(WITNESS_CACHE)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	k, _, err := c.Last()
	if err != nil {
		return 0, err
	}

	return BytesToUint64(k), nil
}

func (db *HermezDb) TruncateWitnessCacheBelow(below uint64) error {
	c, err := db.tx.Cursor(WITNESS_CACHE)
	if err != nil {
		return err
	}
	defer c.Close()

	for k, _, err := c.SeekExact(Uint64ToBytes(below - 1)); k != nil; k, _, err = c.Prev() {
		if err != nil {
			return err
		}

		if err = db.tx.Delete(WITNESS_CACHE, k); err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) WriteConfirmedL1InfoTreeUpdate(index, l1BlockNumber uint64) error {
	combinedBytes := append(Uint64ToBytes(index), Uint64ToBytes(l1BlockNumber)...)
	return db.tx.Put(CONFIRMED_L1_INFO_TREE_UPDATE, []byte{1}, combinedBytes)
}

func (db *HermezDbReader) GetConfirmedL1InfoTreeUpdate() (index, l1BlockNumber uint64, err error) {
	v, err := db.tx.GetOne(CONFIRMED_L1_INFO_TREE_UPDATE, []byte{1})
	if err != nil {
		return 0, 0, err
	}
	if len(v) == 0 {
		return 0, 0, nil
	}
	return BytesToUint64(v[:8]), BytesToUint64(v[8:]), nil
}
