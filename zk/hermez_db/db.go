package hermez_db

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	dstypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/types"
)

const L1VERIFICATIONS = "hermez_l1Verifications"                   // l1blockno, batchno -> l1txhash
const L1SEQUENCES = "hermez_l1Sequences"                           // l1blockno, batchno -> l1txhash
const FORKIDS = "hermez_forkIds"                                   // batchNo -> forkId
const BLOCKBATCHES = "hermez_blockBatches"                         // l2blockno -> batchno
const GLOBAL_EXIT_ROOTS = "hermez_globalExitRoots"                 // l2blockno -> GER
const GLOBAL_EXIT_ROOTS_BATCHES = "hermez_globalExitRoots_batches" // l2blockno -> GER
const TX_PRICE_PERCENTAGE = "hermez_txPricePercentage"             // txHash -> txPricePercentage
const STATE_ROOTS = "hermez_stateRoots"                            // l2blockno -> stateRoot

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

func NewHermezDb(tx kv.RwTx) (*HermezDb, error) {
	db := &HermezDb{tx: tx}
	db.HermezDbReader = NewHermezDbReader(tx)

	return db, nil
}

func CreateHermezBuckets(tx kv.RwTx) error {
	err := tx.CreateBucket(L1VERIFICATIONS)
	if err != nil {
		return err
	}
	err = tx.CreateBucket(L1SEQUENCES)
	if err != nil {
		return err
	}
	err = tx.CreateBucket(FORKIDS)
	if err != nil {
		return err
	}
	err = tx.CreateBucket(BLOCKBATCHES)
	if err != nil {
		return err
	}
	err = tx.CreateBucket(GLOBAL_EXIT_ROOTS)
	if err != nil {
		return err
	}
	err = tx.CreateBucket(GLOBAL_EXIT_ROOTS_BATCHES)
	if err != nil {
		return err
	}
	err = tx.CreateBucket(TX_PRICE_PERCENTAGE)
	if err != nil {
		return err
	}
	err = tx.CreateBucket(STATE_ROOTS)
	if err != nil {
		return err
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
		return 0, nil
	}

	if BytesToUint64(k) != l2BlockNo {
		return 0, nil
	}

	return BytesToUint64(v), nil
}

func (db *HermezDbReader) GetL2BlockNosByBatch(batchNo uint64) ([]uint64, error) {
	// TODO: not the most efficient way of doing this
	c, err := db.tx.Cursor(BLOCKBATCHES)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var blockNos []uint64
	var k, v []byte

	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			break
		}
		if BytesToUint64(v) == batchNo {
			blockNos = append(blockNos, BytesToUint64(k))
		}
	}

	return blockNos, err
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

func (db *HermezDbReader) GetHighestBlockInBatch(batchNo uint64) (uint64, error) {
	blocks, err := db.GetL2BlockNosByBatch(batchNo)
	if err != nil {
		return 0, err
	}

	max := uint64(0)
	for _, block := range blocks {
		if block > max {
			max = block
		}
	}

	return max, nil
}

func (db *HermezDbReader) GetHighestVerifiedBlockNo() (uint64, error) {
	v, err := db.GetLatestVerification()
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, nil
	}

	blockNo, err := db.GetHighestBlockInBatch(v.BatchNo)
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

	return db.GetVerificationByBatchNo(batchNo)
}

func (db *HermezDbReader) GetSequenceByL1Block(l1BlockNo uint64) (*types.L1BatchInfo, error) {
	return db.getByL1Block(L1SEQUENCES, l1BlockNo)
}

func (db *HermezDbReader) GetSequenceByBatchNo(batchNo uint64) (*types.L1BatchInfo, error) {
	return db.getByBatchNo(L1SEQUENCES, batchNo)
}

func (db *HermezDbReader) GetVerificationByL1Block(l1BlockNo uint64) (*types.L1BatchInfo, error) {
	return db.getByL1Block(L1VERIFICATIONS, l1BlockNo)
}

func (db *HermezDbReader) GetVerificationByBatchNo(batchNo uint64) (*types.L1BatchInfo, error) {
	return db.getByBatchNo(L1VERIFICATIONS, batchNo)
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
			if len(v) != 64 {
				return nil, fmt.Errorf("invalid hash length")
			}

			l1TxHash := common.BytesToHash(v[:32])
			stateRoot := common.BytesToHash(v[32:])

			return &types.L1BatchInfo{
				BatchNo:   batchNo,
				L1BlockNo: l1Block,
				StateRoot: stateRoot,
				L1TxHash:  l1TxHash,
			}, nil
		}
	}

	return nil, nil
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
			if len(v) != 64 {
				return nil, fmt.Errorf("invalid hash length")
			}

			l1TxHash := common.BytesToHash(v[:32])
			stateRoot := common.BytesToHash(v[32:])

			return &types.L1BatchInfo{
				BatchNo:   batchNo,
				L1BlockNo: l1Block,
				StateRoot: stateRoot,
				L1TxHash:  l1TxHash,
			}, nil
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

	k, v, err := c.Last()
	if err != nil {
		return nil, err
	}

	l1BlockNo, batchNo, err := SplitKey(k)
	if err != nil {
		return nil, err
	}

	if len(v) != 64 {
		return nil, fmt.Errorf("invalid hash length")
	}

	l1TxHash := common.BytesToHash(v[:32])
	stateRoot := common.BytesToHash(v[32:])

	return &types.L1BatchInfo{
		BatchNo:   batchNo,
		L1BlockNo: l1BlockNo,
		L1TxHash:  l1TxHash,
		StateRoot: stateRoot,
	}, nil
}

func (db *HermezDb) WriteSequence(l1BlockNo, batchNo uint64, l1TxHash common.Hash, stateRoot common.Hash) error {
	return db.tx.Put(L1SEQUENCES, ConcatKey(l1BlockNo, batchNo), append(l1TxHash.Bytes(), stateRoot.Bytes()...))
}

func (db *HermezDb) WriteVerification(l1BlockNo, batchNo uint64, l1TxHash common.Hash, stateRoot common.Hash) error {
	return db.tx.Put(L1VERIFICATIONS, ConcatKey(l1BlockNo, batchNo), append(l1TxHash.Bytes(), stateRoot.Bytes()...))
}

func (db *HermezDb) WriteBlockBatch(l2BlockNo, batchNo uint64) error {
	return db.tx.Put(BLOCKBATCHES, Uint64ToBytes(l2BlockNo), Uint64ToBytes(batchNo))
}

func (db *HermezDb) WriteBlockGlobalExitRoot(l2BlockNo uint64, ger common.Hash) error {
	return db.tx.Put(GLOBAL_EXIT_ROOTS, Uint64ToBytes(l2BlockNo), ger.Bytes())
}

func (db *HermezDbReader) GetBlockGlobalExitRoot(l2BlockNo uint64) (common.Hash, error) {
	data, err := db.tx.GetOne(GLOBAL_EXIT_ROOTS, Uint64ToBytes(l2BlockNo))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(data), nil
}

func (db *HermezDb) WriteBatchGBatchGlobalExitRoot(batchNumber uint64, ger dstypes.GerUpdate) error {
	return db.tx.Put(GLOBAL_EXIT_ROOTS_BATCHES, Uint64ToBytes(batchNumber), ger.EncodeToBytes())
}

func (db *HermezDbReader) GetBatchGlobalExitRoots(fromBatchNum, toBatchNum uint64) ([]*dstypes.GerUpdate, error) {
	c, err := db.tx.Cursor(GLOBAL_EXIT_ROOTS_BATCHES)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var gers []*dstypes.GerUpdate
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
			gers = append(gers, gerUpdate)
		}
	}

	return gers, err
}

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

func (db *HermezDb) DeleteBatchGlobalExitRoots(fromBatchNum, toBatchNum uint64) error {
	for i := fromBatchNum; i <= toBatchNum; i++ {
		err := db.tx.Delete(GLOBAL_EXIT_ROOTS_BATCHES, Uint64ToBytes(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) DeleteBlockGlobalExitRoots(fromBlockNum, toBlockNum uint64) error {
	for i := fromBlockNum; i <= toBlockNum; i++ {
		err := db.tx.Delete(GLOBAL_EXIT_ROOTS, Uint64ToBytes(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDb) DeleteBlockBatches(fromBatchNum, toBatchNum uint64) error {
	for i := fromBatchNum; i <= toBatchNum; i++ {
		err := db.tx.Delete(FORKIDS, Uint64ToBytes(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *HermezDbReader) GetForkId(batchNo uint64) (uint64, error) {
	c, err := db.tx.Cursor(FORKIDS)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	var forkId uint64 = 0
	var k, v []byte

	for k, v, err = c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			break
		}
		currentBatchNo := BytesToUint64(k)
		if currentBatchNo <= batchNo {
			forkId = BytesToUint64(v)
		} else {
			break
		}
	}

	return forkId, err
}

func (db *HermezDb) WriteForkId(batchNo, forkId uint64) error {
	return db.tx.Put(FORKIDS, Uint64ToBytes(batchNo), Uint64ToBytes(forkId))
}

func (db *HermezDb) DeleteForkIds(fromBatchNum, toBatchNum uint64) error {
	for i := fromBatchNum; i <= toBatchNum; i++ {
		err := db.tx.Delete(FORKIDS, Uint64ToBytes(i))
		if err != nil {
			return err
		}
	}

	return nil
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
