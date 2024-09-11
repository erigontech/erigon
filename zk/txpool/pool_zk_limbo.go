package txpool

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcache"
	"github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/log/v3"
	"github.com/status-im/keycard-go/hexutils"
)

const (
	TablePoolLimbo                   = "PoolLimbo"
	DbKeyInvalidTxPrefix             = uint8(1)
	DbKeySlotsPrefix                 = uint8(2)
	DbKeyAwaitingBlockHandlingPrefix = uint8(3)

	DbKeyBatchesWitnessPrefix     = uint8(16)
	DbKeyBatchesL1InfoTreePrefix  = uint8(17)
	DbKeyBatchesBatchNumberPrefix = uint8(18)
	DbKeyBatchesForkIdPrefix      = uint8(19)

	DbKeyBlockNumber    = uint8(32)
	DbKeyBlockTimestamp = uint8(33)

	DbKeyTxRlpPrefix         = uint8(48)
	DbKeyTxStreamBytesPrefix = uint8(49)
	DbKeyTxRootPrefix        = uint8(50)
	DbKeyTxHashPrefix        = uint8(51)
	DbKeyTxSenderPrefix      = uint8(52)
)

var emptyHash = common.Hash{}

type LimboSendersWithChangedState struct {
	Storage map[uint64]int32
}

func NewLimboSendersWithChangedState() *LimboSendersWithChangedState {
	return &LimboSendersWithChangedState{
		Storage: make(map[uint64]int32),
	}
}

func (_this *LimboSendersWithChangedState) increment(senderId uint64) {
	value, found := _this.Storage[senderId]
	if !found {
		value = 0
	}
	_this.Storage[senderId] = value + 1

}

func (_this *LimboSendersWithChangedState) decrement(senderId uint64) {
	value, found := _this.Storage[senderId]
	if found {
		_this.Storage[senderId] = value - 1
	}

}

type LimboBatchTransactionDetails struct {
	Rlp         []byte
	StreamBytes []byte
	Root        common.Hash
	Hash        common.Hash
	Sender      common.Address
}

func newLimboBatchTransactionDetails(rlp, streamBytes []byte, hash common.Hash, sender common.Address) *LimboBatchTransactionDetails {
	return &LimboBatchTransactionDetails{
		Rlp:         rlp,
		StreamBytes: streamBytes,
		Root:        common.Hash{},
		Hash:        hash,
		Sender:      sender,
	}
}

func (_this *LimboBatchTransactionDetails) hasRoot() bool {
	return _this.Root != emptyHash
}

type Limbo struct {
	invalidTxsMap map[string]uint8 //invalid tx: hash -> handled
	limboSlots    *types.TxSlots
	limboBatches  []*LimboBatchDetails

	// used to denote some process has made the pool aware that an unwind is about to occur and to wait
	// until the unwind has been processed before allowing yielding of transactions again
	awaitingBlockHandling atomic.Bool
}

func newLimbo() *Limbo {
	return &Limbo{
		invalidTxsMap:         make(map[string]uint8),
		limboSlots:            &types.TxSlots{},
		limboBatches:          make([]*LimboBatchDetails, 0),
		awaitingBlockHandling: atomic.Bool{},
	}
}

func (_this *Limbo) resizeBatches(batchIndex, blockIndex, txIndex int) {
	if batchIndex == -1 {
		return
	}

	size := batchIndex + 1
	for i := len(_this.limboBatches); i < size; i++ {
		_this.limboBatches = append(_this.limboBatches, NewLimboBatchDetails())
	}
	_this.limboBatches[batchIndex].resizeBlocks(blockIndex, txIndex)
}

func (_this *Limbo) getFirstTxWithoutRootByBatch(batchNumber uint64) (*LimboBatchDetails, *LimboBatchBlockDetails, *LimboBatchTransactionDetails) {
	for _, limboBatch := range _this.limboBatches {
		for _, limboBlock := range limboBatch.Blocks {
			for _, limboTx := range limboBlock.Transactions {
				if !limboTx.hasRoot() {
					if batchNumber < limboBatch.BatchNumber {
						return nil, nil, nil
					}
					if batchNumber > limboBatch.BatchNumber {
						panic(fmt.Errorf("requested batch %d while the network is already on %d", limboBatch.BatchNumber, batchNumber))
					}

					return limboBatch, limboBlock, limboTx
				}
			}
		}
	}

	return nil, nil, nil
}

func (_this *Limbo) getLimboTxDetailsByTxHash(txHash *common.Hash) (*LimboBatchDetails, *LimboBatchBlockDetails, *LimboBatchTransactionDetails, uint32) {
	for _, limboBatch := range _this.limboBatches {
		limboTx, limboBlock, i := limboBatch.getTxDetailsByHash(txHash)
		if limboTx != nil {
			return limboBatch, limboBlock, limboTx, i
		}
	}

	return nil, nil, nil, math.MaxUint32
}

type LimboBatchBlockDetails struct {
	BlockNumber  uint64
	Timestamp    uint64
	Transactions []*LimboBatchTransactionDetails
}

func newLimboBatchBlockDetails(blockNumber, timestamp uint64) *LimboBatchBlockDetails {
	return &LimboBatchBlockDetails{
		BlockNumber:  blockNumber,
		Timestamp:    timestamp,
		Transactions: make([]*LimboBatchTransactionDetails, 0),
	}
}

func (_this *LimboBatchBlockDetails) resizeTransactions(txIndex int) {
	if txIndex == -1 {
		return
	}
	size := txIndex + 1
	for i := len(_this.Transactions); i < size; i++ {
		_this.Transactions = append(_this.Transactions, &LimboBatchTransactionDetails{})
	}
}

func (_this *LimboBatchBlockDetails) AppendTransaction(rlp, streamBytes []byte, hash common.Hash, sender common.Address) {
	_this.Transactions = append(_this.Transactions, newLimboBatchTransactionDetails(rlp, streamBytes, hash, sender))
}

type LimboBatchDetails struct {
	Witness                 []byte
	L1InfoTreeMinTimestamps map[uint64]uint64
	BatchNumber             uint64
	ForkId                  uint64
	Blocks                  []*LimboBatchBlockDetails
}

func NewLimboBatchDetails() *LimboBatchDetails {
	return &LimboBatchDetails{
		L1InfoTreeMinTimestamps: make(map[uint64]uint64),
		Blocks:                  make([]*LimboBatchBlockDetails, 0),
	}
}

func (_this *LimboBatchDetails) AppendBlock(blockNumber, timestamp uint64) *LimboBatchBlockDetails {
	limboBlock := newLimboBatchBlockDetails(blockNumber, timestamp)
	_this.Blocks = append(_this.Blocks, limboBlock)
	return limboBlock
}

func (_this *LimboBatchDetails) GetFirstBlockNumber() uint64 {
	return _this.Blocks[0].BlockNumber
}

func (_this *LimboBatchDetails) resizeBlocks(blockIndex, txIndex int) {
	if blockIndex == -1 {
		return
	}

	size := blockIndex + 1
	for i := len(_this.Blocks); i < size; i++ {
		_this.Blocks = append(_this.Blocks, &LimboBatchBlockDetails{})
	}
	_this.Blocks[blockIndex].resizeTransactions(txIndex)
}

func (_this *LimboBatchDetails) getTxDetailsByHash(txHash *common.Hash) (*LimboBatchTransactionDetails, *LimboBatchBlockDetails, uint32) {
	for _, limboBlock := range _this.Blocks {
		for j, limboTx := range limboBlock.Transactions {
			if limboTx.Hash == *txHash {
				return limboTx, limboBlock, uint32(j)
			}
		}
	}

	return nil, nil, math.MaxUint32
}

func (p *TxPool) GetLimboRecoveryDetails(batchNumber uint64) (uint64, *common.Hash, map[uint64]uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()

	limboBatch, limboBlock, limboTx := p.limbo.getFirstTxWithoutRootByBatch(batchNumber)
	if limboBatch == nil {
		return 0, nil, nil
	}
	blockTimestampsMap := make(map[uint64]uint64)
	for _, lb := range limboBatch.Blocks {
		blockTimestampsMap[lb.BlockNumber] = lb.Timestamp
		if lb.BlockNumber == limboBlock.BlockNumber {
			break
		}
	}

	return limboBlock.BlockNumber, &limboTx.Hash, blockTimestampsMap
}

func (p *TxPool) GetLimboTxRplsByHash(tx kv.Tx, blockNumber uint64, txHash *common.Hash) (*types.TxsRlp, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	limboBatch, limboBlock, _, txIndex := p.limbo.getLimboTxDetailsByTxHash(txHash)
	if limboBatch == nil {
		return nil, fmt.Errorf("missing transaction")
	}

	var workinglimboBlock *LimboBatchBlockDetails
	for _, workinglimboBlock = range limboBatch.Blocks {
		if workinglimboBlock.BlockNumber == blockNumber {
			break
		}
	}
	if workinglimboBlock == nil {
		return nil, fmt.Errorf("missing transaction")
	}

	txSize := uint32(len(workinglimboBlock.Transactions))
	if limboBlock.BlockNumber == workinglimboBlock.BlockNumber {
		txSize = txIndex + 1
	}

	txsRlps := &types.TxsRlp{}
	txsRlps.Resize(uint(txSize))
	for i := uint32(0); i < txSize; i++ {
		limboTx := workinglimboBlock.Transactions[i]
		txsRlps.Txs[i] = limboTx.Rlp
		copy(txsRlps.Senders.At(int(i)), limboTx.Sender[:])
		txsRlps.IsLocal[i] = true // all limbo tx are considered local //TODO: explain better about local
	}

	return txsRlps, nil
}

func (p *TxPool) UpdateLimboRootByTxHash(txHash *common.Hash, stateRoot *common.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()

	_, _, limboTx, _ := p.limbo.getLimboTxDetailsByTxHash(txHash)
	limboTx.Root = *stateRoot
}

func (p *TxPool) ProcessLimboBatchDetails(details *LimboBatchDetails) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.limbo.limboBatches = append(p.limbo.limboBatches, details)

	/*
		as we know we're about to enter an unwind we need to ensure that all the transactions have been
		handled after the unwind by the call to OnNewBlock before we can start yielding again.  There
		is a risk that in the small window of time between this call and the next call to yield
		by the stage loop a TX with a nonce too high will be yielded and cause an error during execution

		potential dragons here as if the OnNewBlock is never called the call to yield will always return empty
	*/
	p.denyYieldingTransactions()
}

func (p *TxPool) GetLimboDetails() []*LimboBatchDetails {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.limbo.limboBatches
}

func (p *TxPool) GetLimboDetailsCloned() []*LimboBatchDetails {
	p.lock.Lock()
	defer p.lock.Unlock()

	limboBatchesClone := make([]*LimboBatchDetails, len(p.limbo.limboBatches))
	copy(limboBatchesClone, p.limbo.limboBatches)
	return limboBatchesClone
}

func (p *TxPool) MarkProcessedLimboDetails(size int, invalidTxs []*string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, idHash := range invalidTxs {
		p.limbo.invalidTxsMap[*idHash] = 0
	}

	p.limbo.limboBatches = p.limbo.limboBatches[size:]
}

// should be called from within a locked context from the pool
func (p *TxPool) addLimboToUnwindTxs(unwindTxs *types.TxSlots) {
	for idx, slot := range p.limbo.limboSlots.Txs {
		unwindTxs.Append(slot, p.limbo.limboSlots.Senders.At(idx), p.limbo.limboSlots.IsLocal[idx])
	}
}

// should be called from within a locked context from the pool
func (p *TxPool) trimLimboSlots(unwindTxs *types.TxSlots) (types.TxSlots, *types.TxSlots, *types.TxSlots) {
	resultLimboTxs := types.TxSlots{}
	resultUnwindTxs := types.TxSlots{}
	resultForDiscard := types.TxSlots{}

	hasInvalidTxs := len(p.limbo.invalidTxsMap) > 0

	for idx, slot := range unwindTxs.Txs {
		if p.isTxKnownToLimbo(slot.IDHash) {
			resultLimboTxs.Append(slot, unwindTxs.Senders.At(idx), unwindTxs.IsLocal[idx])
		} else {
			if hasInvalidTxs {
				idHash := hexutils.BytesToHex(slot.IDHash[:])
				_, ok := p.limbo.invalidTxsMap[idHash]
				if ok {
					p.limbo.invalidTxsMap[idHash] = 1
					resultForDiscard.Append(slot, unwindTxs.Senders.At(idx), unwindTxs.IsLocal[idx])
					continue
				}
			}
			resultUnwindTxs.Append(slot, unwindTxs.Senders.At(idx), unwindTxs.IsLocal[idx])
		}
	}

	return resultUnwindTxs, &resultLimboTxs, &resultForDiscard
}

// should be called from within a locked context from the pool
func (p *TxPool) finalizeLimboOnNewBlock(limboTxs *types.TxSlots) {
	p.limbo.limboSlots = limboTxs

	forDelete := make([]*string, 0, len(p.limbo.invalidTxsMap))
	for idHash, shouldDelete := range p.limbo.invalidTxsMap {
		if shouldDelete == 1 {
			forDelete = append(forDelete, &idHash)
		}
	}

	for _, idHash := range forDelete {
		delete(p.limbo.invalidTxsMap, *idHash)
	}
}

// should be called from within a locked context from the pool
func (p *TxPool) isTxKnownToLimbo(hash common.Hash) bool {
	for _, limboBatch := range p.limbo.limboBatches {
		for _, limboBlock := range limboBatch.Blocks {
			for _, limboTx := range limboBlock.Transactions {
				if limboTx.Hash == hash {
					return true
				}
			}
		}
	}
	return false
}

func (p *TxPool) isDeniedYieldingTransactions() bool {
	return p.limbo.awaitingBlockHandling.Load()
}

func (p *TxPool) denyYieldingTransactions() {
	p.limbo.awaitingBlockHandling.Store(true)
}

func (p *TxPool) allowYieldingTransactions() {
	p.limbo.awaitingBlockHandling.Store(false)
}

func (p *TxPool) flushLockedLimbo(tx kv.RwTx) (err error) {
	if !p.ethCfg.Limbo {
		return nil
	}

	if err := tx.CreateBucket(TablePoolLimbo); err != nil {
		return err
	}

	if err := tx.ClearBucket(TablePoolLimbo); err != nil {
		return err
	}

	for hash, handled := range p.limbo.invalidTxsMap {
		hashAsBytes := hexutils.HexToBytes(hash)
		key := append([]byte{DbKeyInvalidTxPrefix}, hashAsBytes...)
		tx.Put(TablePoolLimbo, key, []byte{handled})
	}

	v := make([]byte, 0, 1024)
	for i, txSlot := range p.limbo.limboSlots.Txs {
		v = common.EnsureEnoughSize(v, 20+len(txSlot.Rlp))
		sender := p.limbo.limboSlots.Senders.At(i)

		copy(v[:20], sender)
		copy(v[20:], txSlot.Rlp)

		key := append([]byte{DbKeySlotsPrefix}, txSlot.IDHash[:]...)
		if err := tx.Put(TablePoolLimbo, key, v); err != nil {
			return err
		}
	}

	keyBytesBatch := make([]byte, 5)
	keyBytesBatchUint64Array := make([]byte, 13)
	keyBytesBlock := make([]byte, 9)
	keyBytesTx := make([]byte, 13)
	bytes8Value := make([]byte, 8)

	for i, limboBatch := range p.limbo.limboBatches {
		binary.LittleEndian.PutUint32(keyBytesBatch[1:5], uint32(i))
		binary.LittleEndian.PutUint32(keyBytesBatchUint64Array[1:5], uint32(i))
		binary.LittleEndian.PutUint32(keyBytesBlock[1:5], uint32(i))
		binary.LittleEndian.PutUint32(keyBytesTx[1:5], uint32(i))

		// Witness
		keyBytesBatch[0] = DbKeyBatchesWitnessPrefix
		if err := tx.Put(TablePoolLimbo, keyBytesBatch, limboBatch.Witness); err != nil {
			return err
		}

		// L1InfoTreeMinTimestamps
		keyBytesBatch[0] = DbKeyBatchesL1InfoTreePrefix
		copy(keyBytesBatchUint64Array, keyBytesBatch)
		for k, v := range limboBatch.L1InfoTreeMinTimestamps {
			binary.LittleEndian.PutUint64(keyBytesBatchUint64Array[5:13], uint64(k))
			binary.LittleEndian.PutUint64(bytes8Value[:], v)
			if err := tx.Put(TablePoolLimbo, keyBytesBatchUint64Array, bytes8Value); err != nil {
				return err
			}
		}

		// BatchNumber
		keyBytesBatch[0] = DbKeyBatchesBatchNumberPrefix
		binary.LittleEndian.PutUint64(bytes8Value[:], limboBatch.BatchNumber)
		if err := tx.Put(TablePoolLimbo, keyBytesBatch, bytes8Value); err != nil {
			return err
		}

		// ForkId
		keyBytesBatch[0] = DbKeyBatchesForkIdPrefix
		binary.LittleEndian.PutUint64(bytes8Value[:], limboBatch.ForkId)
		if err := tx.Put(TablePoolLimbo, keyBytesBatch, bytes8Value); err != nil {
			return err
		}

		for j, limboBlock := range limboBatch.Blocks {
			binary.LittleEndian.PutUint32(keyBytesBlock[5:9], uint32(j))
			binary.LittleEndian.PutUint32(keyBytesTx[5:9], uint32(j))

			// Block - Block number
			keyBytesBlock[0] = DbKeyBlockNumber
			binary.LittleEndian.PutUint64(bytes8Value[:], limboBlock.BlockNumber)
			if err := tx.Put(TablePoolLimbo, keyBytesBlock, bytes8Value); err != nil {
				return err
			}

			// Block - Timestamp
			keyBytesBlock[0] = DbKeyBlockTimestamp
			binary.LittleEndian.PutUint64(bytes8Value[:], limboBlock.Timestamp)
			if err := tx.Put(TablePoolLimbo, keyBytesBlock, bytes8Value); err != nil {
				return err
			}

			for k, limboTx := range limboBlock.Transactions {
				binary.LittleEndian.PutUint32(keyBytesTx[9:13], uint32(k))

				// Transaction - Rlp
				keyBytesTx[0] = DbKeyTxRlpPrefix
				if err := tx.Put(TablePoolLimbo, keyBytesTx, limboTx.Rlp[:]); err != nil {
					return err
				}

				// Transaction - Stream bytes
				keyBytesTx[0] = DbKeyTxStreamBytesPrefix
				if err := tx.Put(TablePoolLimbo, keyBytesTx, limboTx.StreamBytes[:]); err != nil {
					return err
				}

				// Transaction - Root
				keyBytesTx[0] = DbKeyTxRootPrefix
				if err := tx.Put(TablePoolLimbo, keyBytesTx, limboTx.Root[:]); err != nil {
					return err
				}

				// Transaction - Hash
				keyBytesTx[0] = DbKeyTxHashPrefix
				if err := tx.Put(TablePoolLimbo, keyBytesTx, limboTx.Hash[:]); err != nil {
					return err
				}

				// Transaction - Sender
				keyBytesTx[0] = DbKeyTxSenderPrefix
				if err := tx.Put(TablePoolLimbo, keyBytesTx, limboTx.Sender[:]); err != nil {
					return err
				}
			}
		}
	}

	v = []byte{0}
	if p.limbo.awaitingBlockHandling.Load() {
		v[0] = 1
	}
	if err := tx.Put(TablePoolLimbo, []byte{DbKeyAwaitingBlockHandlingPrefix}, v); err != nil {
		return err
	}

	return nil
}

func (p *TxPool) fromDBLimbo(ctx context.Context, tx kv.Tx, cacheView kvcache.CacheView) error {
	if !p.ethCfg.Limbo {
		return nil
	}

	p.limbo.limboSlots = &types.TxSlots{}
	parseCtx := types.NewTxParseContext(p.chainID)
	parseCtx.WithSender(false)

	it, err := tx.Range(TablePoolLimbo, nil, nil)
	if err != nil {
		return err
	}

	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}

		switch k[0] {
		case DbKeyInvalidTxPrefix:
			hash := hexutils.BytesToHex(k[1:])
			p.limbo.invalidTxsMap[hash] = v[0]
		case DbKeySlotsPrefix:
			addr, txRlp := *(*[20]byte)(v[:20]), v[20:]
			txn := &types.TxSlot{}

			_, err = parseCtx.ParseTransaction(txRlp, 0, txn, nil, false /* hasEnvelope */, nil)
			if err != nil {
				err = fmt.Errorf("err: %w, rlp: %x", err, txRlp)
				log.Warn("[txpool] fromDB: parseTransaction", "err", err)
				continue
			}

			txn.SenderID, txn.Traced = p.senders.getOrCreateID(addr)
			binary.BigEndian.Uint64(v)

			// ValidateTx function validates a tx against current network state.
			// Limbo transactions are expected to be invalid according to current network state.
			// That's why there is no point to check it while recovering the pool from a database.
			// These transactions may become valid after some of the current tx in the pool are executed
			// so leave the decision whether a limbo transaction (or any other transaction that has been unwound) to the execution stage.
			// if reason := p.validateTx(txn, true, cacheView, addr); reason != NotSet && reason != Success {
			// 	return nil
			// }
			p.limbo.limboSlots.Append(txn, addr[:], true)
		case DbKeyBatchesWitnessPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBatches(int(batchIndex), -1, -1)
			p.limbo.limboBatches[batchIndex].Witness = v
		case DbKeyBatchesL1InfoTreePrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBatches(int(batchIndex), -1, -1)
			l1InfoTreeKey := binary.LittleEndian.Uint64(k[5:13])
			p.limbo.limboBatches[batchIndex].L1InfoTreeMinTimestamps[l1InfoTreeKey] = binary.LittleEndian.Uint64(v)
		case DbKeyBatchesBatchNumberPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBatches(int(batchIndex), -1, -1)
			p.limbo.limboBatches[batchIndex].BatchNumber = binary.LittleEndian.Uint64(v)
		case DbKeyBatchesForkIdPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBatches(int(batchIndex), -1, -1)
			p.limbo.limboBatches[batchIndex].ForkId = binary.LittleEndian.Uint64(v)
		case DbKeyBlockNumber:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			blockIndex := binary.LittleEndian.Uint32(k[5:9])
			p.limbo.resizeBatches(int(batchIndex), int(blockIndex), -1)
			p.limbo.limboBatches[batchIndex].Blocks[blockIndex].BlockNumber = binary.LittleEndian.Uint64(v)
		case DbKeyBlockTimestamp:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			blockIndex := binary.LittleEndian.Uint32(k[5:9])
			p.limbo.resizeBatches(int(batchIndex), int(blockIndex), -1)
			p.limbo.limboBatches[batchIndex].Blocks[blockIndex].Timestamp = binary.LittleEndian.Uint64(v)
		case DbKeyTxRlpPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			blockIndex := binary.LittleEndian.Uint32(k[5:9])
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			p.limbo.resizeBatches(int(batchIndex), int(blockIndex), int(txIndex))
			p.limbo.limboBatches[batchIndex].Blocks[blockIndex].Transactions[txIndex].Rlp = v
		case DbKeyTxStreamBytesPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			blockIndex := binary.LittleEndian.Uint32(k[5:9])
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			p.limbo.resizeBatches(int(batchIndex), int(blockIndex), int(txIndex))
			p.limbo.limboBatches[batchIndex].Blocks[blockIndex].Transactions[txIndex].StreamBytes = v
		case DbKeyTxRootPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			blockIndex := binary.LittleEndian.Uint32(k[5:9])
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			p.limbo.resizeBatches(int(batchIndex), int(blockIndex), int(txIndex))
			copy(p.limbo.limboBatches[batchIndex].Blocks[blockIndex].Transactions[txIndex].Root[:], v)
		case DbKeyTxHashPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			blockIndex := binary.LittleEndian.Uint32(k[5:9])
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			p.limbo.resizeBatches(int(batchIndex), int(blockIndex), int(txIndex))
			copy(p.limbo.limboBatches[batchIndex].Blocks[blockIndex].Transactions[txIndex].Hash[:], v)
		case DbKeyTxSenderPrefix:
			batchIndex := binary.LittleEndian.Uint32(k[1:5])
			blockIndex := binary.LittleEndian.Uint32(k[5:9])
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			p.limbo.resizeBatches(int(batchIndex), int(blockIndex), int(txIndex))
			copy(p.limbo.limboBatches[batchIndex].Blocks[blockIndex].Transactions[txIndex].Sender[:], v)
		case DbKeyAwaitingBlockHandlingPrefix:
			p.limbo.awaitingBlockHandling.Store(v[0] != 0)
		default:
			panic("Invalid key")
		}

	}

	return nil
}

func prepareSendersWithChangedState(txs *types.TxSlots) *LimboSendersWithChangedState {
	sendersWithChangedState := NewLimboSendersWithChangedState()

	for _, txn := range txs.Txs {
		sendersWithChangedState.increment(txn.SenderID)
	}

	return sendersWithChangedState
}
