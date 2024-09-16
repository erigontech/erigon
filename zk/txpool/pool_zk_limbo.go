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

	DbKeyBlockWitnessPrefix        = uint8(16)
	DbKeyBlockL1InfoTreePrefix     = uint8(17)
	DbKeyBlockBlockTimestampPrefix = uint8(18)
	DbKeyBlockBlockNumberPrefix    = uint8(19)
	DbKeyBlockBatchNumberPrefix    = uint8(20)
	DbKeyBlockForkIdPrefix         = uint8(21)

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

type LimboBlockTransactionDetails struct {
	Rlp         []byte
	StreamBytes []byte
	Root        common.Hash
	Hash        common.Hash
	Sender      common.Address
}

func newLimboBlockTransactionDetails(rlp, streamBytes []byte, hash common.Hash, sender common.Address) *LimboBlockTransactionDetails {
	return &LimboBlockTransactionDetails{
		Rlp:         rlp,
		StreamBytes: streamBytes,
		Root:        common.Hash{},
		Hash:        hash,
		Sender:      sender,
	}
}

func (_this *LimboBlockTransactionDetails) hasRoot() bool {
	return _this.Root != emptyHash
}

type Limbo struct {
	invalidTxsMap map[string]uint8 //invalid tx: hash -> handled
	limboSlots    *types.TxSlots
	limboBlocks   []*LimboBlockDetails

	// used to denote some process has made the pool aware that an unwind is about to occur and to wait
	// until the unwind has been processed before allowing yielding of transactions again
	awaitingBlockHandling atomic.Bool
}

func newLimbo() *Limbo {
	return &Limbo{
		invalidTxsMap:         make(map[string]uint8),
		limboSlots:            &types.TxSlots{},
		limboBlocks:           make([]*LimboBlockDetails, 0),
		awaitingBlockHandling: atomic.Bool{},
	}
}

func (_this *Limbo) resizeBlocks(blockIndex, txIndex int) {
	if blockIndex == -1 {
		return
	}

	size := blockIndex + 1
	for i := len(_this.limboBlocks); i < size; i++ {
		_this.limboBlocks = append(_this.limboBlocks, NewLimboBlockDetails())
	}

	_this.limboBlocks[blockIndex].resizeTransactions(txIndex)
}

func (_this *Limbo) getFirstTxWithoutRootByBlockNumber(blockNumber uint64) (*LimboBlockDetails, *LimboBlockTransactionDetails) {
	for _, limboBlock := range _this.limboBlocks {
		for _, limboTx := range limboBlock.Transactions {
			if !limboTx.hasRoot() {
				if blockNumber < limboBlock.BlockNumber {
					return nil, nil
				}
				if blockNumber > limboBlock.BlockNumber {
					panic(fmt.Errorf("requested block %d while the network is already on %d", limboBlock.BlockNumber, blockNumber))
				}

				return limboBlock, limboTx
			}
		}
	}

	return nil, nil
}

func (_this *Limbo) getTxDetailsByHash(txHash *common.Hash) (*LimboBlockDetails, *LimboBlockTransactionDetails, uint32, uint32) {
	for i, limboBlock := range _this.limboBlocks {
		limboTx, j := limboBlock.getTxDetailsByHash(txHash)
		if limboTx != nil {
			return limboBlock, limboTx, uint32(i), j
		}
	}

	return nil, nil, math.MaxUint32, math.MaxUint32
}

type LimboBlockDetails struct {
	Witness                 []byte
	L1InfoTreeMinTimestamps map[uint64]uint64
	BlockTimestamp          uint64
	BlockNumber             uint64
	BatchNumber             uint64
	ForkId                  uint64
	Transactions            []*LimboBlockTransactionDetails
}

func NewLimboBlockDetails() *LimboBlockDetails {
	return &LimboBlockDetails{
		L1InfoTreeMinTimestamps: make(map[uint64]uint64),
		Transactions:            make([]*LimboBlockTransactionDetails, 0),
	}
}

func (_this *LimboBlockDetails) resizeTransactions(txIndex int) {
	if txIndex == -1 {
		return
	}
	size := txIndex + 1

	for i := len(_this.Transactions); i < size; i++ {
		_this.Transactions = append(_this.Transactions, &LimboBlockTransactionDetails{})
	}
}

func (_this *LimboBlockDetails) AppendTransaction(rlp, streamBytes []byte, hash common.Hash, sender common.Address) uint32 {
	_this.Transactions = append(_this.Transactions, newLimboBlockTransactionDetails(rlp, streamBytes, hash, sender))
	return uint32(len(_this.Transactions))
}

func (_this *LimboBlockDetails) getTxDetailsByHash(txHash *common.Hash) (*LimboBlockTransactionDetails, uint32) {
	for i, limboTx := range _this.Transactions {
		if limboTx.Hash == *txHash {
			return limboTx, uint32(i)
		}
	}

	return nil, math.MaxUint32
}

func (p *TxPool) GetLimboDetailsForRecovery(blockNumber uint64) (*LimboBlockDetails, *common.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()

	limboBlock, limboTx := p.limbo.getFirstTxWithoutRootByBlockNumber(blockNumber)
	if limboBlock == nil {
		return nil, nil
	}
	return limboBlock, &limboTx.Hash
}

func (p *TxPool) GetLimboTxRplsByHash(tx kv.Tx, txHash *common.Hash) (*types.TxsRlp, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	limboBlock, _, _, txIndex := p.limbo.getTxDetailsByHash(txHash)
	if limboBlock == nil {
		return nil, fmt.Errorf("missing transaction")
	}

	txSize := txIndex + 1

	txsRlps := &types.TxsRlp{}
	txsRlps.Resize(uint(txSize))
	for i := uint32(0); i < txSize; i++ {
		limboTx := limboBlock.Transactions[i]
		txsRlps.Txs[i] = limboTx.Rlp
		copy(txsRlps.Senders.At(int(i)), limboTx.Sender[:])
		txsRlps.IsLocal[i] = true // all limbo tx are considered local //TODO: explain better about local
	}

	return txsRlps, nil
}

func (p *TxPool) UpdateLimboRootByTxHash(txHash *common.Hash, stateRoot *common.Hash) {
	p.lock.Lock()
	defer p.lock.Unlock()

	_, limboTx, _, _ := p.limbo.getTxDetailsByHash(txHash)
	limboTx.Root = *stateRoot
}

func (p *TxPool) ProcessLimboBlockDetails(limboBlock *LimboBlockDetails) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.limbo.limboBlocks = append(p.limbo.limboBlocks, limboBlock)

	/*
		as we know we're about to enter an unwind we need to ensure that all the transactions have been
		handled after the unwind by the call to OnNewBlock before we can start yielding again.  There
		is a risk that in the small window of time between this call and the next call to yield
		by the stage loop a TX with a nonce too high will be yielded and cause an error during execution

		potential dragons here as if the OnNewBlock is never called the call to yield will always return empty
	*/
	p.denyYieldingTransactions()
}

func (p *TxPool) GetLimboDetails() []*LimboBlockDetails {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.limbo.limboBlocks
}

func (p *TxPool) GetLimboDetailsCloned() []*LimboBlockDetails {
	p.lock.Lock()
	defer p.lock.Unlock()

	limboBlocksClone := make([]*LimboBlockDetails, len(p.limbo.limboBlocks))
	copy(limboBlocksClone, p.limbo.limboBlocks)
	return limboBlocksClone
}

func (p *TxPool) MarkProcessedLimboDetails(size int, invalidTxs []*string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, idHash := range invalidTxs {
		p.limbo.invalidTxsMap[*idHash] = 0
	}

	p.limbo.limboBlocks = p.limbo.limboBlocks[size:]
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
	for _, limbo := range p.limbo.limboBlocks {
		for _, limboTx := range limbo.Transactions {
			if limboTx.Hash == hash {
				return true
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

	keyBytesBlock := make([]byte, 5)
	keyBytesBlockUint64Array := make([]byte, 13)
	keyBytesTx := make([]byte, 9)
	bytes8Value := make([]byte, 8)

	for i, limboBlock := range p.limbo.limboBlocks {
		binary.LittleEndian.PutUint32(keyBytesBlock[1:5], uint32(i))
		binary.LittleEndian.PutUint32(keyBytesBlockUint64Array[1:5], uint32(i))
		binary.LittleEndian.PutUint32(keyBytesTx[1:5], uint32(i))

		// Witness
		keyBytesBlock[0] = DbKeyBlockWitnessPrefix
		if err := tx.Put(TablePoolLimbo, keyBytesBlock, limboBlock.Witness); err != nil {
			return err
		}

		// L1InfoTreeMinTimestamps
		keyBytesBlock[0] = DbKeyBlockL1InfoTreePrefix
		copy(keyBytesBlockUint64Array, keyBytesBlock)
		for k, v := range limboBlock.L1InfoTreeMinTimestamps {
			binary.LittleEndian.PutUint64(keyBytesBlockUint64Array[5:13], uint64(k))
			binary.LittleEndian.PutUint64(bytes8Value[:], v)
			if err := tx.Put(TablePoolLimbo, keyBytesBlockUint64Array, bytes8Value); err != nil {
				return err
			}
		}

		// BlockTimestamp
		keyBytesBlock[0] = DbKeyBlockBlockTimestampPrefix
		binary.LittleEndian.PutUint64(bytes8Value[:], limboBlock.BlockTimestamp)
		if err := tx.Put(TablePoolLimbo, keyBytesBlock, bytes8Value); err != nil {
			return err
		}

		// BlockNumber
		keyBytesBlock[0] = DbKeyBlockBlockNumberPrefix
		binary.LittleEndian.PutUint64(bytes8Value[:], limboBlock.BlockNumber)
		if err := tx.Put(TablePoolLimbo, keyBytesBlock, bytes8Value); err != nil {
			return err
		}

		// BatchNumber
		keyBytesBlock[0] = DbKeyBlockBatchNumberPrefix
		binary.LittleEndian.PutUint64(bytes8Value[:], limboBlock.BatchNumber)
		if err := tx.Put(TablePoolLimbo, keyBytesBlock, bytes8Value); err != nil {
			return err
		}

		// ForkId
		keyBytesBlock[0] = DbKeyBlockForkIdPrefix
		binary.LittleEndian.PutUint64(bytes8Value[:], limboBlock.ForkId)
		if err := tx.Put(TablePoolLimbo, keyBytesBlock, bytes8Value); err != nil {
			return err
		}

		for j, limboTx := range limboBlock.Transactions {
			binary.LittleEndian.PutUint32(keyBytesTx[5:9], uint32(j))

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
		case DbKeyBlockWitnessPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBlocks(int(blockIndex), -1)
			p.limbo.limboBlocks[blockIndex].Witness = v
		case DbKeyBlockL1InfoTreePrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBlocks(int(blockIndex), -1)
			l1InfoTreeKey := binary.LittleEndian.Uint64(k[5:13])
			p.limbo.limboBlocks[blockIndex].L1InfoTreeMinTimestamps[l1InfoTreeKey] = binary.LittleEndian.Uint64(v)
		case DbKeyBlockBlockTimestampPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBlocks(int(blockIndex), -1)
			p.limbo.limboBlocks[blockIndex].BlockTimestamp = binary.LittleEndian.Uint64(v)
		case DbKeyBlockBlockNumberPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBlocks(int(blockIndex), -1)
			p.limbo.limboBlocks[blockIndex].BlockNumber = binary.LittleEndian.Uint64(v)
		case DbKeyBlockBatchNumberPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBlocks(int(blockIndex), -1)
			p.limbo.limboBlocks[blockIndex].BatchNumber = binary.LittleEndian.Uint64(v)
		case DbKeyBlockForkIdPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			p.limbo.resizeBlocks(int(blockIndex), -1)
			p.limbo.limboBlocks[blockIndex].ForkId = binary.LittleEndian.Uint64(v)
		case DbKeyTxRlpPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			txIndex := binary.LittleEndian.Uint32(k[5:9])
			p.limbo.resizeBlocks(int(blockIndex), int(txIndex))
			p.limbo.limboBlocks[blockIndex].Transactions[txIndex].Rlp = v
		case DbKeyTxStreamBytesPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			txIndex := binary.LittleEndian.Uint32(k[5:9])
			p.limbo.resizeBlocks(int(blockIndex), int(txIndex))
			p.limbo.limboBlocks[blockIndex].Transactions[txIndex].StreamBytes = v
		case DbKeyTxRootPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			txIndex := binary.LittleEndian.Uint32(k[5:9])
			p.limbo.resizeBlocks(int(blockIndex), int(txIndex))
			copy(p.limbo.limboBlocks[blockIndex].Transactions[txIndex].Root[:], v)
		case DbKeyTxHashPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			txIndex := binary.LittleEndian.Uint32(k[5:9])
			p.limbo.resizeBlocks(int(blockIndex), int(txIndex))
			copy(p.limbo.limboBlocks[blockIndex].Transactions[txIndex].Hash[:], v)
		case DbKeyTxSenderPrefix:
			blockIndex := binary.LittleEndian.Uint32(k[1:5])
			txIndex := binary.LittleEndian.Uint32(k[5:9])
			p.limbo.resizeBlocks(int(blockIndex), int(txIndex))
			copy(p.limbo.limboBlocks[blockIndex].Transactions[txIndex].Sender[:], v)
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
