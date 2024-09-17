package txpool

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/types"
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
	invalidTxsMap        map[string]uint8 //invalid tx: hash -> handled
	limboSlots           *types.TxSlots
	uncheckedLimboBlocks []*LimboBlockDetails
	invalidLimboBlocks   []*LimboBlockDetails

	// used to denote some process has made the pool aware that an unwind is about to occur and to wait
	// until the unwind has been processed before allowing yielding of transactions again
	awaitingBlockHandling atomic.Bool
}

func newLimbo() *Limbo {
	return &Limbo{
		invalidTxsMap:         make(map[string]uint8),
		limboSlots:            &types.TxSlots{},
		uncheckedLimboBlocks:  make([]*LimboBlockDetails, 0),
		invalidLimboBlocks:    make([]*LimboBlockDetails, 0),
		awaitingBlockHandling: atomic.Bool{},
	}
}

func (_this *Limbo) resizeUncheckedBlocks(blockIndex, txIndex int) {
	if blockIndex == -1 {
		return
	}

	size := blockIndex + 1
	for i := len(_this.uncheckedLimboBlocks); i < size; i++ {
		_this.uncheckedLimboBlocks = append(_this.uncheckedLimboBlocks, NewLimboBlockDetails())
	}

	_this.uncheckedLimboBlocks[blockIndex].resizeTransactions(txIndex)
}

func (_this *Limbo) resizeInvalidBlocks(blockIndex, txIndex int) {
	if blockIndex == -1 {
		return
	}

	size := blockIndex + 1
	for i := len(_this.invalidLimboBlocks); i < size; i++ {
		_this.invalidLimboBlocks = append(_this.invalidLimboBlocks, NewLimboBlockDetails())
	}

	_this.invalidLimboBlocks[blockIndex].resizeTransactions(txIndex)
}

func (_this *Limbo) getFirstTxWithoutRootByBlockNumber(blockNumber uint64) (*LimboBlockDetails, *LimboBlockTransactionDetails) {
	for _, limboBlock := range _this.uncheckedLimboBlocks {
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
	for i, limboBlock := range _this.uncheckedLimboBlocks {
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

func (p *TxPool) ProcessUncheckedLimboBlockDetails(limboBlock *LimboBlockDetails) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.limbo.uncheckedLimboBlocks = append(p.limbo.uncheckedLimboBlocks, limboBlock)

	/*
		as we know we're about to enter an unwind we need to ensure that all the transactions have been
		handled after the unwind by the call to OnNewBlock before we can start yielding again.  There
		is a risk that in the small window of time between this call and the next call to yield
		by the stage loop a TX with a nonce too high will be yielded and cause an error during execution

		potential dragons here as if the OnNewBlock is never called the call to yield will always return empty
	*/
	p.denyYieldingTransactions()
}

func (p *TxPool) GetInvalidLimboBlocksDetails() []*LimboBlockDetails {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.limbo.invalidLimboBlocks
}

func (p *TxPool) GetUncheckedLimboBlocksDetailsClonedWeak() []*LimboBlockDetails {
	p.lock.Lock()
	defer p.lock.Unlock()

	limboBlocksClone := make([]*LimboBlockDetails, len(p.limbo.uncheckedLimboBlocks))
	copy(limboBlocksClone, p.limbo.uncheckedLimboBlocks)
	return limboBlocksClone
}

func (p *TxPool) MarkProcessedLimboDetails(size int, invalidBatchesIndices []int, invalidTxs []*string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, idHash := range invalidTxs {
		p.limbo.invalidTxsMap[*idHash] = 0
	}

	for _, invalidBatchesIndex := range invalidBatchesIndices {
		p.limbo.invalidLimboBlocks = append(p.limbo.invalidLimboBlocks, p.limbo.uncheckedLimboBlocks[invalidBatchesIndex])
	}
	p.limbo.uncheckedLimboBlocks = p.limbo.uncheckedLimboBlocks[size:]
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
	for _, limbo := range p.limbo.uncheckedLimboBlocks {
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

func prepareSendersWithChangedState(txs *types.TxSlots) *LimboSendersWithChangedState {
	sendersWithChangedState := NewLimboSendersWithChangedState()

	for _, txn := range txs.Txs {
		sendersWithChangedState.increment(txn.SenderID)
	}

	return sendersWithChangedState
}
