package txpool

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcache"
	"github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/log/v3"
	"github.com/status-im/keycard-go/hexutils"
)

type LimboBlockPersistentHelper struct {
	keyBytesBlock            []byte
	keyBytesBlockUint64Array []byte
	keyBytesTx               []byte
	bytes8Value              []byte
}

func newLimboBlockPersistentHelper() *LimboBlockPersistentHelper {
	return &LimboBlockPersistentHelper{
		keyBytesBlock:            make([]byte, 9),
		keyBytesBlockUint64Array: make([]byte, 17),
		keyBytesTx:               make([]byte, 13),
		bytes8Value:              make([]byte, 8),
	}
}

func (h *LimboBlockPersistentHelper) setBlockIndex(unckeckedBlockIndex, invalidBlockIndex int) {
	binary.LittleEndian.PutUint32(h.keyBytesBlock[1:5], uint32(unckeckedBlockIndex))
	binary.LittleEndian.PutUint32(h.keyBytesBlock[5:9], uint32(invalidBlockIndex))
	binary.LittleEndian.PutUint32(h.keyBytesBlockUint64Array[1:5], uint32(unckeckedBlockIndex))
	binary.LittleEndian.PutUint32(h.keyBytesBlockUint64Array[5:9], uint32(invalidBlockIndex))
	binary.LittleEndian.PutUint32(h.keyBytesTx[1:5], uint32(unckeckedBlockIndex))
	binary.LittleEndian.PutUint32(h.keyBytesTx[5:9], uint32(invalidBlockIndex))
}

func (h *LimboBlockPersistentHelper) setTxIndex(txIndex int) {
	binary.LittleEndian.PutUint32(h.keyBytesTx[9:13], uint32(txIndex))
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

	limboBlockPersistentHelper := newLimboBlockPersistentHelper()
	for i, limboBlock := range p.limbo.uncheckedLimboBlocks {
		limboBlockPersistentHelper.setBlockIndex(i, math.MaxUint32)
		if err = flushLockedLimboBlock(tx, limboBlock, limboBlockPersistentHelper); err != nil {
			return err
		}
	}
	for i, limboBlock := range p.limbo.invalidLimboBlocks {
		limboBlockPersistentHelper.setBlockIndex(math.MaxUint32, i)
		if err = flushLockedLimboBlock(tx, limboBlock, limboBlockPersistentHelper); err != nil {
			return err
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

func flushLockedLimboBlock(tx kv.RwTx, limboBlock *LimboBlockDetails, limboBlockPersistentHelper *LimboBlockPersistentHelper) error {
	keyBytesBlock := limboBlockPersistentHelper.keyBytesBlock
	keyBytesBlockUint64Array := limboBlockPersistentHelper.keyBytesBlockUint64Array
	keyBytesTx := limboBlockPersistentHelper.keyBytesTx
	bytes8Value := limboBlockPersistentHelper.bytes8Value

	// Witness
	keyBytesBlock[0] = DbKeyBlockWitnessPrefix
	if err := tx.Put(TablePoolLimbo, keyBytesBlock, limboBlock.Witness); err != nil {
		return err
	}

	// L1InfoTreeMinTimestamps
	keyBytesBlock[0] = DbKeyBlockL1InfoTreePrefix
	copy(keyBytesBlockUint64Array, keyBytesBlock)
	for k, v := range limboBlock.L1InfoTreeMinTimestamps {
		binary.LittleEndian.PutUint64(keyBytesBlockUint64Array[9:17], uint64(k))
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
		limboBlockPersistentHelper.setTxIndex(j)

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
			fromDBLimboBlock(p, -1, k, v).Witness = v
		case DbKeyBlockL1InfoTreePrefix:
			l1InfoTreeKey := binary.LittleEndian.Uint64(k[9:17])
			fromDBLimboBlock(p, -1, k, v).L1InfoTreeMinTimestamps[l1InfoTreeKey] = binary.LittleEndian.Uint64(v)
		case DbKeyBlockBlockTimestampPrefix:
			fromDBLimboBlock(p, -1, k, v).BlockTimestamp = binary.LittleEndian.Uint64(v)
		case DbKeyBlockBlockNumberPrefix:
			fromDBLimboBlock(p, -1, k, v).BlockNumber = binary.LittleEndian.Uint64(v)
		case DbKeyBlockBatchNumberPrefix:
			fromDBLimboBlock(p, -1, k, v).BatchNumber = binary.LittleEndian.Uint64(v)
		case DbKeyBlockForkIdPrefix:
			fromDBLimboBlock(p, -1, k, v).ForkId = binary.LittleEndian.Uint64(v)
		case DbKeyTxRlpPrefix:
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			fromDBLimboBlock(p, int(txIndex), k, v).Transactions[txIndex].Rlp = v
		case DbKeyTxStreamBytesPrefix:
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			fromDBLimboBlock(p, int(txIndex), k, v).Transactions[txIndex].StreamBytes = v
		case DbKeyTxRootPrefix:
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			copy(fromDBLimboBlock(p, int(txIndex), k, v).Transactions[txIndex].Root[:], v)
		case DbKeyTxHashPrefix:
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			copy(fromDBLimboBlock(p, int(txIndex), k, v).Transactions[txIndex].Hash[:], v)
		case DbKeyTxSenderPrefix:
			txIndex := binary.LittleEndian.Uint32(k[9:13])
			copy(fromDBLimboBlock(p, int(txIndex), k, v).Transactions[txIndex].Sender[:], v)
		case DbKeyAwaitingBlockHandlingPrefix:
			p.limbo.awaitingBlockHandling.Store(v[0] != 0)
		default:
			panic("Invalid key")
		}

	}

	return nil
}

func fromDBLimboBlock(p *TxPool, txIndex int, k, v []byte) *LimboBlockDetails {
	uncheckedBlockIndex := binary.LittleEndian.Uint32(k[1:5])
	invalidBlockIndex := binary.LittleEndian.Uint32(k[5:9])
	if uncheckedBlockIndex != math.MaxUint32 {
		p.limbo.resizeUncheckedBlocks(int(uncheckedBlockIndex), txIndex)
		return p.limbo.uncheckedLimboBlocks[uncheckedBlockIndex]
	}

	p.limbo.resizeInvalidBlocks(int(invalidBlockIndex), txIndex)
	return p.limbo.invalidLimboBlocks[invalidBlockIndex]
}
