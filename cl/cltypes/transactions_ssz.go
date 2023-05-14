package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type TransactionsSSZ struct {
	underlying [][]byte       // underlying tranaction list
	root       libcommon.Hash // root
}

func (t *TransactionsSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) == 0 {
		return nil
	}
	if len(buf) < 4 {
		return ssz.ErrLowBufferSize
	}
	t.root = libcommon.Hash{}
	length := ssz.DecodeOffset(buf[:4]) / 4
	t.underlying = make([][]byte, length)
	for i := uint32(0); i < length; i++ {
		offsetPosition := i * 4
		startTx := ssz.DecodeOffset(buf[offsetPosition:])
		var endTx uint32
		if i == length-1 {
			endTx = uint32(len(buf))
		} else {
			endTx = ssz.DecodeOffset(buf[offsetPosition+4:])
		}
		if endTx < startTx {
			return ssz.ErrBadOffset
		}
		if len(buf) < int(endTx) {
			return ssz.ErrLowBufferSize
		}
		t.underlying[i] = buf[startTx:endTx]
	}
	return nil
}

func (t *TransactionsSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	txOffset := len(t.underlying) * 4
	for _, tx := range t.underlying {
		dst = append(dst, ssz.OffsetSSZ(uint32(txOffset))...)
		txOffset += len(tx)
	}
	// Write all transactions
	for _, tx := range t.underlying {
		dst = append(dst, tx...)
	}
	return dst, nil
}

func (t *TransactionsSSZ) HashSSZ() ([32]byte, error) {
	var err error
	if t.root != (libcommon.Hash{}) {
		return t.root, nil
	}
	t.root, err = merkle_tree.TransactionsListRoot(t.underlying)
	return t.root, err
}

func (t *TransactionsSSZ) EncodingSize() (size int) {
	if t == nil {
		return 0
	}
	for _, tx := range t.underlying {
		size += len(tx) + 4
	}
	return
}

func NewTransactionsSSZFromTransactions(txs [][]byte) *TransactionsSSZ {
	return &TransactionsSSZ{
		underlying: txs,
	}
}

func (t *TransactionsSSZ) UnderlyngReference() [][]byte {
	return t.underlying
}

func (t *TransactionsSSZ) ForEach(fn func(tx []byte, idx int, total int) bool) {
	if t == nil {
		return
	}
	for idx, tx := range t.underlying {
		ok := fn(tx, idx, len(t.underlying))
		if !ok {
			break
		}
	}
}
