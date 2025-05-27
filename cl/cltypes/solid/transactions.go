// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package solid

import (
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

type TransactionsSSZ struct {
	underlying [][]byte    // underlying transaction list
	root       common.Hash // root
}

func (t *TransactionsSSZ) UnmarshalJSON(buf []byte) error {
	tmp := []hexutil.Bytes{}
	t.root = common.Hash{}
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	t.underlying = nil
	for _, tx := range tmp {
		t.underlying = append(t.underlying, tx)
	}
	return nil
}

func (t TransactionsSSZ) MarshalJSON() ([]byte, error) {
	tmp := []hexutil.Bytes{}
	for _, tx := range t.underlying {
		tmp = append(tmp, tx)
	}
	return json.Marshal(tmp)
}

func (*TransactionsSSZ) Clone() clonable.Clonable {
	return &TransactionsSSZ{}
}

func (*TransactionsSSZ) Static() bool {
	return false
}

func (t *TransactionsSSZ) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) == 0 {
		return nil
	}
	if len(buf) < 4 {
		return ssz.ErrLowBufferSize
	}
	t.root = common.Hash{}
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
	if t.root != (common.Hash{}) {
		return t.root, nil
	}
	t.root, err = merkle_tree.TransactionsListRoot(t.underlying)
	return t.root, err
}

func (t *TransactionsSSZ) EncodingSizeSSZ() (size int) {
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
