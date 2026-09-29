// Copyright 2022 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package types

import (
	"bytes"
	"fmt"
	"io"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

//go:generate go run github.com/erigontech/erigon/cmd/tools/jsongen -type Withdrawal

// Withdrawal represents a validator withdrawal from the consensus layer.
// See EIP-4895: Beacon chain push withdrawals as operations.
type Withdrawal struct {
	Index     hexutil.Uint64 `json:"index" ethjson:"quantity"`          // monotonically increasing identifier issued by consensus layer
	Validator hexutil.Uint64 `json:"validatorIndex" ethjson:"quantity"` // index of validator associated with withdrawal
	Address   common.Address `json:"address" ethjson:"data"`            // target address for withdrawn ether
	Amount    hexutil.Uint64 `json:"amount" ethjson:"quantity"`         // value of withdrawal in GWei
}

func (obj *Withdrawal) EncodingSize() int {
	encodingSize := 21 /* Address */
	encodingSize += rlp.U64Len(uint64(obj.Index))
	encodingSize += rlp.U64Len(uint64(obj.Validator))
	encodingSize += rlp.U64Len(uint64(obj.Amount))
	return encodingSize
}

func (obj *Withdrawal) EncodeRLP(w io.Writer) error {
	encodingSize := obj.EncodingSize()

	b := rlp.NewEncodingBuf()
	defer b.Release()

	if err := rlp.EncodeListPrefix(encodingSize, w, b[:]); err != nil {
		return err
	}

	if err := rlp.EncodeU64(uint64(obj.Index), w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeU64(uint64(obj.Validator), w, b[:]); err != nil {
		return err
	}

	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(obj.Address[:]); err != nil {
		return err
	}

	return rlp.EncodeU64(uint64(obj.Amount), w, b[:])
}

func (obj *Withdrawal) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}

	var v uint64
	if v, err = s.Uint64(); err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	obj.Index = hexutil.Uint64(v)
	if v, err = s.Uint64(); err != nil {
		return fmt.Errorf("read Validator: %w", err)
	}
	obj.Validator = hexutil.Uint64(v)
	if err = s.ReadBytes(obj.Address[:]); err != nil {
		return fmt.Errorf("read Address: %w", err)
	}
	if v, err = s.Uint64(); err != nil {
		return fmt.Errorf("read Amount: %w", err)
	}
	obj.Amount = hexutil.Uint64(v)

	return s.ListEnd()
}

func (*Withdrawal) Clone() clonable.Clonable {
	return &Withdrawal{}
}

// Withdrawals implements DerivableList for withdrawals.
type Withdrawals []*Withdrawal

// MarshalFastJSONTo writes the withdrawals as a bare array. The receiver must stay a value, so
// the type itself satisfies the fast-JSON interface.
func (ws Withdrawals) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	jsonstream.ArrayValue(s, ws, writeWithdrawalElem)
	return nil
}

func writeWithdrawalElem(s *jsonstream.StackStream, w **Withdrawal) { _ = (*w).MarshalFastJSONTo(s) }

func (ws Withdrawals) Len() int { return len(ws) }

// EncodeIndex encodes the i'th withdrawal to w. Note that this does not check for errors
// because we assume that *Withdrawal will only ever contain valid withdrawals that were either
// constructed by decoding or via public API in this package.
func (ws Withdrawals) EncodeIndex(i int, w *bytes.Buffer) {
	_ = rlp.Encode(w, ws[i])
}
