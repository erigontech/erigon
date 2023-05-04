// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"bytes"
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/cl/cltypes/clonable"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rlp"
)

//go:generate go run github.com/fjl/gencodec -type Withdrawal -field-override withdrawalMarshaling -out gen_withdrawal_json.go

// Withdrawal represents a validator withdrawal from the consensus layer.
// See EIP-4895: Beacon chain push withdrawals as operations.
type Withdrawal struct {
	Index     uint64            `json:"index"`          // monotonically increasing identifier issued by consensus layer
	Validator uint64            `json:"validatorIndex"` // index of validator associated with withdrawal
	Address   libcommon.Address `json:"address"`        // target address for withdrawn ether
	Amount    uint64            `json:"amount"`         // value of withdrawal in GWei
}

func (obj *Withdrawal) Equal(other *Withdrawal) bool {
	return obj.Index == other.Index && obj.Validator == other.Validator &&
		obj.Address == other.Address && obj.Amount == other.Amount
}

func (obj *Withdrawal) EncodingSize() int {
	encodingSize := 21 /* Address */
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(obj.Index)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(obj.Validator)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(obj.Amount)
	return encodingSize
}

func (obj *Withdrawal) EncodeRLP(w io.Writer) error {
	encodingSize := obj.EncodingSize()

	var b [33]byte
	if err := EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}

	if err := rlp.EncodeInt(obj.Index, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(obj.Validator, w, b[:]); err != nil {
		return err
	}

	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(obj.Address.Bytes()); err != nil {
		return err
	}

	return rlp.EncodeInt(obj.Amount, w, b[:])
}

func (obj *Withdrawal) EncodeSSZ() []byte {
	buf := make([]byte, obj.EncodingSizeSSZ())
	ssz.MarshalUint64SSZ(buf, obj.Index)
	ssz.MarshalUint64SSZ(buf[8:], obj.Validator)
	copy(buf[16:], obj.Address[:])
	ssz.MarshalUint64SSZ(buf[36:], obj.Amount)
	return buf
}

func (obj *Withdrawal) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < obj.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	obj.Index = ssz.UnmarshalUint64SSZ(buf)
	obj.Validator = ssz.UnmarshalUint64SSZ(buf[8:])
	copy(obj.Address[:], buf[16:])
	obj.Amount = ssz.UnmarshalUint64SSZ(buf[36:])
	return nil
}

func (obj *Withdrawal) EncodingSizeSSZ() int {
	// Validator Index (8 bytes) + Index (8 bytes) + Amount (8 bytes) + address length
	return 24 + length.Addr
}

func (obj *Withdrawal) HashSSZ() ([32]byte, error) { // the [32]byte is temporary
	var addressLeaf [32]byte
	copy(addressLeaf[:], obj.Address[:])
	return merkle_tree.ArraysRoot([][32]byte{
		merkle_tree.Uint64Root(obj.Index),
		merkle_tree.Uint64Root(obj.Validator),
		addressLeaf,
		merkle_tree.Uint64Root(obj.Amount),
	}, 4)
}

func (obj *Withdrawal) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}

	if obj.Index, err = s.Uint(); err != nil {
		return fmt.Errorf("read Index: %w", err)
	}
	if obj.Validator, err = s.Uint(); err != nil {
		return fmt.Errorf("read Validator: %w", err)
	}

	var b []byte
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Address: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for Address: %d", len(b))
	}
	copy(obj.Address[:], b)

	if obj.Amount, err = s.Uint(); err != nil {
		return fmt.Errorf("read Amount: %w", err)
	}

	return s.ListEnd()
}

func (*Withdrawal) Clone() clonable.Clonable {
	return &Withdrawal{}
}

// field type overrides for gencodec
type withdrawalMarshaling struct {
	Index     hexutil.Uint64
	Validator hexutil.Uint64
	Amount    hexutil.Uint64
}

// Withdrawals implements DerivableList for withdrawals.
type Withdrawals []*Withdrawal

func (s Withdrawals) Len() int { return len(s) }

// EncodeIndex encodes the i'th withdrawal to w. Note that this does not check for errors
// because we assume that *Withdrawal will only ever contain valid withdrawals that were either
// constructed by decoding or via public API in this package.
func (s Withdrawals) EncodeIndex(i int, w *bytes.Buffer) {
	rlp.Encode(w, s[i])
}

// HashSSZ hash a serie of withdrawals together given certain limit (16 for ETH1).
func (obj Withdrawals) HashSSZ(limit uint64) ([32]byte, error) { // the [32]byte is temporary
	leaves := make([][32]byte, len(obj))
	var err error
	// Compute trees of each withdrawal.
	for i, withdrawal := range obj {
		leaves[i], err = withdrawal.HashSSZ()
		if err != nil {
			return [32]byte{}, err
		}
	}
	// Compute merklized base root.
	baseRoot, err := merkle_tree.MerkleizeVector(leaves, limit)
	if err != nil {
		return [32]byte{}, err
	}
	// Mix with length
	return merkle_tree.ArraysRoot([][32]byte{
		baseRoot,
		merkle_tree.Uint64Root(uint64(len(obj))),
	}, 2)
}
