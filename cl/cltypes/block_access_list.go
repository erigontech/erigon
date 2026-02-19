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

package cltypes

import (
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// BlockAccessList wraps EIP-7928 block_access_list bytes.
// The payload stores RLP-encoded block access list bytes as a byte list.
type BlockAccessList struct {
	data     []byte
	maxBytes uint64
}

func NewBlockAccessList(maxBytes uint64) *BlockAccessList {
	if maxBytes == 0 {
		maxBytes = clparams.MainnetBeaconConfig.MaxBytesPerTransaction
	}
	return &BlockAccessList{maxBytes: maxBytes}
}

func (b *BlockAccessList) Copy() *BlockAccessList {
	if b == nil {
		return nil
	}
	out := NewBlockAccessList(b.maxBytes)
	out.data = common.Copy(b.data)
	return out
}

func (*BlockAccessList) Static() bool {
	return false
}

func (b *BlockAccessList) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, b.data...), nil
}

func (b *BlockAccessList) DecodeSSZ(buf []byte, _ int) error {
	if uint64(len(buf)) > b.maxBytes {
		return fmt.Errorf("block access list size %d exceeds max %d", len(buf), b.maxBytes)
	}
	b.data = common.Copy(buf)
	return nil
}

func (b *BlockAccessList) EncodingSizeSSZ() int {
	return len(b.data)
}

func (b *BlockAccessList) HashSSZ() ([32]byte, error) {
	if uint64(len(b.data)) > b.maxBytes {
		return [32]byte{}, fmt.Errorf("block access list size %d exceeds max %d", len(b.data), b.maxBytes)
	}

	chunks := make([][32]byte, (len(b.data)+31)/32)
	for i := range chunks {
		start := i * 32
		end := start + 32
		if end > len(b.data) {
			end = len(b.data)
		}
		copy(chunks[i][:], b.data[start:end])
	}

	limitChunks := (b.maxBytes + 31) / 32
	baseRoot, err := merkle_tree.MerkleizeVector(chunks, limitChunks)
	if err != nil {
		return [32]byte{}, err
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(len(b.data)))
	return utils.Sha256(baseRoot[:], lengthRoot[:]), nil
}

func (b *BlockAccessList) Bytes() []byte {
	return common.Copy(b.data)
}

func (b *BlockAccessList) SetBytes(buf []byte) error {
	if uint64(len(buf)) > b.maxBytes {
		return fmt.Errorf("block access list size %d exceeds max %d", len(buf), b.maxBytes)
	}
	b.data = common.Copy(buf)
	return nil
}

func (b *BlockAccessList) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutil.Bytes(b.data))
}

func (b *BlockAccessList) UnmarshalJSON(buf []byte) error {
	var data hexutil.Bytes
	if err := json.Unmarshal(buf, &data); err != nil {
		return err
	}
	return b.SetBytes(data)
}
