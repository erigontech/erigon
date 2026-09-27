// Copyright 2026 The Erigon Authors
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

package pbt

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var errOperationEncoding = errors.New("pbin: invalid operation encoding")

func EncodeOp(op Op) ([]byte, error) {
	if len(op.Key) > math.MaxUint16 || len(op.Drop) > math.MaxUint16 {
		return nil, errOperationEncoding
	}
	mergeLen := 1
	if op.merge != nil {
		mergeLen += 1 + 8 + 32 + 32
	}
	data := make([]byte, 2+2+len(op.Key)+len(op.Drop)+eip8297.ValueLength+mergeLen)
	binary.BigEndian.PutUint16(data[0:2], uint16(len(op.Key)))
	binary.BigEndian.PutUint16(data[2:4], uint16(len(op.Drop)))
	pos := 4
	pos += copy(data[pos:], op.Key)
	pos += copy(data[pos:], op.Drop)
	pos += copy(data[pos:], op.Value[:])
	if op.merge == nil {
		data[pos] = 0
		return data, nil
	}
	data[pos] = 1
	pos++
	data[pos] = byte(op.merge.kind)
	pos++
	binary.BigEndian.PutUint64(data[pos:pos+8], op.merge.nonce)
	pos += 8
	balance := op.merge.balance.Bytes32()
	pos += copy(data[pos:], balance[:])
	copy(data[pos:], op.merge.codeHash[:])
	return data, nil
}

func DecodeOp(data []byte) (Op, error) {
	if len(data) < 4+eip8297.ValueLength+1 {
		return Op{}, errOperationEncoding
	}
	keyLen := int(binary.BigEndian.Uint16(data[0:2]))
	dropLen := int(binary.BigEndian.Uint16(data[2:4]))
	pos := 4
	end := pos + keyLen + dropLen + eip8297.ValueLength + 1
	if end > len(data) {
		return Op{}, errOperationEncoding
	}
	op := Op{
		Key:  append([]byte(nil), data[pos:pos+keyLen]...),
		Drop: append([]byte(nil), data[pos+keyLen:pos+keyLen+dropLen]...),
	}
	pos += keyLen + dropLen
	copy(op.Value[:], data[pos:pos+eip8297.ValueLength])
	pos += eip8297.ValueLength
	switch data[pos] {
	case 0:
		if pos+1 != len(data) {
			return Op{}, errOperationEncoding
		}
	case 1:
		if pos+1+1+8+32+32 != len(data) {
			return Op{}, errOperationEncoding
		}
		pos++
		merge := &feedMerge{kind: mergeKind(data[pos])}
		pos++
		merge.nonce = binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
		merge.balance.SetBytes32(data[pos : pos+32])
		pos += 32
		copy(merge.codeHash[:], data[pos:pos+32])
		op.merge = merge
	default:
		return Op{}, errOperationEncoding
	}
	return op, nil
}
