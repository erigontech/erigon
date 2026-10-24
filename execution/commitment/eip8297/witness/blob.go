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

package witness

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

const (
	pbinLeafTag    = byte(0x00)
	pbinBranchTag  = byte(0x01)
	pbinGroupTag   = byte(0x02)
	pbinBitmapSize = 32
)

var (
	errPBinInvalidBlob = errors.New("pbin witness: invalid blob")
	errPBinInvalidStem = errors.New("pbin witness: invalid stem")
)

type PBinLeaf struct {
	Key   []byte
	Value []byte
}

type PBinBranch struct {
	Prefix eip8297.Bitpath
	Left   common.Hash
	Right  common.Hash
}

type PBinGroup struct {
	Position uint16
	Stem     []byte
	Subs     []byte
	Values   [][]byte
}

type PBinDecodedBlob struct {
	Leaf   *PBinLeaf
	Branch *PBinBranch
	Group  *PBinGroup
}

func PBinEncodeLeaf(key, value []byte) ([]byte, error) {
	if len(value) != eip8297.ValueLength {
		return nil, fmt.Errorf("pbin witness: leaf value has length %d, want %d", len(value), eip8297.ValueLength)
	}
	if err := pbinValidateKey(key); err != nil {
		return nil, err
	}
	return eip8297.LeafPreimage(nil, key, value), nil
}

func PBinEncodeBranch(prefix *eip8297.Bitpath, left, right *common.Hash) ([]byte, error) {
	if prefix == nil {
		return nil, errors.New("pbin witness: nil branch prefix")
	}
	if left == nil || right == nil || *left == (common.Hash{}) || *right == (common.Hash{}) {
		return nil, errors.New("pbin witness: branch child hash is empty")
	}
	if prefix.BitLen < 0 || prefix.BitLen > eip8297.MaxPathBits {
		return nil, fmt.Errorf("pbin witness: branch prefix has %d bits", prefix.BitLen)
	}
	return eip8297.BranchPreimage(nil, prefix, left, right), nil
}

func PBinEncodeGroup(group PBinGroup) ([]byte, error) {
	if len(group.Subs) < 2 {
		return nil, errors.New("pbin witness: group needs at least two values")
	}
	if len(group.Subs) != len(group.Values) {
		return nil, errors.New("pbin witness: group bitmap/value count mismatch")
	}
	if err := pbinValidateStem(group.Stem); err != nil {
		return nil, err
	}
	if int(group.Position) > len(group.Stem)*8 {
		return nil, fmt.Errorf("pbin witness: group position %d exceeds stem bits %d", group.Position, len(group.Stem)*8)
	}
	for i, sub := range group.Subs {
		if i > 0 && group.Subs[i-1] >= sub {
			return nil, errors.New("pbin witness: group sub-indices are not strictly increasing")
		}
		if len(group.Values[i]) != eip8297.ValueLength {
			return nil, fmt.Errorf("pbin witness: group value has length %d, want %d", len(group.Values[i]), eip8297.ValueLength)
		}
	}
	blob := make([]byte, 4+len(group.Stem)+pbinBitmapSize+len(group.Values)*eip8297.ValueLength)
	blob[0] = pbinGroupTag
	binary.BigEndian.PutUint16(blob[1:3], group.Position)
	blob[3] = byte(len(group.Stem))
	copy(blob[4:], group.Stem)
	bitmap := blob[4+len(group.Stem) : 4+len(group.Stem)+pbinBitmapSize]
	valuesAt := 4 + len(group.Stem) + pbinBitmapSize
	for i, sub := range group.Subs {
		bitmap[sub>>3] |= 1 << (7 - sub&7)
		copy(blob[valuesAt+i*eip8297.ValueLength:], group.Values[i])
	}
	return blob, nil
}

func PBinDecodeBlob(blob []byte) (PBinDecodedBlob, error) {
	if len(blob) == 0 {
		return PBinDecodedBlob{}, nil
	}
	switch blob[0] {
	case pbinLeafTag:
		keyLen := len(blob) - 1 - eip8297.ValueLength
		if err := pbinValidateKeyLength(keyLen); err != nil {
			return PBinDecodedBlob{}, err
		}
		if err := pbinValidateStem(blob[1 : 1+keyLen-1]); err != nil {
			return PBinDecodedBlob{}, err
		}
		key := append([]byte(nil), blob[1:1+keyLen]...)
		value := append([]byte(nil), blob[1+keyLen:]...)
		return PBinDecodedBlob{Leaf: &PBinLeaf{Key: key, Value: value}}, nil
	case pbinBranchTag:
		prefix, consumed, err := pbinDecodeBitPrefix(blob[1:])
		if err != nil {
			return PBinDecodedBlob{}, err
		}
		rest := blob[1+consumed:]
		if len(rest) != 2*len(common.Hash{}) {
			return PBinDecodedBlob{}, errPBinInvalidBlob
		}
		var left, right common.Hash
		copy(left[:], rest[:len(common.Hash{})])
		copy(right[:], rest[len(common.Hash{}):])
		if left == (common.Hash{}) || right == (common.Hash{}) {
			return PBinDecodedBlob{}, errors.New("pbin witness: branch child hash is empty")
		}
		return PBinDecodedBlob{Branch: &PBinBranch{Prefix: prefix, Left: left, Right: right}}, nil
	case pbinGroupTag:
		group, err := pbinDecodeGroup(blob)
		if err != nil {
			return PBinDecodedBlob{}, err
		}
		return PBinDecodedBlob{Group: &group}, nil
	default:
		return PBinDecodedBlob{}, fmt.Errorf("pbin witness: unknown blob tag %#x", blob[0])
	}
}

func PBinHashBlob(blob []byte) (common.Hash, error) {
	if len(blob) == 0 {
		return common.Hash{}, nil
	}
	decoded, err := PBinDecodeBlob(blob)
	if err != nil {
		return common.Hash{}, err
	}
	if decoded.Group != nil {
		return pbinFoldGroup(decoded.Group), nil
	}
	return eip8297.HashBytes(blob), nil
}

func PBinPath(walk *eip8297.Bitpath) []byte {
	if walk == nil || walk.BitLen == 0 {
		return nil
	}
	return eip8297.AppendBitPrefix(nil, walk)
}

func pbinValidateKey(key []byte) error {
	if err := pbinValidateKeyLength(len(key)); err != nil {
		return err
	}
	return pbinValidateStem(key[:len(key)-1])
}

func pbinValidateKeyLength(length int) error {
	if length != eip8297.AccountKeyLength && length != eip8297.StorageKeyLength {
		return fmt.Errorf("pbin witness: key length %d is not an account or storage key", length)
	}
	return nil
}

func pbinValidateStem(stem []byte) error {
	if len(stem) == 0 {
		return errPBinInvalidStem
	}
	want, ok := eip8297.ZoneKeyLength(stem[0])
	if !ok || len(stem) != want-1 {
		return fmt.Errorf("%w: zone %#x with length %d", errPBinInvalidStem, stem[0], len(stem))
	}
	return nil
}

func pbinDecodeBitPrefix(blob []byte) (eip8297.Bitpath, int, error) {
	if len(blob) < 2 {
		return eip8297.Bitpath{}, 0, errPBinInvalidBlob
	}
	bitLen := int(binary.BigEndian.Uint16(blob))
	if bitLen > eip8297.MaxPathBits {
		return eip8297.Bitpath{}, 0, fmt.Errorf("pbin witness: branch prefix has %d bits", bitLen)
	}
	packedLen := (bitLen + 7) / 8
	if len(blob) < 2+packedLen {
		return eip8297.Bitpath{}, 0, errPBinInvalidBlob
	}
	packed := blob[2 : 2+packedLen]
	if bitLen%8 != 0 && packedLen > 0 && packed[packedLen-1]&((1<<uint(8-bitLen%8))-1) != 0 {
		return eip8297.Bitpath{}, 0, errors.New("pbin witness: non-canonical branch prefix padding")
	}
	return eip8297.PathFromBits(packed, int16(bitLen)), 2 + packedLen, nil
}

func pbinDecodeGroup(blob []byte) (PBinGroup, error) {
	if len(blob) < 4 {
		return PBinGroup{}, errPBinInvalidBlob
	}
	position := binary.BigEndian.Uint16(blob[1:3])
	stemLen := int(blob[3])
	if stemLen != eip8297.AccountKeyLength-1 && stemLen != eip8297.StorageKeyLength-1 {
		return PBinGroup{}, fmt.Errorf("pbin witness: stem length %d is not valid", stemLen)
	}
	if int(position) > stemLen*8 {
		return PBinGroup{}, fmt.Errorf("pbin witness: group position %d exceeds stem bits %d", position, stemLen*8)
	}
	base := 4 + stemLen + pbinBitmapSize
	if len(blob) < base {
		return PBinGroup{}, errPBinInvalidBlob
	}
	stem := blob[4 : 4+stemLen]
	if err := pbinValidateStem(stem); err != nil {
		return PBinGroup{}, err
	}
	bitmap := blob[4+stemLen : base]
	k := 0
	for _, value := range bitmap {
		k += bits.OnesCount8(value)
	}
	if k < 2 || len(blob) != base+k*eip8297.ValueLength {
		return PBinGroup{}, errors.New("pbin witness: bitmap/value count mismatch")
	}
	group := PBinGroup{Position: position, Stem: append([]byte(nil), stem...), Subs: make([]byte, 0, k), Values: make([][]byte, 0, k)}
	values := blob[base:]
	for sub := range 256 {
		if bitmap[sub>>3]&(1<<(7-uint(sub)&7)) == 0 {
			continue
		}
		group.Subs = append(group.Subs, byte(sub))
		group.Values = append(group.Values, append([]byte(nil), values[:eip8297.ValueLength]...))
		values = values[eip8297.ValueLength:]
	}
	return group, nil
}

func pbinFoldGroup(group *PBinGroup) common.Hash {
	return pbinFoldRange(group, 0, len(group.Subs), 0, int(group.Position), len(group.Stem)*8)
}

func pbinFoldRange(group *PBinGroup, start, end, from, extraLo, extraHi int) common.Hash {
	if end-start == 1 {
		key := append(append([]byte(nil), group.Stem...), group.Subs[start])
		return eip8297.HashBytes(eip8297.LeafPreimage(nil, key, group.Values[start]))
	}
	b := 8 - bits.Len8(group.Subs[start]^group.Subs[end-1])
	middle := start + 1
	for middle < end && group.Subs[middle]>>(7-b)&1 == 0 {
		middle++
	}
	var prefix eip8297.Bitpath
	if extraHi > extraLo {
		stemPath := eip8297.PathFromBytes(group.Stem)
		prefix = stemPath.Slice(int16(extraLo), int16(extraHi))
		for bit := from; bit < b; bit++ {
			prefix.AppendBit(uint64(group.Subs[start] >> (7 - bit) & 1))
		}
	} else {
		subPath := eip8297.PathFromBytes([]byte{group.Subs[start]})
		prefix = subPath.Slice(int16(from), int16(b))
	}
	left := pbinFoldRange(group, start, middle, b+1, 0, 0)
	right := pbinFoldRange(group, middle, end, b+1, 0, 0)
	return eip8297.HashBytes(eip8297.BranchPreimage(nil, &prefix, &left, &right))
}
