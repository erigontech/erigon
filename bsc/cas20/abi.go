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

package cas20

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/vm"
)

// --- ABI helpers ------------------------------------------------------------

func readWord(args []byte, i int) (common.Hash, error) {
	off := i * 32
	if len(args) < off+32 {
		return common.Hash{}, vm.ErrExecutionReverted
	}
	return common.BytesToHash(args[off : off+32]), nil
}

func readAddress(args []byte, i int) (common.Address, error) {
	w, err := readWord(args, i)
	if err != nil {
		return common.Address{}, err
	}
	a, ok := addressFromWord(w)
	if !ok {
		return common.Address{}, vm.ErrExecutionReverted
	}
	return a, nil
}

// wordFitsIn is the check Solidity's external decoder makes for every type
// narrower than a word: dirty high bytes are a malformed encoding, not a value.
func wordFitsIn(w common.Hash, n int) bool {
	for _, b := range w[:32-n] {
		if b != 0 {
			return false
		}
	}
	return true
}

func u64FromWord(w common.Hash) (uint64, bool) {
	if !wordFitsIn(w, 8) {
		return 0, false
	}
	return new(uint256.Int).SetBytes(w[:]).Uint64(), true
}

func addressFromWord(w common.Hash) (common.Address, bool) {
	if !wordFitsIn(w, 20) {
		return common.Address{}, false
	}
	return common.BytesToAddress(w[:]), true
}

func readU64(args []byte, i int) (uint64, error) {
	w, err := readWord(args, i)
	if err != nil {
		return 0, err
	}
	v, ok := u64FromWord(w)
	if !ok {
		return 0, vm.ErrExecutionReverted
	}
	return v, nil
}

func readU256(args []byte, i int) (*uint256.Int, error) {
	w, err := readWord(args, i)
	if err != nil {
		return nil, err
	}
	return new(uint256.Int).SetBytes(w[:]), nil
}

func encU256(v *uint256.Int) []byte {
	b := v.Bytes32()
	return b[:]
}

func encBool(b bool) []byte {
	out := make([]byte, 32)
	if b {
		out[31] = 1
	}
	return out
}

func encString(s string) []byte { return encodeTuple(abiString(s)) }

// --- ABI encoding primitives ------------------------------------------------

type abiPart struct {
	word    common.Hash
	dynamic bool
	tail    []byte
}

func abiWord(w common.Hash) abiPart { return abiPart{word: w} }

func abiBytes(b []byte) abiPart {
	padded := (len(b) + 31) / 32 * 32
	tail := make([]byte, 32+padded)
	l := uint256.NewInt(uint64(len(b))).Bytes32()
	copy(tail[:32], l[:])
	copy(tail[32:], b)
	return abiPart{dynamic: true, tail: tail}
}

func abiString(s string) abiPart { return abiBytes([]byte(s)) }

func abiWordArray(words []common.Hash) abiPart {
	tail := make([]byte, 0, 32*(len(words)+1))
	l := uint256.NewInt(uint64(len(words))).Bytes32()
	tail = append(tail, l[:]...)
	for _, w := range words {
		tail = append(tail, w[:]...)
	}
	return abiPart{dynamic: true, tail: tail}
}

func encodeTuple(parts ...abiPart) []byte {
	head := make([]byte, 0, 32*len(parts))
	tail := make([]byte, 0)
	tailStart := uint64(32 * len(parts))
	for _, p := range parts {
		if !p.dynamic {
			head = append(head, p.word[:]...)
			continue
		}
		off := uint256.NewInt(tailStart + uint64(len(tail))).Bytes32()
		head = append(head, off[:]...)
		tail = append(tail, p.tail...)
	}
	return append(head, tail...)
}

// abi.encode of one dynamic struct wraps it in a one-element tuple, so the
// result opens with an offset word (0x20) before the struct's own head/tail.
func abiEncodeStruct(members ...abiPart) []byte {
	return encodeTuple(abiPart{dynamic: true, tail: encodeTuple(members...)})
}

func readBytesArg(args []byte, argIndex int) ([]byte, error) {
	s, err := readStringArg(args, argIndex)
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

func readBytesArray(args []byte, argIndex int) ([][]byte, error) {
	L := uint64(len(args))
	base, ok := wordU64(args, uint64(argIndex)*32)
	if !ok || base > L || L-base < 32 {
		return nil, vm.ErrExecutionReverted
	}
	n, ok2 := wordU64(args, base)
	if !ok2 {
		return nil, vm.ErrExecutionReverted
	}
	arrData := base + 32
	if n > (L-arrData)/32 {
		return nil, vm.ErrExecutionReverted
	}
	out := make([][]byte, n)
	for i := range n {
		elemOff, ok2 := wordU64(args, arrData+i*32)
		if !ok2 {
			return nil, vm.ErrExecutionReverted
		}
		pos := arrData + elemOff
		if elemOff > L-arrData || pos > L || L-pos < 32 {
			return nil, vm.ErrExecutionReverted
		}
		elemLen, ok3 := wordU64(args, pos)
		if !ok3 {
			return nil, vm.ErrExecutionReverted
		}
		start := pos + 32
		if elemLen > L-start {
			return nil, vm.ErrExecutionReverted
		}
		out[i] = args[start : start+elemLen]
	}
	return out, nil
}

// An offset or length with dirty high bits is a malformed encoding, not a large number.
func wordU64(args []byte, pos uint64) (uint64, bool) {
	if pos > uint64(len(args)) || uint64(len(args))-pos < 32 {
		return 0, false
	}
	for _, b := range args[pos : pos+24] {
		if b != 0 {
			return 0, false
		}
	}
	return new(uint256.Int).SetBytes(args[pos+24 : pos+32]).Uint64(), true
}

func readStringArg(args []byte, argIndex int) (string, error) {
	L := uint64(len(args))
	off, ok := wordU64(args, uint64(argIndex)*32)
	if !ok || off > L || L-off < 32 {
		return "", vm.ErrExecutionReverted
	}
	n, ok2 := wordU64(args, off)
	if !ok2 {
		return "", vm.ErrExecutionReverted
	}
	dataPos := off + 32
	if n > L-dataPos {
		return "", vm.ErrExecutionReverted
	}
	return string(args[dataPos : dataPos+n]), nil
}

func readWordArray(args []byte, argIndex int) ([]common.Hash, error) {
	L := uint64(len(args))
	base, ok := wordU64(args, uint64(argIndex)*32)
	if !ok || base > L || L-base < 32 {
		return nil, vm.ErrExecutionReverted
	}
	n, ok2 := wordU64(args, base)
	if !ok2 {
		return nil, vm.ErrExecutionReverted
	}
	dataPos := base + 32
	if n > (L-dataPos)/32 {
		return nil, vm.ErrExecutionReverted
	}
	out := make([]common.Hash, n)
	for i := range n {
		out[i] = common.BytesToHash(args[dataPos+i*32 : dataPos+i*32+32])
	}
	return out, nil
}

// --- ABI: dynamic uint8[] ---------------------------------------------------

func readUint8Array(args []byte) ([]uint8, error) {
	L := uint64(len(args))
	off, ok := wordU64(args, 0)
	if !ok || off > L || L-off < 32 {
		return nil, vm.ErrExecutionReverted
	}
	n, ok := wordU64(args, off)
	if !ok {
		return nil, vm.ErrExecutionReverted
	}
	dataPos := off + 32
	if n > (L-dataPos)/32 {
		return nil, vm.ErrExecutionReverted
	}
	out := make([]uint8, n)
	for i := range n {
		// Byte-addressed: the caller-supplied head offset need not be 32-aligned.
		v, ok := wordU64(args, dataPos+i*32)
		if !ok || v > 0xff {
			return nil, vm.ErrExecutionReverted
		}
		out[i] = byte(v)
	}
	return out, nil
}
