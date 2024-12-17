package rlp

import (
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"
)

type BaseTypeRLP interface {
	encodingSize() int
	encodeRLP(w io.Writer, b []byte) error
	decodeRLP(s *Stream) error
}

func BaseTypeEncodingSize(typ BaseTypeRLP) int {
	return typ.encodingSize()
}

func BaseTypeEncodeRLP(typ BaseTypeRLP, w io.Writer, b []byte) error {
	return typ.encodeRLP(w, b)
}

func BaseTypeDecodeRLP(typ BaseTypeRLP, s *Stream) error {
	return typ.decodeRLP(s)
}

type Uint uint64
type UintPtr struct{ *uint64 }
type BigInt big.Int
type BigIntPtr struct{ *big.Int }
type Uint256 uint256.Int
type Uint256Ptr struct{ *uint256.Int }
type Bytes8 [8]byte
type Bytes20 [20]byte
type Bytes32 [32]byte
type Bytes256 [256]byte
type Bytes []byte

// --------------- Uint ---------------

func (obj Uint) encodingSize() (size int) {
	return IntLenExcludingHead(uint64(obj)) + 1
}

func (obj *Uint) encodeRLP(w io.Writer, b []byte) error {
	return EncodeInt(uint64(*obj), w, b)
}

func (obj *Uint) decodeRLP(s Stream) error {
	if n, err := s.Uint(); err != nil {
		return err
	} else {
		*obj = Uint(n)
	}
	return nil
}

// --------------- UintPtr ---------------

func (obj *UintPtr) encodingSize() (size int) {
	return IntLenExcludingHead(*obj.uint64) + 1
}

func (obj *UintPtr) encodeRLP(w io.Writer, b []byte) error {
	return EncodeInt(uint64(*obj.uint64), w, b)
}

func (obj *UintPtr) decodeRLP(s Stream) error {
	if n, err := s.Uint(); err != nil {
		return err
	} else {
		*obj = UintPtr{&n}
	}
	return nil
}

// --------------- BigInt ---------------

func (obj BigInt) encodingSize() (size int) {
	bi := big.Int(obj)
	return BigIntLenExcludingHead(&bi) + 1
}

func (obj *BigInt) encodeRLP(w io.Writer, b []byte) error {
	bi := big.Int(*obj)
	return EncodeBigInt(&bi, w, b)
}

func (obj *BigInt) decodeRLP(s Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return err
	} else {
		bi := new(big.Int).SetBytes(b)
		*obj = BigInt(*bi)
	}
	return nil
}

// --------------- BigIntPtr ---------------

func (obj *BigIntPtr) encodingSize() (size int) {
	return BigIntLenExcludingHead(obj.Int) + 1
}

func (obj *BigIntPtr) encodeRLP(w io.Writer, b []byte) error {
	return EncodeBigInt(obj.Int, w, b)
}

func (obj *BigIntPtr) decodeRLP(s Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return err
	} else {
		*obj = BigIntPtr{new(big.Int).SetBytes(b)}
	}
	return nil
}

// --------------- Uint256 ---------------

func (obj Uint256) encodingSize() (size int) {
	u256 := uint256.Int(obj)
	return Uint256LenExcludingHead(&u256) + 1
}

func (obj *Uint256) encodeRLP(w io.Writer, b []byte) error {
	u256 := uint256.Int(*obj)
	return EncodeUint256(&u256, w, b)
}

func (obj *Uint256) decodeRLP(s Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return err
	} else {
		bi := new(uint256.Int).SetBytes(b)
		*obj = Uint256(*bi)
	}
	return nil
}

// --------------- Uint256Ptr ---------------

func (obj *Uint256Ptr) encodingSize() (size int) {
	return Uint256LenExcludingHead(obj.Int) + 1
}

func (obj *Uint256Ptr) encodeRLP(w io.Writer, b []byte) error {
	return EncodeUint256(obj.Int, w, b)
}

func (obj *Uint256Ptr) decodeRLP(s Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return err
	} else {
		*obj = Uint256Ptr{new(uint256.Int).SetBytes(b)}
	}
	return nil
}

// --------------- Bytes8 ---------------

func (obj Bytes8) encodingSize() (size int) {
	return 9
}

func (obj *Bytes8) encodeRLP(w io.Writer, b []byte) error {
	b[0] = 128 + 8
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(obj[:]); err != nil {
		return err
	}
	return nil
}

func (obj *Bytes8) decodeRLP(s Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Address: %w", err)
	} else {
		if len(b) != 8 {
			return fmt.Errorf("wrong size for Address: %d", len(b))
		}
		copy(obj[:], b)
	}
	return nil
}

// --------------- Bytes20 ---------------

func (obj Bytes20) encodingSize() (size int) {
	return 21
}

func (obj *Bytes20) encodeRLP(w io.Writer, b []byte) error {
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(obj[:]); err != nil {
		return err
	}
	return nil
}

func (obj *Bytes20) decodeRLP(s Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Address: %w", err)
	} else {
		if len(b) != 20 {
			return fmt.Errorf("wrong size for Address: %d", len(b))
		}
		copy(obj[:], b)
	}
	return nil
}

// --------------- Bytes32 ---------------

func (obj Bytes32) encodingSize() (size int) {
	return 33
}

func (obj *Bytes32) encodeRLP(w io.Writer, b []byte) error {
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(obj[:]); err != nil {
		return err
	}
	return nil
}

func (obj *Bytes32) decodeRLP(s Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Hash: %w", err)
	} else {
		if len(b) != 32 {
			return fmt.Errorf("wrong size for Hash: %d", len(b))
		}
		copy(obj[:], b)
	}
	return nil
}

// --------------- Bytes256 ---------------

func (obj Bytes256) encodingSize() (size int) {
	return 259
}

func (obj *Bytes256) encodeRLP(w io.Writer, b []byte) error {
	b[0] = 183 + 2
	b[1] = 1
	b[2] = 0
	if _, err := w.Write(b[:3]); err != nil {
		return err
	}
	if _, err := w.Write(obj[:]); err != nil {
		return err
	}
	return nil
}

func (obj *Bytes256) decodeRLP(s Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Hash: %w", err)
	} else {
		if len(b) != 256 {
			return fmt.Errorf("wrong size for Hash: %d", len(b))
		}
		copy(obj[:], b)
	}
	return nil
}

// --------------- Bytes ---------------

func (obj Bytes) encodingSize() (size int) {
	return StringLen(obj)
}

func (obj *Bytes) encodeRLP(w io.Writer, b []byte) error {
	return EncodeString(*obj, w, b)
}

func (obj *Bytes) decodeRLP(s Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Hash: %w", err)
	} else {
		*obj = make([]byte, len(b))
		copy(*obj, b)
	}
	return nil
}
