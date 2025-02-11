package rlp

import (
	"fmt"
	"io"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
)

type rlpEncodable interface {
	encodingSize() int
	encodeRLP(w io.Writer, b []byte) error
}

// func EncodingSizeGeneric[T rlpEncodable](v T) int {
// 	return v.encodingSize()
// }

// func EncodeRLPGeneric[T rlpEncodable](v T, w io.Writer, b []byte) error {
// 	return v.encodeRLP(w, b)
// }

func SliceEncodingSizeGeneric[T rlpEncodable](v []T) int {
	size := 0
	for i := 0; i < len(v); i++ {
		size += v[i].encodingSize()
	}
	return size
}

func SliceEncodeRLPGeneric[T rlpEncodable](v []T, w io.Writer, b []byte) error {
	size := SliceEncodingSizeGeneric(v)
	if err := EncodeStructSizePrefix(size, w, b); err != nil {
		return err
	}
	for i := 0; i < len(v); i++ {
		if err := v[i].encodeRLP(w, b); err != nil {
			return err
		}
	}
	return nil
}

type Bool bool
type Uint uint64
type BigInt big.Int
type Uint256 uint256.Int
type Bytes8 [8]byte
type Bytes20 [20]byte
type Bytes32 [32]byte
type Bytes256 [256]byte
type Bytes []byte

// -- bool

func (v *Bool) encodingSize() (size int) {
	return 1
}

func (v *Bool) encodeRLP(w io.Writer, b []byte) error {
	if *v {
		b[0] = 0x01
	} else {
		b[0] = 0x80
	}
	_, err := w.Write(b[:1])
	return err
}

func (v *Bool) decodeRLP(s *Stream) error {
	if s, err := s.Bool(); err != nil {
		return fmt.Errorf("read Bool: %w", err)
	} else {
		*v = Bool(s)
		return nil
	}
}

// -- uint64

func (v *Uint) encodingSize() (size int) {
	return IntLenExcludingHead(uint64(*v)) + 1
}

func (v *Uint) encodeRLP(w io.Writer, b []byte) error {
	return EncodeInt(uint64(*v), w, b)
}

func (v *Uint) decodeRLP(s *Stream) error {
	if n, err := s.Uint(); err != nil {
		return fmt.Errorf("read Uint: %w", err)
	} else {
		*v = Uint(n)
		return nil
	}
}

// -- bigInt

func (v *BigInt) encodingSize() (size int) {
	if v != nil {
		return BigIntLenExcludingHead((*big.Int)(v)) + 1
	}
	return 1
}

func (v *BigInt) encodeRLP(w io.Writer, b []byte) error {
	return EncodeBigInt((*big.Int)(v), w, b)
}

func (v *BigInt) decodeRLP(s *Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read BigInt: %w", err)
	} else {
		*v = (BigInt)(*new(big.Int).SetBytes(b))
		return nil
	}
}

// -- uint256

func (v *Uint256) encodingSize() (size int) {
	if v != nil {
		return Uint256LenExcludingHead((*uint256.Int)(v)) + 1
	}
	return 1
}

func (v *Uint256) encodeRLP(w io.Writer, b []byte) error {
	return EncodeUint256((*uint256.Int)(v), w, b)
}

func (v *Uint256) decodeRLP(s *Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read BigInt: %w", err)
	} else {
		*v = (Uint256)(*new(uint256.Int).SetBytes(b))
		return nil
	}
}

// -- address

func (v *Bytes20) encodingSize() (size int) {
	if v != nil {
		return 21
	}
	return 1
}

func (v *Bytes20) encodeRLP(w io.Writer, b []byte) error {
	return EncodeOptionalAddress((*common.Address)(v), w, b[:])
}

func (v *Bytes20) decodeRLP(s *Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Address: %w", err)
	} else if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for Address: %d", len(b))
	} else if len(b) > 0 {
		copy(v[:], b)
		return nil
	} else {
		panic("RLPAdress: unhandled case")
	}
}

// -- hash

func (v *Bytes32) encodingSize() (size int) {
	if v != nil {
		return 33
	}
	return 1
}

func (v *Bytes32) encodeRLP(w io.Writer, b []byte) error {
	if v != nil {
		b[0] = 128 + 32
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		_, err := w.Write(v[:])
		return err
	}

	b[0] = 128
	_, err := w.Write(b[:1])
	return err
}

func (v *Bytes32) decodeRLP(s *Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Hash: %w", err)
	} else if len(b) > 0 && len(b) != 32 {
		return fmt.Errorf("wrong size for Hash: %d", len(b))
	} else if len(b) > 0 {
		copy(v[:], b)
		return nil
	} else {
		panic("RLPhash: unhandled case")
	}
}

// -- []byte

func (v *Bytes) encodingSize() (size int) {
	return StringLen([]byte(*v))
}

func (v *Bytes) encodeRLP(w io.Writer, b []byte) error {
	return EncodeString([]byte(*v), w, b)
}

func (v *Bytes) decodeRLP(s *Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Bytes: %w", err)
	} else if len(b) > 0 {
		cpy := make([]byte, len(b))
		copy(cpy, b)
		*v = Bytes(cpy)
		return nil
	} else {
		panic("Bytes: unhandled case")
	}
}

// func GenericEncodingSize(v interface{}) (size int) {
// 	switch v := v.(type) {
// 	case bool:
// 		return 1
// 	case int8:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case int16:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case int32:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case int64:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case int:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case uint8:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case uint16:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case uint32:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case uint64:
// 		return IntLenExcludingHead(v) + 1
// 	case uint:
// 		return IntLenExcludingHead(uint64(v)) + 1
// 	case common.Address:
// 		return 21
// 	case common.Hash:
// 		return 33
// 	default:
// 		panic("GenericEncodingSize: unhandled case")
// 	}
// }

// func GenericEncodingRLP(v interface{}, w io.Writer, b []byte) error {
// 	switch v := v.(type) {
// 	case bool:
// 		if v {
// 			b[0] = 0x01
// 		} else {
// 			b[0] = 0x80
// 		}
// 		_, err := w.Write(b[:1])
// 		return err
// 	case int8:
// 		return EncodeInt(uint64(v), w, b)
// 	case int16:
// 		return EncodeInt(uint64(v), w, b)
// 	case int32:
// 		return EncodeInt(uint64(v), w, b)
// 	case int64:
// 		return EncodeInt(uint64(v), w, b)
// 	case int:
// 		return EncodeInt(uint64(v), w, b)
// 	case uint8:
// 		return EncodeInt(uint64(v), w, b)
// 	case uint16:
// 		return EncodeInt(uint64(v), w, b)
// 	case uint32:
// 		return EncodeInt(uint64(v), w, b)
// 	case uint64:
// 		return EncodeInt(v, w, b)
// 	case uint:
// 		return EncodeInt(uint64(v), w, b)
// 	case common.Address:
// 		b[0] = 128 + 20
// 		if _, err := w.Write(b[:1]); err != nil {
// 			return err
// 		}
// 		_, err := w.Write(v[:])
// 		return err
// 	case common.Hash:
// 		b[0] = 128 + 32
// 		if _, err := w.Write(b[:1]); err != nil {
// 			return err
// 		}
// 		_, err := w.Write(v[:])
// 		return err
// 	default:
// 		panic("GenericEncodingSize: unhandled case")
// 	}
// }
