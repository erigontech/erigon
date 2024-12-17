package types

import (
	"errors"
	"fmt"
	"io"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/holiman/uint256"
)

type baseTypeRLP interface {
	encodingSize() int
	encodeRLP(w io.Writer, b []byte) error
	decodeRLP(s *rlp.Stream) error
}

type uintRLP uint64
type bigIntRLP big.Int
type uint256RLP uint256.Int
type addressRLP libcommon.Address
type hashRLP libcommon.Hash
type stringRLP []byte

// --------------- uintRLP ---------------

func (obj uintRLP) encodingSize() (size int) {
	return rlp.IntLenExcludingHead(uint64(obj)) + 1
}

func (obj *uintRLP) encodeRLP(w io.Writer, b []byte) error {
	return rlp.EncodeInt(uint64(*obj), w, b)
}

func (obj *uintRLP) decodeRLP(s *rlp.Stream) error {
	if n, err := s.Uint(); err != nil {
		return err
	} else {
		*obj = uintRLP(n)
	}
	return nil
}

// --------------- bigIntRLP ---------------

func (obj bigIntRLP) encodingSize() (size int) {
	bi := big.Int(obj)
	return rlp.BigIntLenExcludingHead(&bi) + 1
}

func (obj *bigIntRLP) encodeRLP(w io.Writer, b []byte) error {
	bi := big.Int(*obj)
	return rlp.EncodeBigInt(&bi, w, b)
}

func (obj *bigIntRLP) decodeRLP(s *rlp.Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return err
	} else {
		bi := new(big.Int).SetBytes(b)
		*obj = bigIntRLP(*bi)
	}
	return nil
}

// --------------- uint256RLP ---------------

func (obj uint256RLP) encodingSize() (size int) {
	u256 := uint256.Int(obj)
	return rlp.Uint256LenExcludingHead(&u256) + 1
}

func (obj *uint256RLP) encodeRLP(w io.Writer, b []byte) error {
	u256 := uint256.Int(*obj)
	return rlp.EncodeUint256(&u256, w, b)
}

func (obj *uint256RLP) decodeRLP(s *rlp.Stream) error {
	if b, err := s.Uint256Bytes(); err != nil {
		return err
	} else {
		bi := new(uint256.Int).SetBytes(b)
		*obj = uint256RLP(*bi)
	}
	return nil
}

// --------------- addressRLP ---------------

func (obj addressRLP) encodingSize() (size int) {
	return 21
}

func (obj *addressRLP) encodeRLP(w io.Writer, b []byte) error {
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(obj[:]); err != nil {
		return err
	}
	return nil
}

func (obj *addressRLP) decodeRLP(s *rlp.Stream) error {
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

// --------------- hashRLP ---------------

func (obj hashRLP) encodingSize() (size int) {
	return 33
}

func (obj *hashRLP) encodeRLP(w io.Writer, b []byte) error {
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(obj[:]); err != nil {
		return err
	}
	return nil
}

func (obj *hashRLP) decodeRLP(s *rlp.Stream) error {
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

// --------------- stringRLP ---------------

func (obj stringRLP) encodingSize() (size int) {
	return rlp.StringLen(obj)
}

func (obj *stringRLP) encodeRLP(w io.Writer, b []byte) error {
	return rlp.EncodeString(*obj, w, b)
}

func (obj *stringRLP) decodeRLP(s *rlp.Stream) error {
	if b, err := s.Bytes(); err != nil {
		return fmt.Errorf("read Hash: %w", err)
	} else {
		copy(*obj, b)
	}
	return nil
}

type TestingStruct struct {
	_uintRLP    *uint64
	_bigIntRLP  big.Int
	_uint256RLP uint256.Int
	_addressRLP libcommon.Address
	_hashRLP    libcommon.Hash
	_stringRLP  []byte
}

func (t *TestingStruct) EncodingSize() (size int) {
	_u64 := uintRLP(*t._uintRLP)
	size += _u64.encodingSize()
	size += bigIntRLP(t._bigIntRLP).encodingSize()
	size += uint256RLP(t._uint256RLP).encodingSize()
	size += addressRLP(t._addressRLP).encodingSize()
	size += hashRLP(t._hashRLP).encodingSize()
	size += stringRLP(t._stringRLP).encodingSize()
	return
}

func (t *TestingStruct) EncodeRLP(w io.Writer) error {
	var b [32]byte
	if err := rlp.EncodeStructSizePrefix(t.EncodingSize(), w, b[:]); err != nil {
		return err
	}
	_u64 := uintRLP(*t._uintRLP)
	if err := _u64.encodeRLP(w, b[:]); err != nil {
		return err
	}
	_bi := bigIntRLP(t._bigIntRLP)
	if err := _bi.encodeRLP(w, b[:]); err != nil {
		return err
	}
	_u256 := uint256RLP(t._uint256RLP)
	if err := _u256.encodeRLP(w, b[:]); err != nil {
		return err
	}
	_addr := addressRLP(t._addressRLP)
	if err := _addr.encodeRLP(w, b[:]); err != nil {
		return err
	}
	_hash := hashRLP(t._hashRLP)
	if err := _hash.encodeRLP(w, b[:]); err != nil {
		return err
	}
	_stringRLP := stringRLP(t._stringRLP)
	if err := _stringRLP.encodeRLP(w, b[:]); err != nil {
		return err
	}
	return nil
}

func (t *TestingStruct) DecodeRLP(s *rlp.Stream) error {
	var err error
	var b []byte

	if err = startList(s); err != nil {
		return err
	}

	if n, err := s.Uint(); err != nil {
		return err
	} else {
		t._uintRLP = &n
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	} else {
		t._bigIntRLP = *new(big.Int).SetBytes(b)
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	} else {
		t._uint256RLP = *new(uint256.Int).SetBytes(b)
	}

	if b, err = s.Bytes(); err != nil {
		return err
	} else {
		if len(b) != 20 {
			return fmt.Errorf("expected length 20, got: %d", len(b))
		}
		copy(t._addressRLP[:], b)
	}

	if b, err = s.Bytes(); err != nil {
		return err
	} else {
		if len(b) != 32 {
			return fmt.Errorf("expected length 32, got: %d", len(b))
		}
		copy(t._hashRLP[:], b)
	}

	if b, err = s.Bytes(); err != nil {
		return err
	} else {
		t._stringRLP = make([]byte, len(b))
		copy(t._stringRLP, b)
	}

	if err = checkListEnd(s); err != nil {
		return err
	}

	return nil
}

func startList(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	return nil
}

func checkListEnd(s *rlp.Stream) error {
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("expected list end: %w", err)
	}
	return nil
}

func checkEOL(err error) error {
	if !errors.Is(err, rlp.EOL) {
		return fmt.Errorf("expected EOL, got: %w", err)
	}
	return nil
}

// type addressSliceRLP []libcommon.Address

// func (obj addressSliceRLP) encodingSize() (size int) {
// 	return len(obj) * addressRLP{}.encodingSize()
// }

// func (obj *addressSliceRLP) encodeRLP(w io.Writer, b []byte) error {
// 	if err := rlp.EncodeStructSizePrefix(obj.encodingSize(), w, b[:]); err != nil {
// 		return err
// 	}
// 	for _, slice := range *obj {
// 		s := addressRLP(slice)
// 		if err := (&s).encodeRLP(w, b); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (obj *addressSliceRLP) decodeRLP(s *rlp.Stream) error {
// 	_, err := s.List()
// 	if err != nil {
// 		fmt.Println("THIS ERR")
// 		return err
// 	}
// 	hash := &addressRLP{}
// 	for err == nil {
// 		err = hash.decodeRLP(s)
// 		if err != nil {
// 			break
// 		}
// 		*obj = append(*obj, libcommon.Address(*hash))
// 	}
// 	if !errors.Is(err, rlp.EOL) {
// 		return fmt.Errorf("hash slice, expected EOL, got: %w", err)
// 	}
// 	if err = s.ListEnd(); err != nil {
// 		return fmt.Errorf("close hashSlice: %w", err)
// 	}
// 	return nil
// }

// type hashSliceRLP []libcommon.Hash

// func (obj hashSliceRLP) encodingSize() (size int) {
// 	return len(obj) * hashRLP{}.encodingSize()
// }

// func (obj *hashSliceRLP) encodeRLP(w io.Writer, b []byte) error {
// 	if err := rlp.EncodeStructSizePrefix(obj.encodingSize(), w, b[:]); err != nil {
// 		return err
// 	}
// 	for _, slice := range *obj {
// 		s := hashRLP(slice)
// 		if err := (s).encodeRLP(w, b); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (obj *hashSliceRLP) decodeRLP(s *rlp.Stream) error {
// 	_, err := s.List()
// 	if err != nil {
// 		fmt.Println("THIS ERR")
// 		return err
// 	}
// 	hash := hashRLP{}
// 	for err == nil {
// 		err = hash.decodeRLP(s)
// 		if err != nil {
// 			break
// 		}
// 		*obj = append(*obj, libcommon.Hash(hash))
// 	}
// 	if !errors.Is(err, rlp.EOL) {
// 		return fmt.Errorf("hash slice, expected EOL, got: %w", err)
// 	}
// 	if err = s.ListEnd(); err != nil {
// 		return fmt.Errorf("close hashSlice: %w", err)
// 	}
// 	return nil
// }

// type AccessTupleRLP struct {
// 	Address     libcommon.Address `json:"address"`
// 	StorageKeys []libcommon.Hash  `json:"storageKeys"`
// }

// // func AccessTupleAsAccessTupleRLP(a AccessTuple) *AccessTupleRLP {
// // 	return &AccessTupleRLP{
// // 		address:     addressRLP(a.Address),
// // 		storageKeys: hashSliceRLP(a.StorageKeys),
// // 	}
// // }

// func (obj *AccessTupleRLP) encodingSize() (size int) {
// 	size += addressRLP(obj.Address).encodingSize()
// 	sliceSize := hashSliceRLP(obj.StorageKeys).encodingSize()
// 	size += rlp.ListPrefixLen(sliceSize) + sliceSize
// 	return
// }

// func (obj *AccessTupleRLP) encodeRLP(w io.Writer, b []byte) error {
// 	if err := rlp.EncodeStructSizePrefix(obj.encodingSize(), w, b[:]); err != nil {
// 		return err
// 	}
// 	s := addressRLP(obj.Address)
// 	if err := s.encodeRLP(w, b); err != nil {
// 		return err
// 	}
// 	ss := hashSliceRLP(obj.StorageKeys)
// 	if err := ss.encodeRLP(w, b); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (obj *AccessTupleRLP) decodeRLP(s *rlp.Stream) error {
// 	_, err := s.List()
// 	if err != nil {
// 		fmt.Println("THIS ERR 11")
// 		return err
// 	}
// 	address := addressRLP{}
// 	if err := address.decodeRLP(s); err != nil {
// 		return err
// 	}
// 	hashSlice := hashSliceRLP{}
// 	if err := hashSlice.decodeRLP(s); err != nil {
// 		return err
// 	}
// 	if err = s.ListEnd(); err != nil {
// 		return fmt.Errorf("close hashSlice: %w", err)
// 	}
// 	obj.Address = libcommon.Address(address)
// 	obj.StorageKeys = hashSlice
// 	return nil
// }

// type AccessListRLP []AccessTuple

// func (obj AccessListRLP) encodingSize() (size int) {
// 	for _, tup := range obj {
// 		ttup := AccessTupleRLP(tup)
// 		tsize := ttup.encodingSize()
// 		size += rlp.ListPrefixLen(tsize) + tsize
// 	}
// 	return
// }

// func (obj *AccessListRLP) encodeRLP(w io.Writer, b []byte) error {
// 	if err := rlp.EncodeStructSizePrefix(obj.encodingSize(), w, b[:]); err != nil {
// 		return err
// 	}
// 	for _, slice := range *obj {
// 		s := AccessTupleRLP(slice)
// 		if err := (s).encodeRLP(w, b); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (obj *AccessListRLP) decodeRLP(s *rlp.Stream) error {
// 	_, err := s.List()
// 	if err != nil {
// 		return err
// 	}
// 	tup := AccessTupleRLP{}
// 	for err == nil {
// 		err = tup.decodeRLP(s)
// 		if err != nil {
// 			break
// 		}
// 		*obj = append(*obj, AccessTuple(tup))
// 	}
// 	if !errors.Is(err, rlp.EOL) {
// 		return fmt.Errorf("hash slice, expected EOL, got: %w", err)
// 	}
// 	if err = s.ListEnd(); err != nil {
// 		return fmt.Errorf("close hashSlice: %w", err)
// 	}
// 	return nil
// }
