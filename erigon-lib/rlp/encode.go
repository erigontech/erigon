// Copyright 2014 The go-ethereum Authors
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

package rlp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/bits"
	"reflect"

	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
)

// https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/
const (
	// EmptyStringCode is the RLP code for empty strings.
	EmptyStringCode = 0x80
	// EmptyListCode is the RLP code for empty lists.
	EmptyListCode = 0xC0
)

var ErrNegativeBigInt = errors.New("rlp: cannot encode negative big.Int")

var (
	// Common encoded values.
	// These are useful when implementing EncodeRLP.
	EmptyString = []byte{EmptyStringCode}
	EmptyList   = []byte{EmptyListCode}
)

// Encoder is implemented by types that require custom
// encoding rules or want to encode private fields.
type Encoder interface {
	// EncodeRLP should write the RLP encoding of its receiver to w.
	// If the implementation is a pointer method, it may also be
	// called for nil pointers.
	//
	// Implementations should generate valid RLP. The data written is
	// not verified at the moment, but a future version might. It is
	// recommended to write only a single value but writing multiple
	// values or no value at all is also permitted.
	EncodeRLP(io.Writer) error
}

// Encode writes the RLP encoding of val to w. Note that Encode may
// perform many small writes in some cases. Consider making w
// buffered.
//
// Please see package-level documentation of encoding rules.
func Encode(w io.Writer, val interface{}) error {
	if outer, ok := w.(*encBuffer); ok {
		// Encode was called by some type's EncodeRLP.
		// Avoid copying by writing to the outer encBuffer directly.
		return outer.encode(val)
	}
	eb := encBufferPool.Get().(*encBuffer)
	defer encBufferPool.Put(eb)
	eb.reset()
	if err := eb.encode(val); err != nil {
		return err
	}
	return eb.toWriter(w)
}

func Write(w io.Writer, val []byte) error {
	if outer, ok := w.(*encBuffer); ok {
		// Encode was called by some type's EncodeRLP.
		// Avoid copying by writing to the outer encBuffer directly.
		_, err := outer.Write(val)
		return err
	}

	_, err := w.Write(val)
	return err
}

// EncodeToBytes returns the RLP encoding of val.
// Please see package-level documentation for the encoding rules.
func EncodeToBytes(val interface{}) ([]byte, error) {
	eb := encBufferPool.Get().(*encBuffer)
	defer encBufferPool.Put(eb)
	eb.reset()
	if err := eb.encode(val); err != nil {
		return nil, err
	}
	return eb.toBytes(), nil
}

// EncodeToReader returns a reader from which the RLP encoding of val
// can be read. The returned size is the total size of the encoded
// data.
//
// Please see the documentation of Encode for the encoding rules.
func EncodeToReader(val interface{}) (size int, r io.Reader, err error) {
	eb := encBufferPool.Get().(*encBuffer)
	eb.reset()
	if err := eb.encode(val); err != nil {
		return 0, nil, err
	}
	return eb.size(), &encReader{buf: eb}, nil
}

type listhead struct {
	offset int // index of this header in string data
	size   int // total size of encoded data (including list headers)
}

// encode writes head to the given buffer, which must be at least
// 9 bytes long. It returns the encoded bytes.
func (head *listhead) encode(buf []byte) []byte {
	return buf[:puthead(buf, 0xC0, 0xF7, uint64(head.size))]
}

// headsize returns the size of a list or string header
// for a value of the given size.
func headsize(size uint64) int {
	if size < 56 {
		return 1
	}
	return 1 + intsize(size)
}

// puthead writes a list or string header to buf.
// buf must be at least 9 bytes long.
func puthead(buf []byte, smalltag, largetag byte, size uint64) int {
	if size < 56 {
		buf[0] = smalltag + byte(size)
		return 1
	}
	sizesize := putint(buf[1:], size)
	buf[0] = largetag + byte(sizesize)
	return sizesize + 1
}

var encoderInterface = reflect.TypeOf(new(Encoder)).Elem()

// makeWriter creates a writer function for the given type.
func makeWriter(typ reflect.Type, ts tags) (writer, error) {
	kind := typ.Kind()
	switch {
	case typ == rawValueType:
		return writeRawValue, nil
	case typ.AssignableTo(reflect.PtrTo(bigInt)):
		return writeBigIntPtr, nil
	case typ.AssignableTo(bigInt):
		return writeBigIntNoPtr, nil
	case typ.AssignableTo(reflect.PtrTo(uint256Int)):
		return writeUint256Ptr, nil
	case typ.AssignableTo(uint256Int):
		return writeUint256NoPtr, nil
	case kind == reflect.Ptr:
		return makePtrWriter(typ, ts)
	case reflect.PtrTo(typ).Implements(encoderInterface):
		return makeEncoderWriter(typ), nil
	case isUint(kind):
		return writeUint, nil
	case isInt(kind):
		return writeInt, nil
	case kind == reflect.Bool:
		return writeBool, nil
	case kind == reflect.String:
		return writeString, nil
	case kind == reflect.Slice && isByte(typ.Elem()):
		return writeBytes, nil
	case kind == reflect.Array && isByte(typ.Elem()):
		return makeByteArrayWriter(typ), nil
	case kind == reflect.Slice || kind == reflect.Array:
		return makeSliceWriter(typ, ts)
	case kind == reflect.Struct:
		return makeStructWriter(typ)
	case kind == reflect.Interface:
		return writeInterface, nil
	default:
		return nil, fmt.Errorf("rlp: type %v is not RLP-serializable", typ)
	}
}

func writeRawValue(val reflect.Value, w *encBuffer) error {
	w.str = append(w.str, val.Bytes()...)
	return nil
}

func writeUint(val reflect.Value, w *encBuffer) error {
	w.encodeUint(val.Uint())
	return nil
}

func writeInt(val reflect.Value, w *encBuffer) error {
	i := val.Int()
	if i < 0 {
		return fmt.Errorf("rlp: type %T -ve values are not RLP-serializable", val)
	}
	w.encodeUint(uint64(i))
	return nil
}

func writeBool(val reflect.Value, w *encBuffer) error {
	if val.Bool() {
		w.str = append(w.str, 0x01)
	} else {
		w.str = append(w.str, EmptyStringCode)
	}
	return nil
}

func writeBigIntPtr(val reflect.Value, w *encBuffer) error {
	ptr := val.Interface().(*big.Int)
	if ptr == nil {
		w.str = append(w.str, EmptyStringCode)
		return nil
	}
	return writeBigInt(ptr, w)
}

func writeBigIntNoPtr(val reflect.Value, w *encBuffer) error {
	i := val.Interface().(big.Int)
	return writeBigInt(&i, w)
}

// wordBytes is the number of bytes in a big.Word
const wordBytes = (32 << (uint64(^big.Word(0)) >> 63)) / 8

func writeBigInt(i *big.Int, w *encBuffer) error {
	if i.Sign() == -1 {
		return errors.New("rlp: cannot encode negative *big.Int")
	}
	bitlen := i.BitLen()
	if bitlen <= 64 {
		w.encodeUint(i.Uint64())
		return nil
	}
	// Integer is larger than 64 bits, encode from i.Bits().
	// The minimal byte length is bitlen rounded up to the next
	// multiple of 8, divided by 8.
	length := ((bitlen + 7) & -8) >> 3
	w.encodeStringHeader(length)
	w.str = append(w.str, make([]byte, length)...)
	index := length
	buf := w.str[len(w.str)-length:]
	for _, d := range i.Bits() {
		for j := 0; j < wordBytes && index > 0; j++ {
			index--
			buf[index] = byte(d)
			d >>= 8
		}
	}
	return nil
}

func writeUint256Ptr(val reflect.Value, w *encBuffer) error {
	ptr := val.Interface().(*uint256.Int)
	if ptr == nil {
		w.str = append(w.str, EmptyStringCode)
		return nil
	}
	return writeUint256(ptr, w)
}

func writeUint256NoPtr(val reflect.Value, w *encBuffer) error {
	i := val.Interface().(uint256.Int)
	return writeUint256(&i, w)
}

func writeUint256(i *uint256.Int, w *encBuffer) error {
	if i.IsZero() {
		w.str = append(w.str, EmptyStringCode)
	} else if i.LtUint64(0x80) {
		w.str = append(w.str, byte(i.Uint64()))
	} else {
		n := i.ByteLen()
		w.str = append(w.str, EmptyStringCode+byte(n))
		pos := len(w.str)
		w.str = append(w.str, make([]byte, n)...)
		i.WriteToSlice(w.str[pos:])
	}
	return nil
}

func writeBytes(val reflect.Value, w *encBuffer) error {
	w.encodeString(val.Bytes())
	return nil
}

var byteType = reflect.TypeOf(byte(0))

func makeByteArrayWriter(typ reflect.Type) writer {
	length := typ.Len()
	if length == 0 {
		return writeLengthZeroByteArray
	} else if length == 1 {
		return writeLengthOneByteArray
	}
	if typ.Elem() != byteType {
		return writeNamedByteArray
	}
	return func(val reflect.Value, w *encBuffer) error {
		writeByteArrayCopy(length, val, w)
		return nil
	}
}

func writeLengthZeroByteArray(val reflect.Value, w *encBuffer) error {
	w.str = append(w.str, 0x80)
	return nil
}

func writeLengthOneByteArray(val reflect.Value, w *encBuffer) error {
	b := byte(val.Index(0).Uint())
	if b <= 0x7f {
		w.str = append(w.str, b)
	} else {
		w.str = append(w.str, 0x81, b)
	}
	return nil
}

// writeByteArrayCopy encodes byte arrays using reflect.Copy. This is
// the fast path for [N]byte where N > 1.
func writeByteArrayCopy(length int, val reflect.Value, w *encBuffer) {
	w.encodeStringHeader(length)
	offset := len(w.str)
	w.str = append(w.str, make([]byte, length)...)
	w.bufvalue.SetBytes(w.str[offset:])
	reflect.Copy(w.bufvalue, val)
}

// writeNamedByteArray encodes byte arrays with named element type.
// This exists because reflect.Copy can't be used with such types.
func writeNamedByteArray(val reflect.Value, w *encBuffer) error {
	if !val.CanAddr() {
		// Slice requires the value to be addressable.
		// Make it addressable by copying.
		copy := reflect.New(val.Type()).Elem()
		copy.Set(val)
		val = copy
	}
	size := val.Len()
	slice := val.Slice(0, size).Bytes()
	w.encodeString(slice)
	return nil
}

func writeString(val reflect.Value, w *encBuffer) error {
	s := val.String()
	if len(s) == 1 && s[0] <= 0x7f {
		// fits single byte, no string header
		w.str = append(w.str, s[0])
	} else {
		w.encodeStringHeader(len(s))
		w.str = append(w.str, s...)
	}
	return nil
}

func writeInterface(val reflect.Value, w *encBuffer) error {
	if val.IsNil() {
		// Write empty list. This is consistent with the previous RLP
		// encoder that we had and should therefore avoid any
		// problems.
		w.str = append(w.str, EmptyListCode)
		return nil
	}
	eval := val.Elem()
	wtr, wErr := cachedWriter(eval.Type())
	if wErr != nil {
		return wErr
	}
	return wtr(eval, w)
}

func makeSliceWriter(typ reflect.Type, ts tags) (writer, error) {
	etypeinfo := theTC.infoWhileGenerating(typ.Elem(), tags{})
	if etypeinfo.writerErr != nil {
		return nil, etypeinfo.writerErr
	}
	writer := func(val reflect.Value, w *encBuffer) error {
		if !ts.tail {
			defer w.listEnd(w.list())
		}
		vlen := val.Len()
		for i := 0; i < vlen; i++ {
			if err := etypeinfo.writer(val.Index(i), w); err != nil {
				return err
			}
		}
		return nil
	}
	return writer, nil
}

func makeStructWriter(typ reflect.Type) (writer, error) {
	fields, err := structFields(typ)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		if f.info.writerErr != nil {
			return nil, structFieldError{typ, f.index, f.info.writerErr}
		}
	}

	var writer writer
	firstOptionalField := firstOptionalField(fields)
	if firstOptionalField == len(fields) {
		// This is the writer function for structs without any optional fields.
		writer = func(val reflect.Value, w *encBuffer) error {
			lh := w.list()
			for _, f := range fields {
				if err := f.info.writer(val.Field(f.index), w); err != nil {
					return err
				}
			}
			w.listEnd(lh)
			return nil
		}
	} else {
		// If there are any "optional" fields, the writer needs to perform additional
		// checks to determine the output list length.
		writer = func(val reflect.Value, w *encBuffer) error {
			lastField := len(fields) - 1
			for ; lastField >= firstOptionalField; lastField-- {
				if !val.Field(fields[lastField].index).IsZero() {
					break
				}
			}
			lh := w.list()
			for i := 0; i <= lastField; i++ {
				if err := fields[i].info.writer(val.Field(fields[i].index), w); err != nil {
					return err
				}
			}
			w.listEnd(lh)
			return nil
		}
	}
	return writer, nil
}

func makePtrWriter(typ reflect.Type, ts tags) (writer, error) {
	etypeinfo := theTC.infoWhileGenerating(typ.Elem(), tags{})
	if etypeinfo.writerErr != nil {
		return nil, etypeinfo.writerErr
	}
	// Determine how to encode nil pointers.
	var nilKind Kind
	if ts.nilOK {
		nilKind = ts.nilKind // use struct tag if provided
	} else {
		nilKind = defaultNilKind(typ.Elem())
	}

	writer := func(val reflect.Value, w *encBuffer) error {
		if val.IsNil() {
			if nilKind == String {
				w.str = append(w.str, EmptyStringCode)
			} else {
				w.listEnd(w.list())
			}
			return nil
		}
		return etypeinfo.writer(val.Elem(), w)
	}
	return writer, nil
}

func makeEncoderWriter(typ reflect.Type) writer {
	if typ.Implements(encoderInterface) {
		return func(val reflect.Value, w *encBuffer) error {
			return val.Interface().(Encoder).EncodeRLP(w)
		}
	}
	w := func(val reflect.Value, w *encBuffer) error {
		if !val.CanAddr() {
			// package json simply doesn't call MarshalJSON for this case, but encodes the
			// value as if it didn't implement the interface. We don't want to handle it that
			// way.
			return fmt.Errorf("rlp: unaddressable value of type %v, EncodeRLP is pointer method", val.Type())
		}
		return val.Addr().Interface().(Encoder).EncodeRLP(w)
	}
	return w
}

// putint writes i to the beginning of b in big endian byte
// order, using the least number of bytes needed to represent i.
func putint(b []byte, i uint64) (size int) {
	switch {
	case i < (1 << 8):
		b[0] = byte(i)
		return 1
	case i < (1 << 16):
		b[0] = byte(i >> 8)
		b[1] = byte(i)
		return 2
	case i < (1 << 24):
		b[0] = byte(i >> 16)
		b[1] = byte(i >> 8)
		b[2] = byte(i)
		return 3
	case i < (1 << 32):
		b[0] = byte(i >> 24)
		b[1] = byte(i >> 16)
		b[2] = byte(i >> 8)
		b[3] = byte(i)
		return 4
	case i < (1 << 40):
		b[0] = byte(i >> 32)
		b[1] = byte(i >> 24)
		b[2] = byte(i >> 16)
		b[3] = byte(i >> 8)
		b[4] = byte(i)
		return 5
	case i < (1 << 48):
		b[0] = byte(i >> 40)
		b[1] = byte(i >> 32)
		b[2] = byte(i >> 24)
		b[3] = byte(i >> 16)
		b[4] = byte(i >> 8)
		b[5] = byte(i)
		return 6
	case i < (1 << 56):
		b[0] = byte(i >> 48)
		b[1] = byte(i >> 40)
		b[2] = byte(i >> 32)
		b[3] = byte(i >> 24)
		b[4] = byte(i >> 16)
		b[5] = byte(i >> 8)
		b[6] = byte(i)
		return 7
	default:
		b[0] = byte(i >> 56)
		b[1] = byte(i >> 48)
		b[2] = byte(i >> 40)
		b[3] = byte(i >> 32)
		b[4] = byte(i >> 24)
		b[5] = byte(i >> 16)
		b[6] = byte(i >> 8)
		b[7] = byte(i)
		return 8
	}
}

// intsize computes the minimum number of bytes required to store i.
func intsize(i uint64) (size int) {
	return common.BitLenToByteLen(bits.Len64(i))
}

func IntLenExcludingHead(i uint64) int {
	if i < 0x80 {
		return 0
	}
	return intsize(i)
}

func BigIntLenExcludingHead(i *big.Int) int {
	bitLen := i.BitLen()
	if bitLen < 8 {
		return 0
	}
	return common.BitLenToByteLen(bitLen)
}

func Uint256LenExcludingHead(i *uint256.Int) int {
	bitLen := i.BitLen()
	if bitLen < 8 {
		return 0
	}
	return common.BitLenToByteLen(bitLen)
}

// precondition: len(buffer) >= 9
func EncodeInt(i uint64, w io.Writer, buffer []byte) error {
	if 0 < i && i < 0x80 {
		buffer[0] = byte(i)
		_, err := w.Write(buffer[:1])
		return err
	}

	binary.BigEndian.PutUint64(buffer[1:], i)
	size := intsize(i)
	buffer[8-size] = 0x80 + byte(size)
	_, err := w.Write(buffer[8-size : 9])
	return err
}

func EncodeBigInt(i *big.Int, w io.Writer, buffer []byte) error {
	bitLen := 0 // treat nil as 0
	if i != nil {
		bitLen = i.BitLen()
	}
	if bitLen < 8 {
		if bitLen > 0 {
			buffer[0] = byte(i.Uint64())
		} else {
			buffer[0] = 0x80
		}
		_, err := w.Write(buffer[:1])
		return err
	}

	size := common.BitLenToByteLen(bitLen)
	buffer[0] = 0x80 + byte(size)
	i.FillBytes(buffer[1 : 1+size])
	_, err := w.Write(buffer[:1+size])
	return err
}

func EncodeUint256(i *uint256.Int, w io.Writer, buffer []byte) error {
	buffer[0] = 0x80
	if i == nil {
		_, err := w.Write(buffer[:1])
		return err
	}
	nBits := i.BitLen()
	if nBits == 0 {
		_, err := w.Write(buffer[:1])
		return err
	}
	buffer[0] = byte(i[0])
	if nBits <= 7 {
		_, err := w.Write(buffer[:1])
		return err
	}
	nBytes := byte(common.BitLenToByteLen(nBits))
	buffer[0] = 0x80 + nBytes
	if _, err := w.Write(buffer[:1]); err != nil {
		return err
	}
	i.PutUint256(buffer)
	_, err := w.Write(buffer[32-nBytes : 32])
	return err
}

func EncodeString(s []byte, w io.Writer, buffer []byte) error {
	switch len(s) {
	case 0:
		buffer[0] = 128
		if _, err := w.Write(buffer[:1]); err != nil {
			return err
		}
	case 1:
		if s[0] >= 128 {
			buffer[0] = 129
			if _, err := w.Write(buffer[:1]); err != nil {
				return err
			}
		}
		if _, err := w.Write(s); err != nil {
			return err
		}
	default:
		if err := EncodeStringSizePrefix(len(s), w, buffer); err != nil {
			return err
		}
		if _, err := w.Write(s); err != nil {
			return err
		}
	}
	return nil
}

func EncodeStringSizePrefix(size int, w io.Writer, buffer []byte) error {
	if size >= 56 {
		beSize := common.BitLenToByteLen(bits.Len(uint(size)))
		binary.BigEndian.PutUint64(buffer[1:], uint64(size))
		buffer[8-beSize] = byte(beSize) + 183
		if _, err := w.Write(buffer[8-beSize : 9]); err != nil {
			return err
		}
	} else {
		buffer[0] = byte(size) + 128
		if _, err := w.Write(buffer[:1]); err != nil {
			return err
		}
	}
	return nil
}

func EncodeOptionalAddress(addr *common.Address, w io.Writer, buffer []byte) error {
	if addr == nil {
		buffer[0] = 128
	} else {
		buffer[0] = 128 + 20
	}

	if _, err := w.Write(buffer[:1]); err != nil {
		return err
	}
	if addr != nil {
		if _, err := w.Write(addr[:]); err != nil {
			return err
		}
	}

	return nil
}

func EncodeStructSizePrefix(size int, w io.Writer, buffer []byte) error {
	if size >= 56 {
		beSize := common.BitLenToByteLen(bits.Len(uint(size)))
		binary.BigEndian.PutUint64(buffer[1:], uint64(size))
		buffer[8-beSize] = byte(beSize) + 247
		if _, err := w.Write(buffer[8-beSize : 9]); err != nil {
			return err
		}
	} else {
		buffer[0] = byte(size) + 192
		if _, err := w.Write(buffer[:1]); err != nil {
			return err
		}
	}
	return nil
}

func ByteSliceSliceSize(bb [][]byte) int {
	size := 0
	for i := 0; i < len(bb); i++ {
		size += StringLen(bb[i])
	}
	return size + ListPrefixLen(size)
}

func EncodeByteSliceSlice(bb [][]byte, w io.Writer, b []byte) error {
	totalSize := 0
	for i := 0; i < len(bb); i++ {
		totalSize += StringLen(bb[i])
	}

	if err := EncodeStructSizePrefix(totalSize, w, b); err != nil {
		return err
	}

	for i := 0; i < len(bb); i++ {
		if err := EncodeString(bb[i], w, b); err != nil {
			return err
		}
	}
	return nil
}
