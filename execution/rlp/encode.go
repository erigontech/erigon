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
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp/internal/rlpstruct"
)

const (
	EmptyStringCode     = 0x80 // short string prefix (length 0–55)
	LongStringCode      = 0xB7 // long string prefix (length 56+)
	EmptyListCode       = 0xC0 // short list prefix (length 0–55)
	LongListCode        = 0xF7 // long list prefix (length 56+)
	SingleByteThreshold = 0x80 // values below this are encoded as themselves
)

var ErrNegativeBigInt = errors.New("rlp: cannot encode negative big.Int")

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

// --- Reflection-based API ---

// Encode writes the RLP encoding of val to w. Note that Encode may
// perform many small writes in some cases. Consider making w
// buffered.
//
// Please see package-level documentation of encoding rules.
func Encode(w io.Writer, val interface{}) error {
	// Optimization: reuse *encBuffer when called by EncodeRLP.
	if buf := encBufferFromWriter(w); buf != nil {
		return buf.encode(val)
	}

	buf := getEncBuffer()
	defer encBufferPool.Put(buf)
	if err := buf.encode(val); err != nil {
		return err
	}
	return buf.writeTo(w)
}

// EncodeToBytes returns the RLP encoding of val.
// Please see package-level documentation for the encoding rules.
func EncodeToBytes(val interface{}) ([]byte, error) {
	buf := getEncBuffer()
	defer encBufferPool.Put(buf)

	if err := buf.encode(val); err != nil {
		return nil, err
	}
	return buf.makeBytes(), nil
}

// EncodeToReader returns a reader from which the RLP encoding of val
// can be read. The returned size is the total size of the encoded
// data.
//
// Please see the documentation of Encode for the encoding rules.
func EncodeToReader(val interface{}) (size int, r io.Reader, err error) {
	buf := getEncBuffer()
	if err := buf.encode(val); err != nil {
		encBufferPool.Put(buf)
		return 0, nil, err
	}
	// Note: can't put the reader back into the pool here
	// because it is held by encReader. The reader puts it
	// back when it has been fully consumed.
	return buf.size(), &encReader{buf: buf}, nil
}

type listhead struct {
	offset int // index of this header in string data
	size   int // total size of encoded data (including list headers)
}

// encode writes head to the given buffer, which must be at least
// 9 bytes long. It returns the encoded bytes.
func (head *listhead) encode(buf []byte) []byte {
	return buf[:encodePrefixToBuf(head.size, buf, EmptyListCode, LongListCode)]
}

var encoderInterface = reflect.TypeFor[Encoder]()

// makeWriter creates a writer function for the given type.
func makeWriter(typ reflect.Type, ts rlpstruct.Tags) (writer, error) {
	kind := typ.Kind()
	switch {
	case typ == rawValueType:
		return writeRawValue, nil
	case typ.AssignableTo(reflect.PointerTo(bigInt)):
		return writeBigIntPtr, nil
	case typ.AssignableTo(bigInt):
		return writeBigIntNoPtr, nil
	case typ == reflect.PointerTo(u256Int):
		return writeU256IntPtr, nil
	case typ == u256Int:
		return writeU256IntNoPtr, nil
	case kind == reflect.Pointer:
		return makePtrWriter(typ, ts)
	case reflect.PointerTo(typ).Implements(encoderInterface):
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
	w.writeUint64(val.Uint())
	return nil
}

func writeInt(val reflect.Value, w *encBuffer) error {
	i := val.Int()
	if i < 0 {
		return fmt.Errorf("rlp: type %v -ve values are not RLP-serializable", val.Type())
	}
	w.writeUint64(uint64(i))
	return nil
}

func writeBool(val reflect.Value, w *encBuffer) error {
	w.writeBool(val.Bool())
	return nil
}

func writeBigIntPtr(val reflect.Value, w *encBuffer) error {
	ptr := val.Interface().(*big.Int)
	if ptr == nil {
		w.str = append(w.str, EmptyStringCode)
		return nil
	}
	if ptr.Sign() == -1 {
		return ErrNegativeBigInt
	}
	w.writeBigInt(ptr)
	return nil
}

func writeBigIntNoPtr(val reflect.Value, w *encBuffer) error {
	i := val.Interface().(big.Int)
	if i.Sign() == -1 {
		return ErrNegativeBigInt
	}
	w.writeBigInt(&i)
	return nil
}

func writeU256IntPtr(val reflect.Value, w *encBuffer) error {
	ptr := val.Interface().(*uint256.Int)
	if ptr == nil {
		w.str = append(w.str, EmptyStringCode)
		return nil
	}
	w.writeUint256(ptr)
	return nil
}

func writeU256IntNoPtr(val reflect.Value, w *encBuffer) error {
	i := val.Interface().(uint256.Int)
	w.writeUint256(&i)
	return nil
}

func writeBytes(val reflect.Value, w *encBuffer) error {
	w.writeBytes(val.Bytes())
	return nil
}

func makeByteArrayWriter(typ reflect.Type) writer {
	switch typ.Len() {
	case 0:
		return writeLengthZeroByteArray
	case 1:
		return writeLengthOneByteArray
	default:
		return func(val reflect.Value, w *encBuffer) error {
			if !val.CanAddr() {
				// Getting the byte slice of val requires it to be addressable. Make it
				// addressable by copying.
				copy := reflect.New(val.Type()).Elem()
				copy.Set(val)
				val = copy
			}
			slice := val.Bytes()
			w.encodeStringHeader(len(slice))
			w.str = append(w.str, slice...)
			return nil
		}
	}
}

func writeLengthZeroByteArray(val reflect.Value, w *encBuffer) error {
	w.str = append(w.str, EmptyStringCode)
	return nil
}

func writeLengthOneByteArray(val reflect.Value, w *encBuffer) error {
	b := byte(val.Index(0).Uint())
	if b <= 0x7f {
		w.str = append(w.str, b)
	} else {
		w.str = append(w.str, EmptyStringCode+1, b)
	}
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
	writer, err := cachedWriter(eval.Type())
	if err != nil {
		return err
	}
	return writer(eval, w)
}

func makeSliceWriter(typ reflect.Type, ts rlpstruct.Tags) (writer, error) {
	etypeinfo := theTC.infoWhileGenerating(typ.Elem(), rlpstruct.Tags{})
	if etypeinfo.writerErr != nil {
		return nil, etypeinfo.writerErr
	}

	var wfn writer
	if ts.Tail {
		// This is for struct tail slices.
		// w.list is not called for them.
		wfn = func(val reflect.Value, w *encBuffer) error {
			vlen := val.Len()
			for i := 0; i < vlen; i++ {
				if err := etypeinfo.writer(val.Index(i), w); err != nil {
					return err
				}
			}
			return nil
		}
	} else {
		// This is for regular slices and arrays.
		wfn = func(val reflect.Value, w *encBuffer) error {
			vlen := val.Len()
			if vlen == 0 {
				w.str = append(w.str, EmptyListCode)
				return nil
			}
			listOffset := w.list()
			for i := 0; i < vlen; i++ {
				if err := etypeinfo.writer(val.Index(i), w); err != nil {
					return err
				}
			}
			w.listEnd(listOffset)
			return nil
		}
	}
	return wfn, nil
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

func makePtrWriter(typ reflect.Type, ts rlpstruct.Tags) (writer, error) {
	nilEncoding := byte(EmptyListCode)
	if typeNilKind(typ.Elem(), ts) == String {
		nilEncoding = EmptyStringCode
	}

	etypeinfo := theTC.infoWhileGenerating(typ.Elem(), rlpstruct.Tags{})
	if etypeinfo.writerErr != nil {
		return nil, etypeinfo.writerErr
	}

	writer := func(val reflect.Value, w *encBuffer) error {
		if ev := val.Elem(); ev.IsValid() {
			return etypeinfo.writer(ev, w)
		}
		w.str = append(w.str, nilEncoding)
		return nil
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

// --- Internal helpers ---

// putint writes i to the beginning of b in big endian byte
// order, using the least number of bytes needed to represent i.
func putint(b []byte, i uint64) (size int) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], i)
	size = common.BitLenToByteLen(bits.Len64(i))
	copy(b, tmp[8-size:])
	return size
}

func encodePrefixToBuf(size int, to []byte, smallTag, largeTag byte) int {
	if size >= 56 {
		beLen := putint(to[1:], uint64(size))
		to[0] = largeTag + byte(beLen)
		return 1 + beLen
	}
	to[0] = smallTag + byte(size)
	return 1
}

// --- Encoding scratch buffer ---

// EncodingBuf is a pooled scratch buffer for hand-written EncodeRLP methods.
type EncodingBuf [32]byte

var encodingBufPool = sync.Pool{
	New: func() any { return new(EncodingBuf) },
}

func NewEncodingBuf() *EncodingBuf {
	return encodingBufPool.Get().(*EncodingBuf)
}

func (b *EncodingBuf) Release() {
	encodingBufPool.Put(b)
}

// --- Integer encoding ---

// U64Len returns the RLP-encoded length of i.
func U64Len(i uint64) int {
	if i < SingleByteThreshold {
		return 1
	}
	return 1 + common.BitLenToByteLen(bits.Len64(i))
}

// EncodeU64ToBuf encodes i as an RLP string into to and returns the number of bytes written.
func EncodeU64ToBuf(i uint64, to []byte) int {
	if i == 0 {
		to[0] = EmptyStringCode
		return 1
	}
	if i < SingleByteThreshold {
		to[0] = byte(i) // fits single byte
		return 1
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	size := common.BitLenToByteLen(bits.Len64(i))
	to[0] = EmptyStringCode + byte(size)
	copy(to[1:], buf[8-size:])
	return 1 + size
}

// EncodeU32ToBuf encodes i as an RLP string into to and returns the number of bytes written.
func EncodeU32ToBuf(i uint32, to []byte) int {
	return EncodeU64ToBuf(uint64(i), to)
}

// EncodeU64 encodes i as an RLP string via w.
// precondition: len(buffer) >= 9
func EncodeU64(i uint64, w io.Writer, buffer []byte) error {
	if 0 < i && i < SingleByteThreshold {
		buffer[0] = byte(i)
		_, err := w.Write(buffer[:1])
		return err
	}
	// i == 0 is handled here: PutUint64 writes zeros, BitLenToByteLen(Len64(0)) == 0,
	// so buffer[8] = EmptyStringCode and we write the single byte 0x80.
	binary.BigEndian.PutUint64(buffer[1:], i)
	size := common.BitLenToByteLen(bits.Len64(i))
	buffer[8-size] = EmptyStringCode + byte(size)
	_, err := w.Write(buffer[8-size : 9])
	return err
}

// BigIntLen returns the RLP-encoded length of i.
func BigIntLen(i *big.Int) int {
	bitLen := 0 // treat nil as 0
	if i != nil {
		bitLen = i.BitLen()
	}
	if bitLen < 8 {
		return 1
	}
	// Strictly speaking, +1 is not correct when the number is longer than 55 bytes
	// (see https://ethereum.org/developers/docs/data-structures-and-encoding/rlp/),
	// but in practice all our numbers are smaller than that.
	return 1 + common.BitLenToByteLen(bitLen)
}

// EncodeBigInt encodes i as an RLP string via w.
func EncodeBigInt(i *big.Int, w io.Writer, buffer []byte) error {
	bitLen := 0 // treat nil as 0
	if i != nil {
		bitLen = i.BitLen()
	}
	if bitLen < 8 {
		if bitLen > 0 {
			buffer[0] = byte(i.Uint64())
		} else {
			buffer[0] = EmptyStringCode
		}
		_, err := w.Write(buffer[:1])
		return err
	}

	size := common.BitLenToByteLen(bitLen)
	buffer[0] = EmptyStringCode + byte(size)
	i.FillBytes(buffer[1 : 1+size])
	_, err := w.Write(buffer[:1+size])
	return err
}

// Uint256Len returns the RLP-encoded length of i.
func Uint256Len(i uint256.Int) int {
	bitLen := i.BitLen()
	if bitLen < 8 {
		return 1
	}
	return 1 + common.BitLenToByteLen(bitLen)
}

// EncodeUint256 encodes i as an RLP string via w.
func EncodeUint256(i uint256.Int, w io.Writer, buffer []byte) error {
	nBits := i.BitLen()
	if nBits == 0 {
		buffer[0] = EmptyStringCode
		_, err := w.Write(buffer[:1])
		return err
	}
	if nBits <= 7 {
		buffer[0] = byte(i[0])
		_, err := w.Write(buffer[:1])
		return err
	}
	nBytes := common.BitLenToByteLen(nBits)
	if nBytes < 32 {
		i.PutUint256(buffer)
		// Overwrite the last leading zero byte with the RLP size prefix,
		// producing [prefix, value...] in a single contiguous write.
		buffer[31-nBytes] = EmptyStringCode + byte(nBytes)
		_, err := w.Write(buffer[31-nBytes : 32])
		return err
	}
	// nBytes == 32 (rare: value >= 2^248): prefix can't fit before the value.
	buffer[0] = EmptyStringCode + 32
	if _, err := w.Write(buffer[:1]); err != nil {
		return err
	}
	i.PutUint256(buffer)
	_, err := w.Write(buffer[:32])
	return err
}

// --- String encoding ---

// StringLen returns the RLP-encoded length of s.
func StringLen(s []byte) int {
	sLen := len(s)
	switch {
	case sLen >= 56:
		beLen := common.BitLenToByteLen(bits.Len(uint(sLen)))
		return 1 + beLen + sLen
	case sLen == 0:
		return 1
	case sLen == 1:
		if s[0] < SingleByteThreshold {
			return 1
		}
		return 1 + sLen
	default: // 1<s<56
		return 1 + sLen
	}
}

// EncodeStringToBuf encodes src as an RLP string into dst and returns the number of bytes written.
func EncodeStringToBuf(src []byte, dst []byte) int {
	switch {
	case len(src) >= 56:
		beLen := common.BitLenToByteLen(bits.Len(uint(len(src))))
		binary.BigEndian.PutUint64(dst[1:], uint64(len(src)))
		_ = dst[beLen+len(src)]

		dst[8-beLen] = byte(beLen) + LongStringCode
		copy(dst, dst[8-beLen:9])
		copy(dst[1+beLen:], src)
		return 1 + beLen + len(src)
	case len(src) == 0:
		dst[0] = EmptyStringCode
		return 1
	case len(src) == 1:
		if src[0] < SingleByteThreshold {
			dst[0] = src[0]
			return 1
		}
		dst[0] = EmptyStringCode + 1
		dst[1] = src[0]
		return 2
	default: // 1<src<56
		_ = dst[len(src)]
		dst[0] = byte(len(src)) + EmptyStringCode
		copy(dst[1:], src)
		return 1 + len(src)
	}
}

// EncodeString encodes s as an RLP string via w.
func EncodeString(s []byte, w io.Writer, buffer []byte) error {
	switch {
	case len(s) == 0:
		buffer[0] = EmptyStringCode
		_, err := w.Write(buffer[:1])
		return err
	case len(s) == 1 && s[0] < SingleByteThreshold:
		_, err := w.Write(s)
		return err
	case len(s) < len(buffer) && len(s) < 56:
		// Short string: assemble prefix + data in buffer for a single write.
		buffer[0] = EmptyStringCode + byte(len(s))
		copy(buffer[1:], s)
		_, err := w.Write(buffer[:1+len(s)])
		return err
	default:
		if err := EncodeStringPrefix(len(s), w, buffer); err != nil {
			return err
		}
		_, err := w.Write(s)
		return err
	}
}

// EncodeStringPrefix writes a string-type size prefix via w.
func EncodeStringPrefix(size int, w io.Writer, buffer []byte) error {
	return encodePrefix(size, w, buffer, EmptyStringCode, LongStringCode)
}

// --- List encoding ---

// ListLen returns the RLP-encoded length of a list with the given content length.
func ListLen(contentLen int) int {
	return ListPrefixLen(contentLen) + contentLen
}

// ListPrefixLen returns the RLP list-header length for a list of the given data length.
func ListPrefixLen(dataLen int) int {
	if dataLen >= 56 {
		return 1 + common.BitLenToByteLen(bits.Len64(uint64(dataLen)))
	}
	return 1
}

// EncodeListPrefixToBuf encodes a list-type size prefix into to and returns the number of bytes written.
func EncodeListPrefixToBuf(dataLen int, to []byte) int {
	return encodePrefixToBuf(dataLen, to, EmptyListCode, LongListCode)
}

// EncodeListPrefix writes a list-type size prefix via w.
func EncodeListPrefix(size int, w io.Writer, buffer []byte) error {
	return encodePrefix(size, w, buffer, EmptyListCode, LongListCode)
}

// encodePrefix writes a size prefix via w. Kept as a separate non-inlined function
// so that the thin wrappers (EncodeListPrefix, EncodeStringPrefix) stay inlineable.
func encodePrefix(size int, w io.Writer, buffer []byte, smallTag, largeTag byte) error {
	n := encodePrefixToBuf(size, buffer, smallTag, largeTag)
	_, err := w.Write(buffer[:n])
	return err
}

// --- Composite helpers ---

// StringListLen returns the RLP-encoded size of a [][]byte as a list of strings.
func StringListLen(bb [][]byte) int {
	size := 0
	for i := 0; i < len(bb); i++ {
		size += StringLen(bb[i])
	}
	return size + ListPrefixLen(size)
}

// EncodeStringList encodes a [][]byte as an RLP list of strings via w.
func EncodeStringList(bb [][]byte, w io.Writer, b []byte) error {
	totalSize := 0
	for i := 0; i < len(bb); i++ {
		totalSize += StringLen(bb[i])
	}

	if err := EncodeListPrefix(totalSize, w, b); err != nil {
		return err
	}

	for i := 0; i < len(bb); i++ {
		if err := EncodeString(bb[i], w, b); err != nil {
			return err
		}
	}
	return nil
}
