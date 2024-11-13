package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"

	"github.com/erigontech/erigon/rlp"
	"github.com/holiman/uint256"
)

const (
	SHORT_STR  byte = 0x80
	LONG_STR   byte = 0xB7
	SHORT_LIST byte = 0xC0
	LONG_LIST  byte = 0xF7

	UINT64_MAX = (1 << 64) - 1
)

func __assert(exp bool, msg string) {
	if !exp {
		panic(msg)
	}
}

func bitLenToByteLen(bitLen int) int {
	return (bitLen + 7) / 8
}

/*
	The logic. Since some Erigon structures such as RawBody contain nested structures,
	RLP encoding of such structers can be represented as following:

	List - top level
	- List - 1st level
	-- List - 2nd level
	--- List - 3d level
	--- EndList - 3d level
	-- EndList - 2nd level
	- EndList - 1st level
	EndList - top level

	Between RLP lists there could be simple types such as bool, uint64 etc. Therefor the most outer/top level
	content size is calculated as sum(bytes_content) + cumulative_size(inner_lists). See _calculateListSizes,
	it's done recursively.

	To illustrate encoding of the top level struct:

	[start:pause]
		[start:pause]
			[start:pause]
				[start:pause]


	start - offset start to the list content
	pause - start of the next level list
	[start:pause] - is byte array or RLP encoded simple types
*/

type listFlag struct {
	start int // offset to starting point of list content
	end   int // offset to ending point of list content
	size  int // size of list content + cumulative length of nested prefixes, does not include prefix length of this list
}

type encBuffer struct {
	str   []byte // writes contents of Lists
	heads []listFlag
	enc   [33]byte // buffer for encoding numbers
}

var encBufferPool = sync.Pool{
	New: func() interface{} { return new(encBuffer) },
}

func NewEncodeBuffer() *encBuffer {
	buf := encBufferPool.Get().(*encBuffer)
	buf._reset()
	return buf
}

/* ========================================================
	Internal methods. Do not use directly unless 100% sure
======================================================== */

// Resets the buffer. Internal method
func (buf *encBuffer) _reset() {
	buf.str = buf.str[:0]
	buf.heads = buf.heads[:0]
	buf.enc = [33]byte{}
}

func (buf *encBuffer) _encodeEmptyStr() {
	buf.str = append(buf.str, 0x80)
}

// for boolean and integer encoding only
func (buf *encBuffer) _encodeByte(n byte) {
	switch {
	case n == 0:
		buf.str = append(buf.str, 0x80) // this part is a bit uncertain, for integers is okay to push 0x80, but not for single byte = 0
		return
	case n <= 0x7F:
		buf.str = append(buf.str, n)
		return
	default:
		// n >= 0x80
		buf.str = append(buf.str, SHORT_STR+1)
		buf.str = append(buf.str, n)
		return
	}
}

// Base method. Writes the `src` to underlying buffer as RLP encoded string.
// Every type encoding methods will end up calling this method.
// Internal/private method, not recommended to use directly
func (buf *encBuffer) _encodeString(src []byte) {

	src_len := len(src)
	switch {
	case uint64(src_len) > UINT64_MAX: // If a string is 2^64 bytes long
		panic("uint64(src_len) > UINT64_MAX")
	// case src_len == 1 && src[0] == 0: // single byte 0, encodes as 0x80 // TODO(racytech): this is buggy, or decoder is buggy
	// 	buf.str = append(buf.str, 0x80)
	// 	return
	case src_len == 1 && src[0] <= 0x7F: // single byte whose value is in the [0x00, 0x7f], src[0] >= 1!
		buf.str = append(buf.str, src[0])
		return
	case src_len < 56: // string is 0-55 bytes long
		buf.str = append(buf.str, SHORT_STR+byte(src_len))
		buf.str = append(buf.str, src...)
		return
	case src_len >= 56: // string is more than 55 bytes long, range [0xb8, 0xbf]
		idx := buf._uintToBytes(uint64(src_len))
		buf.str = append(buf.str, LONG_STR+byte(8-idx))
		buf.str = append(buf.str, buf.enc[idx:8]...)
		buf.str = append(buf.str, src...)
		return
	default:
		panic(fmt.Sprintf("_encodeString: error: hitting default case: src: %v, src_len: %v", src, src_len))
	}
}

// Writes `n` as big-endian byte array to the internal buffer and returns the starting index
// of the first non 0 byte.
// Internal/private methond, not recommended to use directly.
func (buf *encBuffer) _uintToBytes(n uint64) int {
	buf.enc = [33]byte{} // reset

	buf.enc[0] = byte(n >> 56)
	buf.enc[1] = byte(n >> 48)
	buf.enc[2] = byte(n >> 40)
	buf.enc[3] = byte(n >> 32)
	buf.enc[4] = byte(n >> 24)
	buf.enc[5] = byte(n >> 16)
	buf.enc[6] = byte(n >> 8)
	buf.enc[7] = byte(n)

	switch {
	case buf.enc[0] != 0:
		return 0
	case buf.enc[0] == 0 && buf.enc[1] != 0:
		return 1
	case buf.enc[1] == 0 && buf.enc[2] != 0:
		return 2
	case buf.enc[2] == 0 && buf.enc[3] != 0:
		return 3
	case buf.enc[3] == 0 && buf.enc[4] != 0:
		return 4
	case buf.enc[4] == 0 && buf.enc[5] != 0:
		return 5
	case buf.enc[5] == 0 && buf.enc[6] != 0:
		return 6
	case buf.enc[6] == 0 && buf.enc[7] != 0:
		return 7
	case buf.enc[7] == 0:
		// __assert(n == 0)
		return 7
	default:
		panic(fmt.Sprintf("_uintToBytes: hitting default case: %v", buf.enc[:8]))
	}
}

// Recursive method. Calculates cumulative size of the inner lists and adds it to the content's size of this list.
// Resulting size does not include prefix length of itself.
// Internal/private methond, not recommended to use directly.
func (buf *encBuffer) _calculateListSizes(idx int) int {
	if idx >= len(buf.heads) {
		return 0
	}

	buf.heads[idx].size = buf.heads[idx].end - buf.heads[idx].start + buf._calculateListSizes(idx+1)
	if buf.heads[idx].size < 56 {
		return 1 // +1 byte for encoding inner list
	} else {
		i := buf._uintToBytes(uint64(buf.heads[idx].size))
		return 1 + (8 - i) // +1 + 8-i bytes for encoding inner list
	}
}

// Writes encoded list prefix to the `w`
// Internal/private methond, not recommended to use directly.
func (buf *encBuffer) _encodeListSize(w io.Writer, n int) error {
	if n < 56 {
		buf.enc[0] = SHORT_LIST + byte(n)
		if _, err := w.Write(buf.enc[:1]); err != nil {
			return err
		}
	} else {
		idx := buf._uintToBytes(uint64(n))
		buf.enc[9] = LONG_LIST + byte(8-idx)
		if _, err := w.Write(buf.enc[9:10]); err != nil {
			return err
		}
		if _, err := w.Write(buf.enc[idx:8]); err != nil {
			return err
		}
	}
	return nil
}

// Writes RLP encoded output into `w`.
func (buf *encBuffer) _flush(w io.Writer) error {

	buf._calculateListSizes(0)
	listHeadCount := len(buf.heads)
	for i := 0; i < listHeadCount-1; i++ {
		// encode list prefix first
		if err := buf._encodeListSize(w, buf.heads[i].size); err != nil {
			return err
		}
		// encode content from offset buf.heads[i].start to offset buf.heads[i+1].start
		if _, err := w.Write(buf.str[buf.heads[i].start:buf.heads[i+1].start]); err != nil {
			return err
		}
	}
	// encode last list prefix
	if err := buf._encodeListSize(w, buf.heads[listHeadCount-1].size); err != nil {
		return err
	}
	// encode rest of the content starting from buf.heads[listHeadCount-1].start offset
	if _, err := w.Write(buf.str[buf.heads[listHeadCount-1].start:]); err != nil {
		return err
	}
	return nil
}

/* ========================================================
	Exported. Type encoding methods.
======================================================== */

func (buf *encBuffer) EncodeBool(b bool) {
	if b {
		buf._encodeByte(0x01)
	} else {
		buf._encodeByte(0x00)
	}
}

func (buf *encBuffer) EncodeInt(n uint64) {
	if n == 0 {
		buf._encodeByte(0x00)
		return
	}
	idx := buf._uintToBytes(n)
	buf._encodeString(buf.enc[idx:8])
}

func (buf *encBuffer) EncodeBigInt(n *big.Int) {
	if n == nil {
		buf._encodeEmptyStr()
		return
	}
	bitlen := n.BitLen()
	if bitlen <= 64 {
		buf.EncodeInt(n.Uint64())
		return
	}
	size := bitLenToByteLen(bitlen)
	n.FillBytes(buf.enc[:size])
	buf._encodeString(buf.enc[:size])
}

func (buf *encBuffer) EncodeUint256(n *uint256.Int) {
	if n == nil {
		buf._encodeEmptyStr()
	}
	bitlen := n.BitLen()
	if bitlen <= 64 {
		buf.EncodeInt(n.Uint64())
		return
	}
	size := bitLenToByteLen(bitlen)
	binary.BigEndian.PutUint64(buf.enc[1:9], n[3])
	binary.BigEndian.PutUint64(buf.enc[9:17], n[2])
	binary.BigEndian.PutUint64(buf.enc[17:25], n[1])
	binary.BigEndian.PutUint64(buf.enc[25:33], n[0])
	buf._encodeString(buf.enc[32-size:])
}

func (buf *encBuffer) EncodeString(s string) {
	buf._encodeString([]byte(s)) // creates allocation
}

func (buf *encBuffer) EncodeBytes(src []byte) {
	buf._encodeString(src)
}

func (buf *encBuffer) Flush(w io.Writer) error {
	return buf._flush(w)
}

/* ========================================================
	Example struct. For testing and demonstration purposes
======================================================== */

// Every struct has to have `EncodeContents` method
// EncodeContents - encodes inner parts of the struct

type RLPList interface {
	EncodeContents(buf *encBuffer)
}

// Generic function. Saves the offset to the beginning of the content -> calls EncodeContents -> saves the offset to the end of the content.
func EncodeList[T RLPList](_type T, buf *encBuffer) {
	buf.heads = append(buf.heads, listFlag{len(buf.str), 0, 0}) // save starting point of the content
	idx := len(buf.heads) - 1                                   // save index of the header

	T.EncodeContents(_type, buf)

	buf.heads[idx].end = len(buf.str) // save the offset to the end of content
}

type ExampleStruct struct {
	_bool    bool
	_uint    uint64
	_bigInt  *big.Int
	_uint256 *uint256.Int
	_string  string
	_bytes   []byte
	_2Darray [][]byte
}

type _2Dbytes [][]byte

func (arr _2Dbytes) EncodeContents(buf *encBuffer) {
	for _, b := range arr {
		buf._encodeString(b)
	}
}

func (t *ExampleStruct) encodeRLP(w io.Writer) error {
	buf := NewEncodeBuffer()
	defer encBufferPool.Put(buf)

	// t.EncodeList(buf)
	EncodeList(t, buf)
	return buf.Flush(w)
}

func (t *ExampleStruct) EncodeContents(buf *encBuffer) {
	buf.EncodeBool(t._bool)
	buf.EncodeInt(t._uint)
	buf.EncodeBigInt(t._bigInt)
	// encode uint256 here

	// _2Dbytes(t._2Darray).EncodeList(buf)
	EncodeList(_2Dbytes(t._2Darray), buf)

	buf.EncodeString(t._string)
	buf.EncodeBytes(t._bytes)

}

func (t *ExampleStruct) decodeRLP(s *rlp.Stream) error {
	var b []byte

	_, err := s.List()
	if err != nil {
		fmt.Println("THIS ERR")
		return err
	}

	if t._bool, err = s.Bool(); err != nil {
		return fmt.Errorf("read t._bool: %w", err)
	}

	if t._uint, err = s.Uint(); err != nil {
		return fmt.Errorf("read t._uint: %w", err)
	}

	if b, err = s.Uint256Bytes(); err != nil {
		if errors.Is(err, rlp.EOL) {
			t._bigInt = nil
			if err := s.ListEnd(); err != nil {
				return fmt.Errorf("close list t._bigInt: %w", err)
			}
			return nil
		}
		return fmt.Errorf("read BaseFee: %w", err)
	}
	if b != nil {
		t._bigInt = new(big.Int).SetBytes(b) //
	}

	_, err = s.List()
	if err != nil {
		return fmt.Errorf("open t._2Darray: %w", err)
	}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		t._2Darray = append(t._2Darray, b)
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobVersionedHashes: %w", err)
	}

	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read t._string: %w", err)
	}
	t._string = string(b)
	// fmt.Println(b)

	if t._bytes, err = s.Bytes(); err != nil {
		return fmt.Errorf("read t._bytes: %w", err)
	}
	s.Bytes()

	return s.ListEnd()
}

/* ========================================================
	Header type test
======================================================== */

func (h *Header) EncodeContents(buf *encBuffer) {

	buf.EncodeBytes(h.ParentHash[:])
	buf.EncodeBytes(h.UncleHash[:])
	buf.EncodeBytes(h.Coinbase[:])
	buf.EncodeBytes(h.Root[:])
	buf.EncodeBytes(h.TxHash[:])
	buf.EncodeBytes(h.ReceiptHash[:])
	buf.EncodeBytes(h.Bloom[:])

	buf.EncodeBigInt(h.Difficulty)
	buf.EncodeBigInt(h.Number)
	buf.EncodeInt(h.GasLimit)
	buf.EncodeInt(h.GasUsed)
	buf.EncodeInt(h.Time)

	buf.EncodeBytes(h.Extra)

	if len(h.AuRaSeal) > 0 {
		buf.EncodeInt(h.AuRaStep)
		buf.EncodeBytes(h.AuRaSeal)
	} else {
		buf.EncodeBytes(h.MixDigest[:])
	}

	buf.EncodeBytes(h.Nonce[:])

	if h.BaseFee != nil { // TODO(racytech): some values when 0 or nil are unstable, fix it
		buf.EncodeBigInt(h.BaseFee)
	}
	if h.WithdrawalsHash != nil {
		buf.EncodeBytes(h.WithdrawalsHash[:])
	}
	if h.BlobGasUsed != nil {
		buf.EncodeInt(*h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		buf.EncodeInt(*h.ExcessBlobGas)
	}
	if h.ParentBeaconBlockRoot != nil {
		buf.EncodeBytes(h.ParentBeaconBlockRoot.Bytes())
	}
	if h.RequestsHash != nil {
		buf.EncodeBytes(h.RequestsHash.Bytes())
	}
	if h.Verkle {
		buf.EncodeBytes(h.VerkleProof)
		// h.VerkleKeyVals // needs own EncodeContents logic, so we can call EncodeList on it
	}
}

func (h *Header) _encodeRLP(w io.Writer) error { // TODO(racytech): encodeRLP could be generic
	buf := NewEncodeBuffer()
	defer encBufferPool.Put(buf)

	// t.EncodeList(buf)
	EncodeList(h, buf)
	return buf.Flush(w)
}
