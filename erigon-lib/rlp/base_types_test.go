package rlp

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"testing"
	"time"

	"math/rand"

	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
)

type TRand struct {
	rnd *rand.Rand
}

func NewTRand() *TRand {
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	return &TRand{rnd: rand.New(src)}
}

func (tr *TRand) RandIntInRange(_min, _max int) int {
	return (tr.rnd.Intn(_max-_min) + _min)
}

func (tr *TRand) RandUint64() *uint64 {
	a := tr.rnd.Uint64()
	return &a
}

func (tr *TRand) RandUint256() *uint256.Int {
	a := new(uint256.Int).SetBytes(tr.RandBytes(tr.RandIntInRange(1, 32)))
	return a
}

func (tr *TRand) RandBig() *big.Int {
	return big.NewInt(int64(tr.rnd.Int()))
}

func (tr *TRand) RandBytes(size int) []byte {
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		arr[i] = byte(tr.rnd.Intn(256))
	}
	return arr
}

func (tr *TRand) RandAddress() common.Address {
	return common.Address(tr.RandBytes(20))
}

func (tr *TRand) RandHash() common.Hash {
	return common.Hash(tr.RandBytes(32))
}

func (tr *TRand) RandTestingStruct() *TestingStruct {
	var _bool bool
	i := tr.RandIntInRange(0, 2)
	if i%2 == 0 {
		_bool = true
	}
	_bb := int(*tr.RandUint64())
	return &TestingStruct{
		a:  _bool,
		b:  int(*tr.RandUint64()),
		bb: &_bb,
		c:  tr.RandAddress(),
		d:  tr.RandHash(),
		e:  tr.RandBig(),
		f:  tr.RandUint256(),
		g:  (*MyBytes)(tr.RandBytes(256)),
	}
}

func check(t *testing.T, f string, want, got interface{}) {
	if !reflect.DeepEqual(want, got) {
		t.Errorf("%s mismatch: want %v, got %v", f, want, got)
	}
}

func checkTestingStruct(t *testing.T, t1, t2 *TestingStruct) {
	check(t, "TestingStruct.a", t1.a, t2.a)
	check(t, "TestingStruct.b", t1.b, t2.b)
	check(t, "TestingStruct.bb", *t1.bb, *t2.bb)
	check(t, "TestingStruct.c", t1.c, t2.c)
	check(t, "TestingStruct.d", t1.d, t2.d)
	check(t, "TestingStruct.e", t1.e, t2.e)
	check(t, "TestingStruct.f", t1.f, t2.f)
	check(t, "TestingStruct.g", t1.g, t2.g)
}

type MyBytes [256]byte

type TestingStruct struct {
	a  bool
	b  int
	bb *int
	c  common.Address
	d  common.Hash
	e  *big.Int
	f  *uint256.Int
	g  *MyBytes
}

func (t *TestingStruct) EncodingSize1() (size int) {
	size += 1
	size += IntLenExcludingHead(uint64(t.b)) + 1
	size += IntLenExcludingHead(uint64(*t.bb)) + 1
	size += 21
	size += 33
	size++
	if t.e != nil {
		size += BigIntLenExcludingHead(t.e)
	}
	size++
	if t.f != nil {
		size += Uint256LenExcludingHead(t.f)
	}
	size += StringLen(t.g[:])
	return
}

// func (t *TestingStruct) EncodingSize2() (size int) {
// 	size += GenericEncodingSize(t.a)
// 	size += GenericEncodingSize(t.b)
// 	size += GenericEncodingSize(t.bb)
// 	size += GenericEncodingSize(t.c)
// 	size += GenericEncodingSize(t.d)
// 	return
// }

func (t *TestingStruct) EncodingSize3() (size int) {
	size += (*Bool)(&t.a).encodingSize()
	n := uint64(t.b)
	size += (*Uint)(&n).encodingSize()
	n = uint64(*t.bb)
	size += (*Uint)(&n).encodingSize()
	size += (*Bytes20)(&t.c).encodingSize()
	size += (*Bytes32)(&t.d).encodingSize()
	size += (*BigInt)(t.e).encodingSize()
	size += (*Uint256)(t.f).encodingSize()
	_bytes := Bytes(t.g[:])
	size += (*Bytes)(&_bytes).encodingSize()
	return
}

func (t *TestingStruct) EncodeRLP1(w io.Writer) error {
	size := t.EncodingSize1()
	var b [32]byte
	if err := EncodeStructSizePrefix(size, w, b[:]); err != nil {
		return err
	}
	if t.a {
		b[0] = 0x01
	} else {
		b[0] = 0x80
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := EncodeInt(uint64(t.b), w, b[:]); err != nil {
		return err
	}
	if err := EncodeInt(uint64(*t.bb), w, b[:]); err != nil {
		return err
	}
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(t.c[:]); err != nil {
		return err
	}
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(t.d[:]); err != nil {
		return err
	}
	if err := EncodeBigInt(t.e, w, b[:]); err != nil {
		return err
	}
	if err := EncodeUint256(t.f, w, b[:]); err != nil {
		return err
	}
	if err := EncodeString(t.g[:], w, b[:]); err != nil {
		return err
	}
	return nil
}

// func (t *TestingStruct) EncodeRLP2(w io.Writer) error {
// 	size := t.EncodingSize2()
// 	var b [32]byte
// 	if err := EncodeStructSizePrefix(size, w, b[:]); err != nil {
// 		return err
// 	}
// 	if err := GenericEncodingRLP(t.a, w, b[:]); err != nil {
// 		return err
// 	}
// 	if err := GenericEncodingRLP(t.b, w, b[:]); err != nil {
// 		return err
// 	}
// 	if err := GenericEncodingRLP(t.c, w, b[:]); err != nil {
// 		return err
// 	}
// 	if err := GenericEncodingRLP(t.d, w, b[:]); err != nil {
// 		return err
// 	}
// 	return nil
// }

func (t *TestingStruct) EncodeRLP3(w io.Writer) error {
	size := t.EncodingSize3()
	var b [32]byte
	if err := EncodeStructSizePrefix(size, w, b[:]); err != nil {
		return err
	}
	if err := (*Bool)(&t.a).encodeRLP(w, b[:]); err != nil {
		return err
	}
	n := uint64(t.b)
	if err := (*Uint)(&n).encodeRLP(w, b[:]); err != nil {
		return err
	}
	n = uint64(*t.bb)
	if err := (*Uint)(&n).encodeRLP(w, b[:]); err != nil {
		return err
	}
	if err := (*Bytes20)(&t.c).encodeRLP(w, b[:]); err != nil {
		return err
	}
	if err := (*Bytes32)(&t.d).encodeRLP(w, b[:]); err != nil {
		return err
	}
	if err := (*BigInt)(t.e).encodeRLP(w, b[:]); err != nil {
		return err
	}
	if err := (*Uint256)(t.f).encodeRLP(w, b[:]); err != nil {
		return err
	}
	_bytes := Bytes(t.g[:])
	if err := (*Bytes)(&_bytes).encodeRLP(w, b[:]); err != nil {
		return err
	}
	return nil
}

func (t *TestingStruct) EncodeRLP4(w io.Writer) error {

	n := uint64(t.b)
	nn := uint64(*t.bb)
	_bytes := Bytes(t.g[:])
	var arr = []rlpEncodable{
		(*Bool)(&t.a),
		(*Uint)(&n),
		(*Uint)(&nn),
		(*Bytes20)(&t.c),
		(*Bytes32)(&t.d),
		(*BigInt)(t.e),
		(*Uint256)(t.f),
		(*Bytes)(&_bytes),
	}

	size := 0
	for idx := range arr {
		size += arr[idx].encodingSize()
	}

	var b [32]byte
	if err := EncodeStructSizePrefix(size, w, b[:]); err != nil {
		return err
	}

	for idx := range arr {
		if err := arr[idx].encodeRLP(w, b[:]); err != nil {
			return err
		}
	}
	return nil

	// type st struct {
	// 	a  Bool
	// 	b  Uint
	// 	bb Uint
	// 	c  Bytes20
	// 	d  Bytes32
	// 	e  BigInt
	// 	f  Uint256
	// 	g  Bytes
	// }

	// var tt = st{
	// 	a:  Bool(t.a),
	// 	b:  Uint(t.b),
	// 	bb: Uint(*t.bb),
	// 	c:  Bytes20(t.c),
	// 	d:  Bytes32(t.d),
	// 	e:  BigInt(*t.e),
	// 	f:  Uint256(*t.f),
	// 	g:  Bytes(t.g[:]),
	// }

	// size := tt.a.encodingSize()
	// size += tt.b.encodingSize()
	// size += tt.bb.encodingSize()
	// size += tt.c.encodingSize()
	// size += tt.d.encodingSize()
	// size += tt.e.encodingSize()
	// size += tt.f.encodingSize()
	// size += tt.g.encodingSize()

	// var b [32]byte
	// if err := EncodeStructSizePrefix(size, w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.a.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.b.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.bb.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.c.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.d.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.e.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.f.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }
	// if err := tt.g.encodeRLP(w, b[:]); err != nil {
	// 	return err
	// }

	// return nil
}

func (t *TestingStruct) DecodeRLP(s *Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	if t.a, err = s.Bool(); err != nil {
		return fmt.Errorf("read Bool: %w", err)
	}
	if n, err := s.Uint(); err != nil {
		return fmt.Errorf("read Uint: %w", err)
	} else {
		t.b = int(n)
	}
	if n, err := s.Uint(); err != nil {
		return fmt.Errorf("read Uint: %w", err)
	} else {
		nn := int(n)
		t.bb = &nn
	}
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Bytes20: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for Bytes20: %d", len(b))
	}
	copy(t.c[:], b)

	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Bytes32: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for Bytes32: %d", len(b))
	}
	copy(t.d[:], b)

	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read BigInt: %w", err)
	}
	t.e = new(big.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read Uint256: %w", err)
	}
	t.f = new(uint256.Int).SetBytes(b)

	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Bytes: %w", err)
	}
	if len(b) != 0 {
		t.g = &MyBytes{}
		copy(t.g[:], b)
	}

	return s.ListEnd()
}

func (t *TestingStruct) DecodeRLP2(s *Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var _bool Bool
	if err := _bool.decodeRLP(s); err != nil {
		return err
	}
	t.a = bool(_bool)
	var _uint Uint
	if err := _uint.decodeRLP(s); err != nil {
		return err
	}
	t.b = int(_uint)
	if err := _uint.decodeRLP(s); err != nil {
		return err
	}
	n := int(_uint)
	t.bb = &n

	var _addr Bytes20
	if err := _addr.decodeRLP(s); err != nil {
		return err
	}
	t.c = common.Address(_addr)

	var _hash Bytes32
	if err := _hash.decodeRLP(s); err != nil {
		return err
	}
	t.d = common.Hash(_hash)

	var _bigInt BigInt
	if err := _bigInt.decodeRLP(s); err != nil {
		return err
	}
	t.e = (*big.Int)(&_bigInt)

	var _uint256 Uint256
	if err := _uint256.decodeRLP(s); err != nil {
		return err
	}
	t.f = (*uint256.Int)(&_uint256)
	var _bytes Bytes
	if err := _bytes.decodeRLP(s); err != nil {
		return err
	}
	t.g = (*MyBytes)(_bytes)

	return s.ListEnd()
}

const RUNS = 10000

func TestGenericEncode(t *testing.T) {
	tr := NewTRand()
	var buf bytes.Buffer
	for i := 0; i < RUNS; i++ {
		enc := tr.RandTestingStruct()

		buf.Reset()
		if err := enc.EncodeRLP1(&buf); err != nil {
			t.Errorf("error: TestingStruct.EncodeRLP1(): %v", err)
		}

		s := NewStream(bytes.NewReader(buf.Bytes()), 0)
		dec1 := &TestingStruct{}
		if err := dec1.DecodeRLP(s); err != nil {
			t.Errorf("error: TestingStruct.DecodeRLP()1: %v", err)
			panic(err)
		}

		buf.Reset()
		if err := enc.EncodeRLP1(&buf); err != nil {
			t.Errorf("error: TestingStruct.EncodeRLP1(): %v", err)
		}

		s = NewStream(bytes.NewReader(buf.Bytes()), 0)
		dec2 := &TestingStruct{}
		if err := dec2.DecodeRLP2(s); err != nil {
			t.Errorf("error: TestingStruct.DecodeRLP()2: %v", err)
			panic(err)
		}

		checkTestingStruct(t, enc, dec2)

		buf.Reset()
		if err := enc.EncodeRLP3(&buf); err != nil {
			t.Errorf("error: TestingStruct.EncodeRLP3(): %v", err)
		}

		s = NewStream(bytes.NewReader(buf.Bytes()), 0)
		dec3 := &TestingStruct{}
		if err := dec3.DecodeRLP(s); err != nil {
			t.Errorf("error: TestingStruct.DecodeRLP3(): %v", err)
			panic(err)
		}

		checkTestingStruct(t, enc, dec3)

		buf.Reset()
		if err := enc.EncodeRLP4(&buf); err != nil {
			t.Errorf("error: TestingStruct.EncodeRLP3(): %v", err)
		}

		s = NewStream(bytes.NewReader(buf.Bytes()), 0)
		dec4 := &TestingStruct{}
		if err := dec4.DecodeRLP(s); err != nil {
			t.Errorf("error: TestingStruct.DecodeRLP3(): %v", err)
			panic(err)
		}

		checkTestingStruct(t, enc, dec4)
	}
}

func BenchmarkTestingStructEncode1(b *testing.B) {
	tr := NewTRand()
	ts := tr.RandTestingStruct()
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		ts.EncodeRLP1(&buf)
		// ts.EncodingSize1()
	}
}

// func BenchmarkTestingStructDecode1(b *testing.B) {
// 	tr := NewTRand()
// 	ts := tr.RandTestingStruct()
// 	var buf bytes.Buffer
// 	ts.EncodeRLP1(&buf)
// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		s := NewStream(bytes.NewReader(buf.Bytes()), 0)
// 		dec2 := &TestingStruct{}
// 		dec2.DecodeRLP(s)
// 	}
// }

// func BenchmarkTestingStruct2(b *testing.B) {
// 	tr := NewTRand()
// 	ts := tr.RandTestingStruct()
// 	// var buf bytes.Buffer
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		// buf.Reset()
// 		// ts.EncodeRLP2(&buf)
// 		ts.EncodingSize2()
// 	}
// }

func BenchmarkTestingStructEncode3(b *testing.B) {
	tr := NewTRand()
	ts := tr.RandTestingStruct()
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		ts.EncodeRLP3(&buf)
		// ts.EncodingSize3()
	}
}

// func BenchmarkTestingStructDecode2(b *testing.B) {
// 	tr := NewTRand()
// 	ts := tr.RandTestingStruct()
// 	var buf bytes.Buffer
// 	ts.EncodeRLP3(&buf)
// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		s := NewStream(bytes.NewReader(buf.Bytes()), 0)
// 		dec2 := &TestingStruct{}
// 		dec2.DecodeRLP2(s)
// 	}
// }

func BenchmarkTestingStructEncode4(b *testing.B) {
	tr := NewTRand()
	ts := tr.RandTestingStruct()
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		ts.EncodeRLP4(&buf)
		// ts.EncodingSize1()
	}
}
