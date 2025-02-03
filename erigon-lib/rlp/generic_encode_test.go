package rlp

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"math/rand"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
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

func check(t *testing.T, f string, want, got interface{}) {
	if !reflect.DeepEqual(want, got) {
		t.Errorf("%s mismatch: want %v, got %v", f, want, got)
	}
}

func (tr *TRand) RandAddress() libcommon.Address {
	return libcommon.Address(tr.RandBytes(20))
}

func (tr *TRand) RandHash() libcommon.Hash {
	return libcommon.Hash(tr.RandBytes(32))
}

func (tr *TRand) RandTestingStruct() *TestingStruct {
	var _bool bool
	i := tr.RandIntInRange(0, 2)
	if i%2 == 0 {
		_bool = true
	}
	return &TestingStruct{
		a: _bool,
		b: int(*tr.RandUint64()),
		c: tr.RandAddress(),
		d: tr.RandHash(),
	}
}

type TestingStruct struct {
	a bool
	b int
	c common.Address
	d common.Hash
}

func (t *TestingStruct) EncodingSize1() (size int) {
	size += 1
	size += IntLenExcludingHead(uint64(t.b))
	size += 21
	size += 33
	return
}

func (t *TestingStruct) EncodingSize2() (size int) {
	size += GenericEncodingSize(t.a)
	size += GenericEncodingSize(t.b)
	size += GenericEncodingSize(t.c)
	size += GenericEncodingSize(t.d)
	return
}

func TestGenericEncodingSize(t *testing.T) {

}

func BenchmarkTestingStruct1(b *testing.B) {
	tr := NewTRand()
	txn := tr.RandTestingStruct()
	// var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// buf.Reset()
		txn.EncodingSize1()
	}
}

func BenchmarkTestingStruct2(b *testing.B) {
	tr := NewTRand()
	txn := tr.RandTestingStruct()
	// var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// buf.Reset()
		txn.EncodingSize2()
	}
}
