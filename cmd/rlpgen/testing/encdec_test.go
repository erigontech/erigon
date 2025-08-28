package testing

import (
	"bytes"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
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

func (tr *TRand) RandBloom() types.Bloom {
	return types.Bloom(tr.RandBytes(types.BloomByteLength))
}

func check(t *testing.T, f string, want, got interface{}) {
	if !reflect.DeepEqual(want, got) {
		t.Errorf("%s mismatch: want %v, got %v", f, want, got)
	}
}

func compareTestingStructs(t *testing.T, a, b *TestingStruct) {
	check(t, "obj.a", a.a, b.a)
	check(t, "obj.aa", a.aa, b.aa)
	check(t, "obj.b", a.b, b.b)
	check(t, "obj.bb", a.bb, b.bb)
	check(t, "obj.c", a.c, b.c)
	check(t, "obj.cc", a.cc, b.cc)
	check(t, "obj.d", a.d, b.d)
	check(t, "obj.dd", a.dd, b.dd)
	check(t, "obj.e", a.e, b.e)
	check(t, "obj.ee", a.ee, b.ee)
	check(t, "obj.f", a.f, b.f)
	check(t, "obj.ff", a.ff, b.ff)
	check(t, "obj.g", a.g, b.g)
	check(t, "obj.gg", a.gg, b.gg)
	check(t, "obj.h", a.h, b.h)
	check(t, "obj.hh", a.hh, b.hh)
	check(t, "obj.i", a.i, b.i)

	if len(a.j) != len(b.j) {
		t.Errorf("len mismatch: want %v, got %v", len(a.j), len(b.j))
	}
	for i := 0; i < len(a.j); i++ {
		check(t, "obj.j each", a.j[i], b.j[i])
	}
	check(t, "obj.j", a.j, b.j)

	if len(a.jj) != len(b.jj) {
		t.Errorf("len mismatch: want %v, got %v", len(a.jj), len(b.jj))
	}
	for i := 0; i < len(a.jj); i++ {
		check(t, "obj.j each", a.jj[i], b.jj[i])
	}

	check(t, "obj.jj", a.jj, b.jj)
	check(t, "obj.k", a.k, b.k)
	check(t, "obj.kk", a.kk, b.kk)
	check(t, "obj.l", a.l, b.l)
	check(t, "obj.ll", a.ll, b.ll)
	check(t, "obj.n", a.m, b.m)
	check(t, "obj.n", a.mm, b.mm)
}

func randTestingStruct(tr *TRand) *TestingStruct {
	// _int := tr.RandIntInRange(0, 1<<32)
	_byteSlice := tr.RandBytes(tr.RandIntInRange(0, 128))

	l := tr.RandIntInRange(0, 8)
	_byteSliceSlice := make([][]byte, l)
	for i := 0; i < l; i++ {
		_byteSliceSlice[i] = tr.RandBytes(tr.RandIntInRange(0, 128))
	}

	l = tr.RandIntInRange(0, 8)
	_byteSliceSlicePtr := make([]*[]byte, l)
	for i := 0; i < l; i++ {
		arr := tr.RandBytes(tr.RandIntInRange(0, 128))
		_byteSliceSlicePtr[i] = &arr
	}

	l = tr.RandIntInRange(0, 8)
	_nonceSlice := make([]types.BlockNonce, l)
	for i := 0; i < l; i++ {
		_nonceSlice[i] = types.BlockNonce(tr.RandBytes(8))
	}

	l = tr.RandIntInRange(0, 8)
	_nonceSlicePtr := make([]*types.BlockNonce, l)
	for i := 0; i < l; i++ {
		nonce := types.BlockNonce(tr.RandBytes(8))
		if i%2 == 0 {
			_nonceSlicePtr[i] = &nonce
		} else {
			_nonceSlicePtr[i] = nil
		}

	}

	l = tr.RandIntInRange(0, 8)
	_addrSlice := make([]common.Address, l)
	for i := 0; i < l; i++ {
		_addrSlice[i] = tr.RandAddress()
	}

	l = tr.RandIntInRange(0, 8)
	_addrSlicePtr := make([]*common.Address, l)
	for i := 0; i < l; i++ {
		addr := tr.RandAddress()
		_addrSlicePtr[i] = &addr
	}

	l = tr.RandIntInRange(0, 8)
	_hashSlice := make([]common.Hash, l)
	for i := 0; i < l; i++ {
		_hashSlice[i] = tr.RandHash()
	}

	l = tr.RandIntInRange(0, 8)
	_hashSlicePtr := make([]*common.Hash, l)
	for i := 0; i < l; i++ {
		hash := tr.RandHash()
		_hashSlicePtr[i] = &hash
	}

	l = tr.RandIntInRange(0, 8)
	_bloomSlice := make([]types.Bloom, l)
	for i := 0; i < l; i++ {
		_bloomSlice[i] = tr.RandBloom()
	}

	l = tr.RandIntInRange(0, 8)
	_bloomSlicePtr := make([]*types.Bloom, l)
	for i := 0; i < l; i++ {
		bloom := tr.RandBloom()
		_bloomSlicePtr[i] = &bloom
	}

	enc := TestingStruct{
		a:  *tr.RandUint64(),
		aa: tr.RandUint64(),
		b:  *tr.RandBig(),
		bb: tr.RandBig(),
		c:  *tr.RandUint256(),
		cc: tr.RandUint256(),
		d:  types.BlockNonce(tr.RandBytes(8)),
		dd: (*types.BlockNonce)(tr.RandBytes(8)),
		e:  tr.RandAddress(),
		ee: (*common.Address)(tr.RandBytes(20)),
		f:  tr.RandHash(),
		ff: (*common.Hash)(tr.RandBytes(32)),
		g:  tr.RandBloom(),
		gg: (*types.Bloom)(tr.RandBytes(256)),
		h:  tr.RandBytes(tr.RandIntInRange(0, 128)),
		hh: &_byteSlice,
		i:  _byteSliceSlice,
		j:  _nonceSlice,
		jj: _nonceSlicePtr,
		k:  _addrSlice,
		kk: _addrSlicePtr,
		l:  _hashSlice,
		ll: _hashSlicePtr,
		m:  [10]byte(tr.RandBytes(10)),
		mm: (*[245]byte)(tr.RandBytes(245)),
	}
	return &enc
}

const RUNS = 1

func TestTestingStruct(t *testing.T) {
	tr := NewTRand()
	var buf bytes.Buffer
	for i := 0; i < RUNS; i++ {

		enc := randTestingStruct(tr)
		buf.Reset()

		if err := enc.EncodeRLP(&buf); err != nil {
			t.Errorf("error: TestingStruct.EncodeRLP(): %v", err)
		}

		s := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		dec := &TestingStruct{}
		if err := dec.DecodeRLP(s); err != nil {
			t.Errorf("error: TestingStruct.DecodeRLP(): %v", err)
			panic(err)
		}
		compareTestingStructs(t, enc, dec)
	}
}

func BenchmarkTestingStructRLP(b *testing.B) {
	tr := NewTRand()
	header := randTestingStruct(tr)
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		header.EncodeRLP(&buf)
	}
}
