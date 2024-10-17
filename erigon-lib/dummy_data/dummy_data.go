package dummydata

import (
	"math/big"
	"math/rand"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
)

type DummyData struct {
	rnd *rand.Rand
}

func NewTRand() *DummyData {
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	return &DummyData{rnd: rand.New(src)}
}

func (tr *DummyData) RandIntInRange(min, max int) int {
	return (tr.rnd.Intn(max-min) + min)
}

func (tr *DummyData) RandUint64() *uint64 {
	a := tr.rnd.Uint64()
	return &a
}

func (tr *DummyData) RandBig() *big.Int {
	return big.NewInt(int64(tr.rnd.Int()))
}

func (tr *DummyData) RandBytes(size int) []byte {
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		arr[i] = byte(tr.rnd.Intn(256))
	}
	return arr
}

func (tr *DummyData) RandAddress() common.Address {
	return common.Address(tr.RandBytes(20))
}

func (tr *DummyData) RandHash() common.Hash {
	return common.Hash(tr.RandBytes(32))
}
