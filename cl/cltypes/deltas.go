package cltypes

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
)

// Deltas data, for https://github.com/ethereum/consensus-specs/tree/master/tests/formats/rewards
type Deltas struct {
	Rewards   []uint64
	Penalties []uint64
}

func (f *Deltas) Clone() clonable.Clonable {
	o := &Deltas{}
	o.Rewards = make([]uint64, len(f.Rewards))
	copy(o.Rewards, f.Rewards)
	o.Penalties = make([]uint64, len(f.Penalties))
	copy(o.Penalties, f.Penalties)
	return o
}

func (f *Deltas) EncodeSSZ(dst []byte) ([]byte, error) {
	return nil, nil
}

func (d *Deltas) DecodeSSZ(buf []byte, version int) error {
	if len(buf) < d.EncodingSizeSSZ() {
		return fmt.Errorf("[Deltas] err; %s", ssz.ErrLowBufferSize)
	}
	o1, o2 := ssz.DecodeOffset(buf[0:4]), ssz.DecodeOffset(buf[4:8])

	tail := buf

	{
		buf = tail[o1:o2]
		num, err := divideInt2(len(buf), 8, 1099511627776)
		if err != nil {
			return err
		}
		d.Rewards = extendUint64(d.Rewards, num)
		for ii := 0; ii < num; ii++ {
			d.Rewards[ii] = ssz.UnmarshalUint64SSZ(buf[ii*8 : (ii+1)*8])
		}
	}

	{
		buf = tail[o2:]
		num, err := divideInt2(len(buf), 8, 1099511627776)
		if err != nil {
			return err
		}
		d.Penalties = extendUint64(d.Penalties, num)
		for ii := 0; ii < num; ii++ {
			d.Penalties[ii] = ssz.UnmarshalUint64SSZ(buf[ii*8 : (ii+1)*8])
		}
	}

	return nil
}

func (f *Deltas) EncodingSizeSSZ() int {
	return 8
}

func (f *Deltas) HashSSZ() ([32]byte, error) {
	return [32]byte{}, nil
}

func divideInt2(a, b, max int) (int, error) {
	num, ok := divideInt(a, b)
	if !ok {
		return 0, fmt.Errorf("xx")
	}
	if num > max {
		return 0, fmt.Errorf("yy")
	}
	return num, nil
}

// DivideInt divides the int fully
func divideInt(a, b int) (int, bool) {
	return a / b, a%b == 0
}
func extendUint64(b []uint64, needLen int) []uint64 {
	if b == nil {
		b = []uint64{}
	}
	b = b[:cap(b)]
	if n := needLen - cap(b); n > 0 {
		b = append(b, make([]uint64, n)...)
	}
	return b[:needLen]
}
