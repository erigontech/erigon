package state

import (
	"os"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// Probe knob E1_SLOTMEMO=1: one IBS-wide record per slot (value, and warm because it was read) checked before the
// per-SLOAD lookups; reverts are ignored. Bound only, not correct.
var e1On = os.Getenv("E1_SLOTMEMO") == "1"

type e1Key struct {
	addr accounts.Address
	key  accounts.StorageKey
}

func (ibs *IntraBlockState) e1Get(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool) {
	v, ok := ibs.e1[e1Key{addr, key}]
	return v, ok
}

func (ibs *IntraBlockState) e1Put(addr accounts.Address, key accounts.StorageKey, v uint256.Int) {
	if ibs.e1 == nil {
		ibs.e1 = map[e1Key]uint256.Int{}
	}
	ibs.e1[e1Key{addr, key}] = v
}

// E1Warm reports a slot the probe memo holds; E1 treats it as warm.
func (ibs *IntraBlockState) E1Warm(addr accounts.Address, key accounts.StorageKey) bool {
	if !e1On {
		return false
	}
	_, ok := ibs.e1[e1Key{addr, key}]
	return ok
}
