package state

import (
	"sync"

	"github.com/holiman/uint256"
)

// BalanceIncrease represents the Increase of balance of an account that did not require
// reading the account first
type BalanceIncrease struct {
	Increase    uint256.Int
	Transferred bool // Set to true when the corresponding stateObject is created and balance Increase is transferred to the stateObject
	Count       int  // Number of increases - this needs tracking for proper reversion
}

func NewBalanceIncrease() *BalanceIncrease {
	bi := balanceIncreasePool.Get().(*BalanceIncrease)
	bi.Increase.Clear()
	bi.Transferred = false
	bi.Count = 0
	return bi
}
func ReturnBalanceIncreaseToPool(v *BalanceIncrease) { balanceIncreasePool.Put(v) }

var balanceIncreasePool = sync.Pool{
	New: func() interface{} { return &BalanceIncrease{} },
}
