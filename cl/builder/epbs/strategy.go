package epbs

import "math/big"

// BidStrategy decides how much to bid for a given slot.
// Decide returns the bid amount, or nil to skip the slot.
type BidStrategy interface {
	Decide(slot uint64, blockValue *big.Int) *big.Int
}
