package epbs

import "math/big"

// BidStrategy determines the bid for a built payload or declines the slot.
type BidStrategy interface {
	Decide(slot uint64, blockValue *big.Int) *big.Int
}
