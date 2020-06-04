package vm

import (
	"fmt"

	"github.com/holiman/uint256"
)

type SaStackItem byte {
	Constant = iota // Constant value that is known even before executing a transaction to the contract
	Input           // Value which is completely determined by the transaction input (i.e. does not come from the state)
	Dynamic         // Value coming from the state, or from memory (if heuristic is not strong enough to track memory)
	Unified         // Value that was constant, but it changes upon re-entering the same code
}

// Variation of the stack that is used for static analysis
type SaStack struct {
	data  []uint256.Int
	kinds []SaStackItem
}

func newSaStack() *SaStack {
	return &SaStack{}
}