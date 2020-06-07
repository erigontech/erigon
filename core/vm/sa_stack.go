package vm

import (
	"github.com/holiman/uint256"
)

type SaStackItem byte

const (
	Constant SaStackItem = iota // Constant value that is known even before executing a transaction to the contract
	Input           // Value which is completely determined by the transaction input (i.e. does not come from the state)
	Dynamic         // Value coming from the state, or from memory (if heuristic is not strong enough to track memory)
	Unified         // Value that was constant, but it changes upon re-entering the same code
)

// Variation of the stack that is used for static analysis
type SaStack struct {
	data  []uint256.Int
	kinds []SaStackItem
}

func newSaStack() *SaStack {
	return &SaStack{}
}

func (st *SaStack) Push(d *uint256.Int) {
	// NOTE push limit (1024) is checked in baseCheck
	st.data = append(st.data, *d)
}

func (st *SaStack) pop() (ret uint256.Int, kind SaStackItem) {
	ret = st.data[len(st.data)-1]
	kind = st.kinds[len(st.kinds)-1]
	st.data = st.data[:len(st.data)-1]
	st.kinds = st.kinds[:len(st.kinds)-1]
	return
}

func (st *SaStack) len() int {
	return len(st.data)
}

func (st *SaStack) peek() (*uint256.Int, SaStackItem) {
	p := st.len()-1
	return &st.data[p], st.kinds[p]
}

// Back returns the n'th item in stack
func (st *SaStack) Back(n int) (*uint256.Int, SaStackItem) {
	p := st.len()-n-1
	return &st.data[p], st.kinds[p]
}

func (st *SaStack) setKind(n int, kind SaStackItem) {
	p := st.len()-n-1
	st.kinds[p] = kind
}