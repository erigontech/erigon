package stack

import (
	"github.com/holiman/uint256"
)

func (st *Stack) PeekAt(position int) *uint256.Int {
	return &st.Data[st.Len()-position]
}
