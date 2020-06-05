package pool

import (
	"sync"

	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
)

var StackPool = NewStack()

type Stack struct {
	*sync.Pool
}

const maxCap = 1024 * 2

func NewStack() *Stack {
	return &Stack{
		&sync.Pool{
			New: func() interface{} {
				return stack.New(maxCap)
			},
		},
	}
}

func (p *Stack) Get() *stack.Stack {
	return p.Pool.Get().(*stack.Stack)
}

func (p *Stack) Put(s *stack.Stack) {
	if s == nil || s.Cap() == 0 || s.Cap() > maxCap {
		return
	}

	s.Reset()
	p.Pool.Put(s)
}
