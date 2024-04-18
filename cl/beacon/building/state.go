package building

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
)

type State struct {
	feeRecipients map[int]common.Address

	mu sync.RWMutex
}

func NewState() *State {
	return &State{
		feeRecipients: map[int]common.Address{},
	}
}

func (s *State) SetFeeRecipient(idx int, address common.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.feeRecipients[idx] = address
}
