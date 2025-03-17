package chaos_monkey

import (
	"fmt"
	rand2 "math/rand/v2"

	"github.com/erigontech/erigon/consensus"
)

const (
	consensusFailureRate = 300
)

func ThrowRandomConsensusError(IsInitialCycle bool, txIndex int, badBlockHalt bool, txTaskErr error) error {
	if !IsInitialCycle && rand2.Int()%consensusFailureRate == 0 && txIndex == 0 && !badBlockHalt {
		return fmt.Errorf("monkey in the datacenter: %w: %v", consensus.ErrInvalidBlock, txTaskErr)
	}
	return nil
}
