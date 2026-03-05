// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package chaos_monkey

import (
	"fmt"

	rand2 "golang.org/x/exp/rand"

	"github.com/erigontech/erigon/execution/protocol/rules"
)

const (
	consensusFailureRate = 300
)

func ThrowRandomConsensusError(IsInitialCycle bool, txIndex int, badBlockHalt bool, txTaskErr error) error {
	if !IsInitialCycle && rand2.Int()%consensusFailureRate == 0 && txIndex == 0 && !badBlockHalt {
		return fmt.Errorf("monkey in the datacenter: %w: %v", rules.ErrInvalidBlock, txTaskErr)
	}
	return nil
}
