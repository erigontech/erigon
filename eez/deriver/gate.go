// Copyright 2026 The Erigon Authors
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

package deriver

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

// ErrOutboundUnauthorized reports an outbound entry L1 paid that no call of
// the Sync block made.
var ErrOutboundUnauthorized = errors.New("outbound entry without a matching L2 call")

// VerifyOutbound checks that each outbound entry has its own
// CrossChainCallExecuted event among logs, emitted by EEZL2 with zero call
// gas, so that a signed transaction of the Sync block made every L2-to-L1
// call L1 settled.
func VerifyOutbound(entries []protocol.ExecutionEntry, logs []*types.Log, rollupID uint64) error {
	available := map[common.Hash]int{}
	for _, l := range logs {
		if l.Address != protocol.EEZL2Address {
			continue
		}
		e, err := protocol.ParseL2CrossChainCallExecuted(l)
		if err != nil {
			continue
		}
		if e.CallGas != 0 {
			return fmt.Errorf("%w: call %x with call gas %d", ErrOutboundUnauthorized, e.CrossChainCallHash, e.CallGas)
		}
		available[e.CrossChainCallHash]++
	}
	for i := range entries {
		e := &entries[i]
		if e.ProxyEntryHash != (common.Hash{}) || len(e.L2ToL1Calls) != 1 {
			return fmt.Errorf("%w: entry %d is not an outbound call", ErrOutboundUnauthorized, i)
		}
		if err := protocol.CheckEntryShape(e); err != nil {
			return fmt.Errorf("%w: entry %d: %w", ErrOutboundUnauthorized, i, err)
		}
		c := &e.L2ToL1Calls[0]
		if c.SourceRollupID != rollupID {
			return fmt.Errorf("%w: entry %d leaves rollup %d, not %d", ErrOutboundUnauthorized, i, c.SourceRollupID, rollupID)
		}
		h, err := protocol.CrossChainCallHash(&protocol.CallHashInput{
			SourceAddress:  c.SourceAddress,
			SourceRollupID: rollupID,
			TargetAddress:  c.TargetAddress,
			TargetRollupID: protocol.MainnetRollupID,
			Value:          c.Value,
			Data:           c.Data,
		})
		if err != nil {
			return fmt.Errorf("%w: entry %d: %w", ErrOutboundUnauthorized, i, err)
		}
		if available[h] == 0 {
			return fmt.Errorf("%w: entry %d (call %x to %x)", ErrOutboundUnauthorized, i, h, c.TargetAddress)
		}
		available[h]--
	}
	return nil
}
