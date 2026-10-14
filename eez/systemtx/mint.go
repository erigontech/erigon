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
package systemtx

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/eez/protocol"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// mintHook credits a system transaction's value to the system address before
// the balance check and removes it again when the outer call fails. Nonce and
// gas stay consumed either way.
type mintHook struct{}

func (mintHook) BeforeExecute(ibs evmtypes.IntraBlockState, msg execprotocol.Message) error {
	value := msg.Value()
	if value.IsZero() {
		return nil
	}
	balance, err := ibs.GetBalance(msg.From())
	if err != nil {
		return err
	}
	if _, overflow := new(uint256.Int).AddOverflow(&balance, value); overflow {
		return fmt.Errorf("%w: system address balance overflows", protocol.ErrSystemTx)
	}
	return ibs.AddBalance(msg.From(), *value, tracing.BalanceChangeUnspecified)
}

func (mintHook) AfterExecute(ibs evmtypes.IntraBlockState, msg execprotocol.Message, failed bool) error {
	value := msg.Value()
	if !failed || value.IsZero() {
		return nil
	}
	// The failed call reverted its own transfers; only the mint is left.
	return ibs.SubBalance(msg.From(), *value, tracing.BalanceChangeUnspecified)
}
