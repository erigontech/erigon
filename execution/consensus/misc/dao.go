// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package misc

import (
	"bytes"
	"errors"
	"math/big"

	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

var (
	// ErrBadProDAOExtra is returned if a header doesn't support the DAO fork on a
	// pro-fork client.
	ErrBadProDAOExtra = errors.New("bad DAO pro-fork extra-data")

	// ErrBadNoDAOExtra is returned if a header does support the DAO fork on a no-
	// fork client.
	ErrBadNoDAOExtra = errors.New("bad DAO no-fork extra-data")
)

// VerifyDAOHeaderExtraData validates the extra-data field of a block header to
// ensure it conforms to DAO hard-fork rules.
//
// DAO hard-fork extension to the header validity:
//
//	a) if the node is no-fork, do not accept blocks in the [fork, fork+10) range
//	   with the fork specific extra-data set
//	b) if the node is pro-fork, require blocks in the specific range to have the
//	   unique extra-data set.
func VerifyDAOHeaderExtraData(config *chain.Config, header *types.Header) error {
	// Short circuit validation if the node doesn't care about the DAO fork
	if config.DAOForkBlock == nil {
		return nil
	}
	// Make sure the block is within the fork's modified extra-data range
	limit := new(big.Int).Add(config.DAOForkBlock, DAOForkExtraRange)
	if header.Number.Cmp(config.DAOForkBlock) < 0 || header.Number.Cmp(limit) >= 0 {
		return nil
	}
	if !bytes.Equal(header.Extra, DAOForkBlockExtra) {
		return ErrBadProDAOExtra
	}
	// All ok, header has the same extra-data we expect
	return nil
}

// ApplyDAOHardFork modifies the state database according to the DAO hard-fork
// rules, transferring all balances of a set of DAO accounts to a single refund
// contract.
func ApplyDAOHardFork(statedb *state.IntraBlockState) error {
	// Retrieve the contract to refund balances into
	exist, err := statedb.Exist(DAORefundContract)
	if err != nil {
		return err
	}
	if !exist {
		statedb.CreateAccount(DAORefundContract, false)
	}

	// Move every DAO account and extra-balance account funds into the refund contract
	for _, addr := range DAODrainList() {
		balance, err := statedb.GetBalance(addr)
		if err != nil {
			return err
		}
		statedb.AddBalance(DAORefundContract, balance, tracing.BalanceIncreaseDaoContract)
		statedb.SetBalance(addr, *u256.N0, tracing.BalanceDecreaseDaoAccount)
	}
	return nil
}
