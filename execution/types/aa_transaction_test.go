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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
)

// PreTransactionGasCost is the only caller that charges the AA per-auth cost, so
// a rule the intrinsic-gas switch reads and this call does not pass is a branch
// nothing can reach.
func TestAAPreTransactionGasCostChargesRevisedPerAuth(t *testing.T) {
	tx := &AccountAbstractionTransaction{Authorizations: make([]Authorization, 2)}
	rules := &chain.Rules{IsHomestead: true, IsIstanbul: true, IsPrague: true, IsAmsterdam: true}

	unrevised, err := tx.PreTransactionGasCost(rules, false)
	require.NoError(t, err)

	rules.EIP8038Revised = true
	revised, err := tx.PreTransactionGasCost(rules, false)
	require.NoError(t, err)

	require.Equal(t, 2*(params.AccountWriteCostEIP8038Revised-params.AccountWriteCostEIP8038), revised-unrevised)
}
