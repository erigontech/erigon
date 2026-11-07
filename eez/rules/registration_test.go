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
package rules

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/rulesconfig"
)

// TestMain checks, before any test registers it, that the engine is not
// registered on import.
func TestMain(m *testing.M) {
	if engineRegistered() {
		fmt.Fprintln(os.Stderr, "eez rules engine registered before Register")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func engineRegistered() (registered bool) {
	defer func() { registered = recover() == nil }()
	rulesconfig.CreateRulesEngineBareBones(context.Background(), networks.Devnet.Config, log.New())
	return
}

func TestEngineSelectedForDevnet(t *testing.T) {
	config.Register()
	Register()
	Register()
	e := rulesconfig.CreateRulesEngineBareBones(t.Context(), networks.Devnet.Config, log.New())
	require.IsType(t, &Engine{}, e)

	// After a restart the chain config comes back from the database.
	_, tx := mdbxtest.NewTestTx(t)
	require.NoError(t, rawdb.WriteChainConfig(tx, networks.Devnet.GenesisHash, networks.Devnet.Config.Copy()))
	stored, err := rawdb.ReadChainConfig(tx, networks.Devnet.GenesisHash)
	require.NoError(t, err)
	require.Equal(t, networks.Devnet.Config.L2, stored.L2)
	require.IsType(t, &Engine{}, rulesconfig.CreateRulesEngineBareBones(t.Context(), stored, log.New()))
	require.Equal(t, config.Name, stored.L2.Name())
}

func TestIsServiceTransaction(t *testing.T) {
	e, err := New(networks.Devnet.Config)
	require.NoError(t, err)
	require.True(t, e.IsServiceTransaction(accounts.InternAddress(protocol.SystemAddress), nil))
	require.False(t, e.IsServiceTransaction(accounts.InternAddress(common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe")), nil))
}

func TestValidateBlockPostExecution(t *testing.T) {
	e, err := New(networks.Devnet.Config)
	require.NoError(t, err)
	var zero uint64
	header := &types.Header{BlobGasUsed: &zero}
	validate := func(txns types.Transactions) error {
		return e.ValidateBlockPostExecution(networks.Devnet.Config, header, 0, 0, false, false, nil, txns, log.New())
	}
	require.NoError(t, validate(types.Transactions{&types.DynamicFeeTransaction{}}))
	require.ErrorIs(t, validate(types.Transactions{&types.DynamicFeeTransaction{}, &types.BlobTx{}}), rules.ErrInvalidBlock)
}
