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

//go:build integration

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain/networkname"
	accounts_steps "github.com/erigontech/erigon/cmd/devnet/accounts/steps"
	contracts_steps "github.com/erigontech/erigon/cmd/devnet/contracts/steps"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/cmd/devnet/services"
)

func TestStateSync(t *testing.T) {
	t.Skip()

	runCtx, err := ContextStart(t, networkname.BorDevnet)
	require.Nil(t, err)
	var ctx context.Context = runCtx

	t.Run("InitSubscriptions", func(t *testing.T) {
		services.InitSubscriptions(ctx, []requests.SubMethod{requests.Methods.ETHNewHeads})
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.Dev, "root-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.BorDevnet, "child-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("DeployChildChainReceiver", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployChildChainReceiver(ctx, "child-funder") //nolint
		require.Nil(t, err)
	})
	t.Run("DeployRootChainSender", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployRootChainSender(ctx, "root-funder") //nolint
		require.Nil(t, err)
	})
	t.Run("GenerateSyncEvents", func(t *testing.T) {
		require.Nil(t, contracts_steps.GenerateSyncEvents(ctx, "root-funder", 10, 2, 2))
	})
	t.Run("ProcessRootTransfers", func(t *testing.T) {
		require.Nil(t, contracts_steps.ProcessRootTransfers(ctx, "root-funder", 10, 2, 2))
	})
	t.Run("BatchProcessRootTransfers", func(t *testing.T) {
		require.Nil(t, contracts_steps.BatchProcessRootTransfers(ctx, "root-funder", 1, 10, 2, 2))
	})
}

func TestChildChainExit(t *testing.T) {
	t.Skip("FIXME: step CreateAccountWithFunds fails: Failed to get transfer tx: failed to search reserves for hashes: no block heads subscription")

	runCtx, err := ContextStart(t, networkname.BorDevnet)
	require.Nil(t, err)
	var ctx context.Context = runCtx

	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.Dev, "root-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.BorDevnet, "child-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("DeployRootChainReceiver", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployRootChainReceiver(ctx, "root-funder") //nolint
		require.Nil(t, err)
	})
	t.Run("DeployChildChainSender", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployChildChainSender(ctx, "child-funder") //nolint
		require.Nil(t, err)
	})
	t.Run("ProcessChildTransfers", func(t *testing.T) {
		require.Nil(t, contracts_steps.ProcessChildTransfers(ctx, "child-funder", 1, 2, 2))
	})
	//t.Run("BatchProcessTransfers", func(t *testing.T) {
	//	require.Nil(t, contracts_steps.BatchProcessTransfers(ctx, "child-funder", 1, 10, 2, 2))
	//})
}
