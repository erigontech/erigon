//go:build integration

package bor

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	accounts_steps "github.com/ledgerwatch/erigon/cmd/devnet/accounts/steps"
	contracts_steps "github.com/ledgerwatch/erigon/cmd/devnet/contracts/steps"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/cmd/devnet/tests"
	"github.com/stretchr/testify/require"
)

func TestStateSync(t *testing.T) {
	t.Skip("FIXME: hangs in GenerateSyncEvents without any visible progress")

	runCtx, err := tests.ContextStart(t, networkname.BorDevnetChainName)
	require.Nil(t, err)
	var ctx context.Context = runCtx

	t.Run("InitSubscriptions", func(t *testing.T) {
		services.InitSubscriptions(ctx, []requests.SubMethod{requests.Methods.ETHNewHeads})
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.DevChainName, "root-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.BorDevnetChainName, "child-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("DeployChildChainReceiver", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployChildChainReceiver(ctx, "child-funder")
		require.Nil(t, err)
	})
	t.Run("DeployRootChainSender", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployRootChainSender(ctx, "root-funder")
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

	runCtx, err := tests.ContextStart(t, networkname.BorDevnetChainName)
	require.Nil(t, err)
	var ctx context.Context = runCtx

	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.DevChainName, "root-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(ctx, networkname.BorDevnetChainName, "child-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("DeployRootChainReceiver", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployRootChainReceiver(ctx, "root-funder")
		require.Nil(t, err)
	})
	t.Run("DeployChildChainSender", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployChildChainSender(ctx, "child-funder")
		require.Nil(t, err)
	})
	t.Run("ProcessChildTransfers", func(t *testing.T) {
		require.Nil(t, contracts_steps.ProcessChildTransfers(ctx, "child-funder", 1, 2, 2))
	})
	//t.Run("BatchProcessTransfers", func(t *testing.T) {
	//	require.Nil(t, contracts_steps.BatchProcessTransfers(ctx, "child-funder", 1, 10, 2, 2))
	//})
}
