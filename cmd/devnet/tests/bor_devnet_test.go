//go:build integration

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	accounts_steps "github.com/ledgerwatch/erigon/cmd/devnet/accounts/steps"
	contracts_steps "github.com/ledgerwatch/erigon/cmd/devnet/contracts/steps"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
)

func TestStateSync(t *testing.T) {
	t.Skip()
	runCtx, err := contextStart(t, networkname.BorDevnetChainName)
	require.Nil(t, err)
	var ctx context.Context = runCtx

	t.Run("InitSubscriptions", func(t *testing.T) {
		services.InitSubscriptions(withTestLogger(ctx, t), []requests.SubMethod{requests.Methods.ETHNewHeads})
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(withTestLogger(ctx, t), networkname.DevChainName, "root-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(withTestLogger(ctx, t), networkname.BorDevnetChainName, "child-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("DeployChildChainReceiver", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployChildChainReceiver(withTestLogger(ctx, t), "child-funder")
		require.Nil(t, err)
	})
	t.Run("DeployRootChainSender", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployRootChainSender(withTestLogger(ctx, t), "root-funder")
		require.Nil(t, err)
	})
	t.Run("GenerateSyncEvents", func(t *testing.T) {
		require.Nil(t, contracts_steps.GenerateSyncEvents(withTestLogger(ctx, t), "root-funder", 10, 2, 2))
	})
	t.Run("ProcessRootTransfers", func(t *testing.T) {
		require.Nil(t, contracts_steps.ProcessRootTransfers(withTestLogger(ctx, t), "root-funder", 10, 2, 2))
	})
	t.Run("BatchProcessRootTransfers", func(t *testing.T) {
		require.Nil(t, contracts_steps.BatchProcessRootTransfers(withTestLogger(ctx, t), "root-funder", 1, 10, 2, 2))
	})
}

func TestChildChainExit(t *testing.T) {
	t.Skip()
	runCtx, err := contextStart(t, networkname.BorDevnetChainName)
	require.Nil(t, err)
	var ctx context.Context = runCtx

	t.Run("InitSubscriptions", func(t *testing.T) {
		services.InitSubscriptions(withTestLogger(ctx, t), []requests.SubMethod{requests.Methods.ETHNewHeads})
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(withTestLogger(ctx, t), networkname.DevChainName, "root-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("CreateAccountWithFunds", func(t *testing.T) {
		_, err := accounts_steps.CreateAccountWithFunds(withTestLogger(ctx, t), networkname.BorDevnetChainName, "child-funder", 200.0)
		require.Nil(t, err)
	})
	t.Run("DeployRootChainReceiver", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployRootChainReceiver(withTestLogger(ctx, t), "root-funder")
		require.Nil(t, err)
	})
	t.Run("DeployChildChainSender", func(t *testing.T) {
		var err error
		ctx, err = contracts_steps.DeployChildChainSender(withTestLogger(ctx, t), "child-funder")
		require.Nil(t, err)
	})
	t.Run("ProcessChildTransfers", func(t *testing.T) {
		require.Nil(t, contracts_steps.ProcessChildTransfers(withTestLogger(ctx, t), "child-funder", 1, 2))
	})
}
