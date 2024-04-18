//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/admin"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts/steps"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/cmd/devnet/transactions"
)

func testDynamicTx(t *testing.T, ctx context.Context) {
	t.Run("InitSubscriptions", func(t *testing.T) {
		services.InitSubscriptions(ctx, []requests.SubMethod{requests.Methods.ETHNewHeads})
	})
	t.Run("PingErigonRpc", func(t *testing.T) {
		require.Nil(t, admin.PingErigonRpc(ctx))
	})
	t.Run("CheckTxPoolContent", func(t *testing.T) {
		transactions.CheckTxPoolContent(ctx, 0, 0, 0)
	})
	t.Run("SendTxWithDynamicFee", func(t *testing.T) {
		const recipientAddress = "0x71562b71999873DB5b286dF957af199Ec94617F7"
		const sendValue uint64 = 10000
		_, err := transactions.SendTxWithDynamicFee(ctx, recipientAddress, accounts.DevAddress, sendValue)
		require.Nil(t, err)
	})
	t.Run("AwaitBlocks", func(t *testing.T) {
		require.Nil(t, transactions.AwaitBlocks(ctx, 2*time.Second))
	})
}

func TestDynamicTxNode0(t *testing.T) {
	t.Skip()

	runCtx, err := ContextStart(t, "")
	require.Nil(t, err)
	testDynamicTx(t, runCtx.WithCurrentNetwork(0).WithCurrentNode(0))
}

func TestDynamicTxAnyNode(t *testing.T) {
	t.Skip()

	runCtx, err := ContextStart(t, "")
	require.Nil(t, err)
	testDynamicTx(t, runCtx.WithCurrentNetwork(0))
}

func TestCallContract(t *testing.T) {
	t.Skip()

	runCtx, err := ContextStart(t, "")
	require.Nil(t, err)
	ctx := runCtx.WithCurrentNetwork(0)

	t.Run("InitSubscriptions", func(t *testing.T) {
		services.InitSubscriptions(ctx, []requests.SubMethod{requests.Methods.ETHNewHeads})
	})
	t.Run("DeployAndCallLogSubscriber", func(t *testing.T) {
		_, err := contracts_steps.DeployAndCallLogSubscriber(ctx, accounts.DevAddress)
		require.Nil(t, err)
	})
}
