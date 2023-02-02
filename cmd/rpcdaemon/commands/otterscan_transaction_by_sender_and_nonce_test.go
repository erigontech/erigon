package commands

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/stretchr/testify/require"
)

func TestGetTransactionBySenderAndNonce(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots, m.TransactionsV3)
	api := NewOtterscanAPI(NewBaseApi(nil, nil, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), m.DB)

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	expectCreator := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	expectCredByTx := common.HexToHash("0x6e25f89e24254ba3eb460291393a4715fd3c33d805334cbd05c1b2efe1080f18")
	_, _, _ = addr, expectCreator, expectCredByTx
	t.Run("valid input", func(t *testing.T) {
		require := require.New(t)
		reply, err := api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 0)
		require.NoError(err)
		expectTxHash := common.HexToHash("0x3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea")
		require.Equal(&expectTxHash, reply)

		reply, err = api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 1)
		require.NoError(err)
		expectTxHash = common.HexToHash("0xcdc63ba35b09f6667f179e271ece766a6ec00a07673c0cf1e7d4e8feb1697566")
		require.Equal(&expectTxHash, reply)

		// skip others...

		reply, err = api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 38)
		require.NoError(err)
		expectTxHash = common.HexToHash("0xb6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059")
		require.Equal(&expectTxHash, reply)

		//reply, err = api.GetTransactionBySenderAndNonce(m.Ctx, expectCreator, 39)
		//require.NoError(err)
		//require.Nil(reply)
	})
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, common.HexToAddress("0x1234"))
		require.NoError(err)
		require.Nil(results)
	})
	t.Run("pass creator as addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, expectCreator)
		require.NoError(err)
		require.Nil(results)
	})
}
