package commands

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/stretchr/testify/require"
)

func TestGetContractCreator(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB)

	addr := libcommon.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	expectCreator := libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	expectCredByTx := libcommon.HexToHash("0x6e25f89e24254ba3eb460291393a4715fd3c33d805334cbd05c1b2efe1080f18")
	t.Run("valid inputs", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, addr)
		require.NoError(err)
		require.Equal(expectCreator, results.Creator)
		require.Equal(expectCredByTx, results.Tx)
	})
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, libcommon.HexToAddress("0x1234"))
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
