package commands_test

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

func TestErrorStarknetSendRawTransaction(t *testing.T) {
	var cases = []struct {
		name  string
		tx    string
		error error
	}{
		{name: "wrong tx type", tx: generateDynamicFeeTransaction(), error: commands.ErrOnlyStarknetTx},
		{name: "not contract creation", tx: generateStarknetTransaction(), error: commands.ErrOnlyContractDeploy},
	}

	m, require := stages.MockWithTxPool(t), require.New(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	starknetClient := starknet.NewCAIROVMClient(conn)
	ff := rpchelper.New(ctx, nil, txPool, txpool.NewMiningClient(conn), func() {})
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	for _, tt := range cases {
		api := commands.NewStarknetAPI(commands.NewBaseApi(ff, stateCache, snapshotsync.NewBlockReader(), nil, nil, false), m.DB, starknetClient, txPool)

		t.Run(tt.name, func(t *testing.T) {
			hex, _ := hexutil.Decode(tt.tx)

			_, err := api.SendRawTransaction(ctx, hex)

			require.ErrorIs(err, tt.error)
		})
	}
}

func generateDynamicFeeTransaction() string {
	buf := bytes.NewBuffer(nil)
	types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			ChainID: new(uint256.Int),
			Nonce:   1,
			Value:   uint256.NewInt(1),
			Gas:     1,
		},
		Tip:    new(uint256.Int),
		FeeCap: new(uint256.Int),
	}.MarshalBinary(buf)

	return hexutil.Encode(buf.Bytes())
}

func generateStarknetTransaction() string {
	buf := bytes.NewBuffer(nil)
	types.StarknetTransaction{
		CommonTx: types.CommonTx{
			ChainID: new(uint256.Int),
			Nonce:   1,
			Value:   uint256.NewInt(1),
			Gas:     1,
			To:      &common.Address{},
		},
		Tip:    new(uint256.Int),
		FeeCap: new(uint256.Int),
	}.MarshalBinary(buf)

	return hexutil.Encode(buf.Bytes())
}
