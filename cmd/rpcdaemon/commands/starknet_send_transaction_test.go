package commands_test

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestErrorStarknetSendRawTransaction(t *testing.T) {
	var cases = []struct {
		name  string
		tx    string
		error error
	}{
		{name: "wrong tx type", tx: "0x02f86583127ed80180800180019637623232363136323639323233613230356235643764c080a0b44c2f4e18ca27e621171da5cf3a0c875c0749c7b998ec2759974280d987143aa04f01823122d972baa1a03b113535d9f9057fd9366fd8770e766b91f835b88ea6", error: commands.ErrOnlyStarknetTx},
		{name: "not contract creation", tx: "0x03f87b83127ed801010182ee489467b1d87101671b127f5f8714789c7192f7ad340e809637623232363136323639323233613230356235643764c080a0ceb955e6039bf37dbf77e4452a10b4a47906bbbd2f6dcf0c15bccb052d3bbb60a03de24d584a0a20523f55a137ebc651e2b092fbc3728d67c9fda09da9f0edd154", error: commands.ErrOnlyContractDeploy},
	}

	m, require := stages.MockWithTxPool(t), require.New(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	ff := filters.New(ctx, nil, txPool, txpool.NewMiningClient(conn))
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	for _, tt := range cases {
		noopTxPool := commands.NewNoopTxPoolClient()
		api := commands.NewStarknetAPI(commands.NewBaseApi(ff, stateCache, snapshotsync.NewBlockReader(), false), m.DB, noopTxPool)

		t.Run(tt.name, func(t *testing.T) {
			hex, _ := hexutil.Decode(tt.tx)

			_, err := api.SendRawTransaction(ctx, hex)

			require.ErrorIs(err, tt.error)
			require.Len(noopTxPool.AddCalls, 0)
		})
	}
}
