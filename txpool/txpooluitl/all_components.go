package txpooluitl

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/log/v3"
)

func AllComponents(ctx context.Context, cfg txpool.Config, newTxs chan txpool.Hashes, coreDB kv.RoDB, sentryClients []direct.SentryClient, stateChangesClient txpool.StateChangesClient) (kv.RwDB, *txpool.TxPool, *txpool.Fetch, *txpool.Send, *txpool.GrpcServer, error) {
	txPoolDB, err := mdbx.NewMDBX(log.New()).Label(kv.TxPoolDB).Path(cfg.DBDir).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).Open()
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	txPool, err := txpool.New(newTxs, coreDB, cfg)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	fetch := txpool.NewFetch(ctx, sentryClients, txPool, stateChangesClient, coreDB, txPoolDB)
	//fetch.ConnectCore()
	//fetch.ConnectSentries()

	send := txpool.NewSend(ctx, sentryClients, txPool)
	txpoolGrpcServer := txpool.NewGrpcServer(ctx, txPool, txPoolDB)
	return txPoolDB, txPool, fetch, send, txpoolGrpcServer, nil
}
