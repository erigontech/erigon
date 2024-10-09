package jsonrpc

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
)

// TxPoolAPI the interface for the txpool_ RPC commands
type TxPoolAPI interface {
	Content(ctx context.Context) (interface{}, error)
	ContentFrom(ctx context.Context, addr libcommon.Address) (map[string]map[string]*RPCTransaction, error)
	Limbo(ctx context.Context) (interface{}, error)
}

// TxPoolAPIImpl data structure to store things needed for net_ commands
type TxPoolAPIImpl struct {
	*BaseAPI
	pool     proto_txpool.TxpoolClient
	db       kv.RoDB
	l2RPCUrl string
	rawPool  *txpool.TxPool
}

// NewTxPoolAPI returns NetAPIImplImpl instance
func NewTxPoolAPI(base *BaseAPI, db kv.RoDB, pool proto_txpool.TxpoolClient, rawPool *txpool.TxPool, l2RPCUrl string) *TxPoolAPIImpl {
	return &TxPoolAPIImpl{
		BaseAPI:  base,
		pool:     pool,
		db:       db,
		l2RPCUrl: l2RPCUrl,
		rawPool:  rawPool,
	}
}

func (api *TxPoolAPIImpl) Content(ctx context.Context) (interface{}, error) {
	if api.l2RPCUrl != "" {
		res, err := client.JSONRPCCall(api.l2RPCUrl, "txpool_content")
		if err != nil {
			return nil, err
		}
		return res.Result, nil
	}

	reply, err := api.pool.All(ctx, &proto_txpool.AllRequest{})
	if err != nil {
		return nil, err
	}

	content := map[string]map[string]map[string]*RPCTransaction{
		"pending": make(map[string]map[string]*RPCTransaction),
		"baseFee": make(map[string]map[string]*RPCTransaction),
		"queued":  make(map[string]map[string]*RPCTransaction),
	}

	pending := make(map[libcommon.Address][]types.Transaction, 8)
	baseFee := make(map[libcommon.Address][]types.Transaction, 8)
	queued := make(map[libcommon.Address][]types.Transaction, 8)
	for i := range reply.Txs {
		txn, err := types.DecodeWrappedTransaction(reply.Txs[i].RlpTx)
		if err != nil {
			return nil, fmt.Errorf("decoding transaction from: %x: %w", reply.Txs[i].RlpTx, err)
		}
		addr := gointerfaces.ConvertH160toAddress(reply.Txs[i].Sender)
		switch reply.Txs[i].TxnType {
		case proto_txpool.AllReply_PENDING:
			if _, ok := pending[addr]; !ok {
				pending[addr] = make([]types.Transaction, 0, 4)
			}
			pending[addr] = append(pending[addr], txn)
		case proto_txpool.AllReply_BASE_FEE:
			if _, ok := baseFee[addr]; !ok {
				baseFee[addr] = make([]types.Transaction, 0, 4)
			}
			baseFee[addr] = append(baseFee[addr], txn)
		case proto_txpool.AllReply_QUEUED:
			if _, ok := queued[addr]; !ok {
				queued[addr] = make([]types.Transaction, 0, 4)
			}
			queued[addr] = append(queued[addr], txn)
		}
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cc, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	curHeader := rawdb.ReadCurrentHeader(tx)
	if curHeader == nil {
		return nil, nil
	}
	// Flatten the pending transactions
	for account, txs := range pending {
		dump := make(map[string]*RPCTransaction)
		for _, txn := range txs {
			dump[fmt.Sprintf("%d", txn.GetNonce())] = newRPCPendingTransaction(txn, curHeader, cc)
		}
		content["pending"][account.Hex()] = dump
	}
	// Flatten the baseFee transactions
	for account, txs := range baseFee {
		dump := make(map[string]*RPCTransaction)
		for _, txn := range txs {
			dump[fmt.Sprintf("%d", txn.GetNonce())] = newRPCPendingTransaction(txn, curHeader, cc)
		}
		content["baseFee"][account.Hex()] = dump
	}
	// Flatten the queued transactions
	for account, txs := range queued {
		dump := make(map[string]*RPCTransaction)
		for _, txn := range txs {
			dump[fmt.Sprintf("%d", txn.GetNonce())] = newRPCPendingTransaction(txn, curHeader, cc)
		}
		content["queued"][account.Hex()] = dump
	}
	return content, nil
}

func (api *TxPoolAPIImpl) ContentFrom(ctx context.Context, addr libcommon.Address) (map[string]map[string]*RPCTransaction, error) {
	reply, err := api.pool.All(ctx, &proto_txpool.AllRequest{})
	if err != nil {
		return nil, err
	}

	content := map[string]map[string]*RPCTransaction{
		"pending": make(map[string]*RPCTransaction),
		"baseFee": make(map[string]*RPCTransaction),
		"queued":  make(map[string]*RPCTransaction),
	}

	pending := make([]types.Transaction, 0, 4)
	baseFee := make([]types.Transaction, 0, 4)
	queued := make([]types.Transaction, 0, 4)
	for i := range reply.Txs {
		txn, err := types.DecodeWrappedTransaction(reply.Txs[i].RlpTx)
		if err != nil {
			return nil, fmt.Errorf("decoding transaction from: %x: %w", reply.Txs[i].RlpTx, err)
		}
		sender := gointerfaces.ConvertH160toAddress(reply.Txs[i].Sender)
		if sender != addr {
			continue
		}

		switch reply.Txs[i].TxnType {
		case proto_txpool.AllReply_PENDING:
			pending = append(pending, txn)
		case proto_txpool.AllReply_BASE_FEE:
			baseFee = append(baseFee, txn)
		case proto_txpool.AllReply_QUEUED:
			queued = append(queued, txn)
		}
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cc, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	curHeader := rawdb.ReadCurrentHeader(tx)
	if curHeader == nil {
		return nil, nil
	}
	// Flatten the pending transactions
	dump := make(map[string]*RPCTransaction)
	for _, txn := range pending {
		dump[fmt.Sprintf("%d", txn.GetNonce())] = newRPCPendingTransaction(txn, curHeader, cc)
	}
	content["pending"] = dump
	// Flatten the baseFee transactions
	dump = make(map[string]*RPCTransaction)
	for _, txn := range baseFee {
		dump[fmt.Sprintf("%d", txn.GetNonce())] = newRPCPendingTransaction(txn, curHeader, cc)
	}
	content["baseFee"] = dump
	// Flatten the queued transactions
	dump = make(map[string]*RPCTransaction)
	for _, txn := range queued {
		dump[fmt.Sprintf("%d", txn.GetNonce())] = newRPCPendingTransaction(txn, curHeader, cc)
	}
	content["queued"] = dump
	return content, nil
}

// Status returns the number of pending and queued transaction in the pool.
func (api *TxPoolAPIImpl) Status(ctx context.Context) (interface{}, error) {
	if api.l2RPCUrl != "" {
		res, err := client.JSONRPCCall(api.l2RPCUrl, "txpool_status")
		if err != nil {
			return nil, err
		}
		return res.Result, nil
	}

	reply, err := api.pool.Status(ctx, &proto_txpool.StatusRequest{})
	if err != nil {
		return nil, err
	}
	return map[string]hexutil.Uint{
		"pending": hexutil.Uint(reply.PendingCount),
		"baseFee": hexutil.Uint(reply.BaseFeeCount),
		"queued":  hexutil.Uint(reply.QueuedCount),
	}, nil
}

func (api *TxPoolAPIImpl) Limbo(ctx context.Context) (interface{}, error) {
	details := api.rawPool.GetInvalidLimboBlocksDetails()
	return details, nil
}
