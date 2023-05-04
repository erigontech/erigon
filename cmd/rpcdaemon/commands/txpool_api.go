package commands

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
)

// NetAPI the interface for the net_ RPC commands
type TxPoolAPI interface {
	Content(ctx context.Context) (map[string]map[string]map[string]*RPCTransaction, error)
}

// TxPoolAPIImpl data structure to store things needed for net_ commands
type TxPoolAPIImpl struct {
	*BaseAPI
	pool proto_txpool.TxpoolClient
	db   kv.RoDB
}

// NewTxPoolAPI returns NetAPIImplImpl instance
func NewTxPoolAPI(base *BaseAPI, db kv.RoDB, pool proto_txpool.TxpoolClient) *TxPoolAPIImpl {
	return &TxPoolAPIImpl{
		BaseAPI: base,
		pool:    pool,
		db:      db,
	}
}

func (api *TxPoolAPIImpl) Content(ctx context.Context) (map[string]map[string]map[string]*RPCTransaction, error) {
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
		txn, err := types.DecodeTransaction(reply.Txs[i].RlpTx)
		if err != nil {
			return nil, err
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
	cc, err := api.chainConfig(tx)
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

// Status returns the number of pending and queued transaction in the pool.
func (api *TxPoolAPIImpl) Status(ctx context.Context) (map[string]hexutil.Uint, error) {
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

/*

// Inspect retrieves the content of the transaction pool and flattens it into an
// easily inspectable list.
func (s *PublicTxPoolAPI) Inspect() map[string]map[string]map[string]string {
	content := map[string]map[string]map[string]string{
		"pending": make(map[string]map[string]string),
		"queued":  make(map[string]map[string]string),
	}
	pending, queue := s.b.TxPoolContent()

	// Define a formatter to flatten a transaction into a string
	var format = func(tx *types.Transaction) string {
		if to := tx.To(); to != nil {
			return fmt.Sprintf("%s: %v wei + %v gas × %v wei", tx.To().Hex(), tx.Value(), tx.Gas(), tx.GasPrice())
		}
		return fmt.Sprintf("contract creation: %v wei + %v gas × %v wei", tx.Value(), tx.Gas(), tx.GasPrice())
	}
	// Flatten the pending transactions
	for account, txs := range pending {
		dump := make(map[string]string)
		for _, tx := range txs {
			dump[fmt.Sprintf("%d", tx.Nonce())] = format(tx)
		}
		content["pending"][account.Hex()] = dump
	}
	// Flatten the queued transactions
	for account, txs := range queue {
		dump := make(map[string]string)
		for _, tx := range txs {
			dump[fmt.Sprintf("%d", tx.Nonce())] = format(tx)
		}
		content["queued"][account.Hex()] = dump
	}
	return content
}
*/
