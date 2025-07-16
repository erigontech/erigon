// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"fmt"
	"strconv"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// TxPoolAPI the interface for the txpool_ RPC commands
type TxPoolAPI interface {
	Content(ctx context.Context) (map[string]map[string]map[string]*ethapi.RPCTransaction, error)
	ContentFrom(ctx context.Context, addr common.Address) (map[string]map[string]*ethapi.RPCTransaction, error)
}

// TxPoolAPIImpl data structure to store things needed for net_ commands
type TxPoolAPIImpl struct {
	*BaseAPI
	pool proto_txpool.TxpoolClient
	db   kv.TemporalRoDB
}

// NewTxPoolAPI returns NetAPIImplImpl instance
func NewTxPoolAPI(base *BaseAPI, db kv.TemporalRoDB, pool proto_txpool.TxpoolClient) *TxPoolAPIImpl {
	return &TxPoolAPIImpl{
		BaseAPI: base,
		pool:    pool,
		db:      db,
	}
}

func (api *TxPoolAPIImpl) Content(ctx context.Context) (map[string]map[string]map[string]*ethapi.RPCTransaction, error) {
	reply, err := api.pool.All(ctx, &proto_txpool.AllRequest{})
	if err != nil {
		return nil, err
	}

	content := map[string]map[string]map[string]*ethapi.RPCTransaction{
		"pending": make(map[string]map[string]*ethapi.RPCTransaction),
		"baseFee": make(map[string]map[string]*ethapi.RPCTransaction),
		"queued":  make(map[string]map[string]*ethapi.RPCTransaction),
	}

	pending := make(map[common.Address][]types.Transaction, 8)
	baseFee := make(map[common.Address][]types.Transaction, 8)
	queued := make(map[common.Address][]types.Transaction, 8)
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

	tx, err := api.db.BeginTemporalRo(ctx)
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
		dump := make(map[string]*ethapi.RPCTransaction, len(txs))
		for _, txn := range txs {
			dump[strconv.FormatUint(txn.GetNonce(), 10)] = newRPCPendingTransaction(txn, curHeader, cc)
		}
		content["pending"][account.Hex()] = dump
	}
	// Flatten the baseFee transactions
	for account, txs := range baseFee {
		dump := make(map[string]*ethapi.RPCTransaction, len(txs))
		for _, txn := range txs {
			dump[strconv.FormatUint(txn.GetNonce(), 10)] = newRPCPendingTransaction(txn, curHeader, cc)
		}
		content["baseFee"][account.Hex()] = dump
	}
	// Flatten the queued transactions
	for account, txs := range queued {
		dump := make(map[string]*ethapi.RPCTransaction, len(txs))
		for _, txn := range txs {
			dump[strconv.FormatUint(txn.GetNonce(), 10)] = newRPCPendingTransaction(txn, curHeader, cc)
		}
		content["queued"][account.Hex()] = dump
	}
	return content, nil
}

func (api *TxPoolAPIImpl) ContentFrom(ctx context.Context, addr common.Address) (map[string]map[string]*ethapi.RPCTransaction, error) {
	reply, err := api.pool.All(ctx, &proto_txpool.AllRequest{})
	if err != nil {
		return nil, err
	}

	content := map[string]map[string]*ethapi.RPCTransaction{
		"pending": make(map[string]*ethapi.RPCTransaction),
		"baseFee": make(map[string]*ethapi.RPCTransaction),
		"queued":  make(map[string]*ethapi.RPCTransaction),
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

	tx, err := api.db.BeginTemporalRo(ctx)
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
	dump := make(map[string]*ethapi.RPCTransaction, len(pending))
	for _, txn := range pending {
		dump[strconv.FormatUint(txn.GetNonce(), 10)] = newRPCPendingTransaction(txn, curHeader, cc)
	}
	content["pending"] = dump
	// Flatten the baseFee transactions
	dump = make(map[string]*ethapi.RPCTransaction, len(baseFee))
	for _, txn := range baseFee {
		dump[strconv.FormatUint(txn.GetNonce(), 10)] = newRPCPendingTransaction(txn, curHeader, cc)
	}
	content["baseFee"] = dump
	// Flatten the queued transactions
	dump = make(map[string]*ethapi.RPCTransaction, len(queued))
	for _, txn := range queued {
		dump[strconv.FormatUint(txn.GetNonce(), 10)] = newRPCPendingTransaction(txn, curHeader, cc)
	}
	content["queued"] = dump
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
		for _, txn := range txs {
			dump[fmt.Sprintf("%d", tx.Nonce())] = format(tx)
		}
		content["pending"][account.Hex()] = dump
	}
	// Flatten the queued transactions
	for account, txs := range queue {
		dump := make(map[string]string)
		for _, txn := range txs {
			dump[fmt.Sprintf("%d", tx.Nonce())] = format(tx)
		}
		content["queued"][account.Hex()] = dump
	}
	return content
}
*/
