// Copyright 2026 The Erigon Authors
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
package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// RPC reads L1 from a node's JSON-RPC endpoint.
type RPC struct {
	client *rpc.Client
}

var _ ChainReader = (*RPC)(nil)

// DialRPC connects to the JSON-RPC endpoint at url.
func DialRPC(ctx context.Context, url string, logger log.Logger) (*RPC, error) {
	client, err := rpc.DialContext(ctx, url, logger)
	if err != nil {
		return nil, err
	}
	return &RPC{client: client}, nil
}

// Close releases the connection.
func (r *RPC) Close() { r.client.Close() }

func (r *RPC) ChainID(ctx context.Context) (uint64, error) {
	var id hexutil.Uint64
	if err := r.client.CallContext(ctx, &id, "eth_chainId"); err != nil {
		return 0, err
	}
	return uint64(id), nil
}

type rpcHeader struct {
	Number     *hexutil.Uint64 `json:"number"`
	Hash       *common.Hash    `json:"hash"`
	ParentHash *common.Hash    `json:"parentHash"`
	Timestamp  *hexutil.Uint64 `json:"timestamp"`
}

func (r *RPC) header(ctx context.Context, method string, block any) (*Header, error) {
	var raw json.RawMessage
	if err := r.client.CallContext(ctx, &raw, method, block, false); err != nil {
		return nil, mapError(err, fmt.Sprintf("%s(%v)", method, block))
	}
	if isNull(raw) {
		return nil, fmt.Errorf("%w: %s(%v)", ErrNotFound, method, block)
	}
	var h rpcHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return nil, fmt.Errorf("%s(%v): %w", method, block, err)
	}
	if h.Number == nil || h.Hash == nil || h.ParentHash == nil || h.Timestamp == nil {
		return nil, fmt.Errorf("%w: %s(%v) lacks number, hash, parentHash or timestamp", ErrInconsistent, method, block)
	}
	return &Header{Number: uint64(*h.Number), Hash: *h.Hash, ParentHash: *h.ParentHash, Time: uint64(*h.Timestamp)}, nil
}

func (r *RPC) HeaderByNumber(ctx context.Context, number uint64) (*Header, error) {
	h, err := r.header(ctx, "eth_getBlockByNumber", hexutil.Uint64(number))
	if err == nil && h.Number != number {
		return nil, fmt.Errorf("%w: asked for block %d, got %d", ErrInconsistent, number, h.Number)
	}
	return h, err
}

func (r *RPC) HeaderByHash(ctx context.Context, hash common.Hash) (*Header, error) {
	h, err := r.header(ctx, "eth_getBlockByHash", hash)
	if err == nil && h.Hash != hash {
		return nil, fmt.Errorf("%w: asked for block %x, got %x", ErrInconsistent, hash, h.Hash)
	}
	return h, err
}

func (r *RPC) Head(ctx context.Context) (*Header, error) {
	return r.header(ctx, "eth_getBlockByNumber", "latest")
}

func (r *RPC) Finalized(ctx context.Context) (*Header, error) {
	return r.header(ctx, "eth_getBlockByNumber", "finalized")
}

func (r *RPC) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	filterTopics := make([]any, len(topics))
	for i, t := range topics {
		if len(t) > 0 {
			filterTopics[i] = t
		}
	}
	filter := map[string]any{
		"fromBlock": hexutil.Uint64(from),
		"toBlock":   hexutil.Uint64(to),
		"address":   address,
		"topics":    filterTopics,
	}
	var logs []*types.Log
	if err := r.client.CallContext(ctx, &logs, "eth_getLogs", filter); err != nil {
		return nil, mapError(err, fmt.Sprintf("eth_getLogs [%d, %d]", from, to))
	}
	return logs, nil
}

func (r *RPC) TransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index uint64) (types.Transaction, error) {
	var raw json.RawMessage
	if err := r.client.CallContext(ctx, &raw, "eth_getTransactionByBlockHashAndIndex", blockHash, hexutil.Uint64(index)); err != nil {
		return nil, mapError(err, fmt.Sprintf("transaction %d of block %x", index, blockHash))
	}
	if isNull(raw) {
		return nil, fmt.Errorf("%w: transaction %d of block %x", ErrNotFound, index, blockHash)
	}
	var meta struct {
		Hash      common.Hash `json:"hash"`
		BlockHash common.Hash `json:"blockHash"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, err
	}
	tx, err := types.UnmarshalTransactionFromJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("transaction %d of block %x: %w", index, blockHash, err)
	}
	if tx.Hash() != meta.Hash || meta.BlockHash != blockHash {
		return nil, fmt.Errorf("%w: transaction %d of block %x hashes to %x, served as %x in block %x",
			ErrInconsistent, index, blockHash, tx.Hash(), meta.Hash, meta.BlockHash)
	}
	return tx, nil
}

func isNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}

// rangeTooWide are provider messages for a log query that matched more than
// the provider serves; the wording is client-specific, the remedy is not.
var rangeTooWide = []string{
	"exceeds max results",
	"query returned more than",
	"more than 10000 results",
	"response size exceeded",
	"query timeout exceeded",
	"range is too large",
	"block range too large",
	"too many results",
	"exceed maximum block range",
}

var notFound = []string{"not found", "unknown block", "finalized block"}

func mapError(err error, what string) error {
	msg := strings.ToLower(err.Error())
	for _, s := range rangeTooWide {
		if strings.Contains(msg, s) {
			return fmt.Errorf("%w: %s: %w", ErrRangeTooWide, what, err)
		}
	}
	for _, s := range notFound {
		if strings.Contains(msg, s) {
			return fmt.Errorf("%w: %s: %w", ErrNotFound, what, err)
		}
	}
	return fmt.Errorf("%s: %w", what, err)
}
