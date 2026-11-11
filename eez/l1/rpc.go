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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
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

var (
	_ ChainReader = (*RPC)(nil)
	_ StateReader = (*RPC)(nil)
)

// DialRPC connects to the JSON-RPC endpoint at url.
func DialRPC(ctx context.Context, url string, logger log.Logger) (*RPC, error) {
	client, err := rpc.DialContext(ctx, url, logger)
	if err != nil {
		return nil, RedactTransport(err)
	}
	return &RPC{client: client}, nil
}

// Close releases the connection.
func (r *RPC) Close() { r.client.Close() }

func (r *RPC) ChainID(ctx context.Context) (uint64, error) {
	var id hexutil.Uint64
	if err := r.client.CallContext(ctx, &id, "eth_chainId"); err != nil {
		return 0, mapError(err, "eth_chainId")
	}
	return uint64(id), nil
}

type rpcHeader struct {
	Number     *hexutil.Uint64 `json:"number"`
	Hash       *common.Hash    `json:"hash"`
	ParentHash *common.Hash    `json:"parentHash"`
	Timestamp  *hexutil.Uint64 `json:"timestamp"`
	MixHash    common.Hash     `json:"mixHash"`
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
	return &Header{Number: uint64(*h.Number), Hash: *h.Hash, ParentHash: *h.ParentHash, Time: uint64(*h.Timestamp), MixDigest: h.MixHash}, nil
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

// canonicalAt is the EIP-1898 block parameter of a state read at hash.
func canonicalAt(hash common.Hash) map[string]any {
	return map[string]any{"blockHash": hash, "requireCanonical": true}
}

func (r *RPC) CodeAt(ctx context.Context, addr common.Address, blockHash common.Hash) ([]byte, error) {
	var code hexutil.Bytes
	if err := r.client.CallContext(ctx, &code, "eth_getCode", addr, canonicalAt(blockHash)); err != nil {
		return nil, mapError(err, fmt.Sprintf("code of %x at block %x", addr, blockHash))
	}
	return code, nil
}

func (r *RPC) CallAt(ctx context.Context, call Call, blockHash common.Hash) ([]byte, error) {
	args := map[string]any{"to": call.To, "input": hexutil.Bytes(call.Data)}
	var out hexutil.Bytes
	if err := r.client.CallContext(ctx, &out, "eth_call", args, canonicalAt(blockHash)); err != nil {
		what := fmt.Sprintf("call to %x at block %x", call.To, blockHash)
		// A revert reason is the contract's text and may read as any other error.
		if rerr, ok := errors.AsType[rpc.Error](err); ok && rerr.ErrorCode() == revertCode {
			return nil, fmt.Errorf("%w: %s: %w", ErrReverted, what, err)
		}
		return nil, mapError(err, what)
	}
	return out, nil
}

// revertCode is the JSON-RPC error code of an execution revert (EIP-1474).
const revertCode = 3

// prunedCode is the JSON-RPC error code of a block whose history the node
// expired (EIP-4444).
const prunedCode = 4444

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
	"narrow your filter",
}

var notFound = []string{"not found", "unknown block", "finalized block", "is not executed"}

// notCanonical are messages of a state read refused at a non-canonical block
// hash.
var notCanonical = []string{"not currently canonical", "is not canonical"}

func mapError(err error, what string) error {
	err = RedactTransport(err)
	msg := strings.ToLower(err.Error())
	if rerr, ok := errors.AsType[rpc.Error](err); (ok && rerr.ErrorCode() == prunedCode) || strings.Contains(msg, "pruned history") {
		return fmt.Errorf("%w: %w: %s: %w", ErrNotFound, ErrBeforeHistory, what, err)
	}
	for _, s := range rangeTooWide {
		if strings.Contains(msg, s) {
			return fmt.Errorf("%w: %s: %w", ErrRangeTooWide, what, err)
		}
	}
	for _, s := range notCanonical {
		if strings.Contains(msg, s) {
			return fmt.Errorf("%w: %w: %s: %w", ErrNotFound, ErrNotCanonical, what, err)
		}
	}
	for _, s := range notFound {
		if strings.Contains(msg, s) {
			return fmt.Errorf("%w: %s: %w", ErrNotFound, what, err)
		}
	}
	return fmt.Errorf("%s: %w", what, err)
}

// transportError prints a failed connection without the request URL, which
// is the configured endpoint, and the local address, which changes per
// connection.
type transportError struct {
	op  net.OpError
	err error
}

func (e *transportError) Error() string { return e.op.Error() }

func (e *transportError) Unwrap() error { return e.err }

// urlError prints a failed request with the URL's scheme and host only: its
// path, query and user info may carry the endpoint's API key.
type urlError struct {
	text string
	err  error
}

func (e *urlError) Error() string { return e.text }

func (e *urlError) Unwrap() error { return e.err }

// RedactTransport returns err printed without what a failed request's error
// text carries of its endpoint beyond scheme and host, and without a failed
// connection's local address; err stays reachable through Unwrap.
func RedactTransport(err error) error {
	if op, ok := errors.AsType[*net.OpError](err); ok {
		short := *op
		short.Source = nil
		return &transportError{op: short, err: err}
	}
	if ue, ok := errors.AsType[*url.Error](err); ok {
		host := "endpoint"
		if u, perr := url.Parse(ue.URL); perr == nil && u.Host != "" {
			host = u.Scheme + "://" + u.Host
		}
		// url.Error prints its URL quoted.
		text := strings.ReplaceAll(err.Error(), strconv.Quote(ue.URL), host)
		return &urlError{text: strings.ReplaceAll(text, ue.URL, host), err: err}
	}
	return err
}
