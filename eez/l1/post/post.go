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

// Package post signs a rollup's L1 transactions and hands them to an L1
// block builder as bundles.
package post

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
)

const (
	// DefaultPriorityFee is the tip of a posting, 10 gwei.
	DefaultPriorityFee = 10_000_000_000
	// nextBlockSlack keeps an unpinned bundle off blocks already being built.
	nextBlockSlack = 2
	// methodNotFound is the JSON-RPC error of a node without the method.
	methodNotFound = -32601
	sendTimeout    = 30 * time.Second
	maxAnswer      = 1 << 20
)

var errNoBundles = errors.New("builder has no eth_sendBundle")

// L1 is the L1 node a poster reads from and sends to.
type L1 interface {
	ChainID(ctx context.Context) (uint64, error)
	Head(ctx context.Context) (*l1.Header, error)
	PendingNonce(ctx context.Context, addr common.Address) (uint64, error)
	BaseFee(ctx context.Context) (*uint256.Int, error)
	Balance(ctx context.Context, addr common.Address) (*uint256.Int, error)
	SendRawTransaction(ctx context.Context, raw []byte) error
}

// Poster signs calls of one L1 contract and sends them to L1.
type Poster struct {
	key      *ecdsa.PrivateKey
	from     common.Address
	to       common.Address
	signer   *types.Signer
	chainID  uint256.Int
	priority uint256.Int
	node     L1
	builder  string
	client   *http.Client
	logger   log.Logger
}

// New returns a poster of key's calls to the contract at to, tipping
// priority, whose bundles go to the eth_sendBundle endpoint builder.
func New(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, priority uint64, builder string, node L1, logger log.Logger) (*Poster, error) {
	id, err := node.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("L1 chain id: %w", err)
	}
	p := &Poster{
		key:     key,
		from:    crypto.PubkeyToAddress(key.PublicKey),
		to:      to,
		node:    node,
		builder: builder,
		client:  &http.Client{Timeout: sendTimeout},
		logger:  logger,
	}
	p.chainID.SetUint64(id)
	p.priority.SetUint64(priority)
	p.signer = types.LatestSignerForChainID(&p.chainID)
	return p, nil
}

// From returns the poster's address.
func (p *Poster) From() common.Address { return p.from }

// Sign returns the poster's EIP-1559 call with input and gas limit gas, at
// its pending nonce, capping the fee at twice the latest base fee plus the
// tip.
func (p *Poster) Sign(ctx context.Context, input []byte, gas uint64) (types.Transaction, error) {
	nonce, err := p.node.PendingNonce(ctx, p.from)
	if err != nil {
		return nil, err
	}
	base, err := p.node.BaseFee(ctx)
	if err != nil {
		return nil, err
	}
	feeCap, overflow := new(uint256.Int).MulOverflow(base, uint256.NewInt(2))
	if !overflow {
		_, overflow = feeCap.AddOverflow(feeCap, &p.priority)
	}
	if overflow {
		feeCap.SetAllOne()
	}
	// Only the gas used is charged; a balance short of the limit may still do.
	need, overflow := new(uint256.Int).MulOverflow(feeCap, uint256.NewInt(gas))
	if balance, err := p.node.Balance(ctx, p.from); err == nil && (overflow || balance.Lt(need)) {
		p.logger.Error("[eez] poster balance below the posting's gas limit at its fee cap", "event", "eez.poster.underfunded",
			"poster", p.from, "balance", balance, "need", need)
	}
	to := p.to
	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: &to, Data: input},
		ChainID:  p.chainID,
		TipCap:   p.priority,
		FeeCap:   *feeCap,
	}
	return types.SignTx(tx, *p.signer, p.key)
}

// Target is the L1 block a bundle is for.
type Target struct {
	// Block is the L1 block; zero aims two blocks above the L1 head.
	Block uint64
	// Time, when set, admits the bundle only in a block with that timestamp.
	Time uint64
}

// Sent is a bundle handed to L1.
type Sent struct {
	// Block is the L1 block the bundle is for.
	Block uint64
	// Time is the timestamp the bundle is pinned to, zero when it is not; a
	// bundle sent through the mempool is not.
	Time uint64
}

// Send hands txs, the poster's call first, to the builder for target. A
// builder without eth_sendBundle has them sent in order to the L1 node's pool
// instead, which keeps neither their order nor the target: the call refused
// there fails Send, a later transaction is left out.
func (p *Poster) Send(ctx context.Context, txs []types.Transaction, target Target) (Sent, error) {
	if len(txs) == 0 {
		return Sent{}, errors.New("bundle of no transactions")
	}
	raw, err := types.MarshalTransactionsBinary(txs)
	if err != nil {
		return Sent{}, err
	}
	block := target.Block
	if block == 0 {
		head, err := p.node.Head(ctx)
		if err != nil {
			return Sent{}, err
		}
		block = head.Number + nextBlockSlack
	}
	err = p.bundle(ctx, raw, block, target.Time)
	switch {
	case err == nil:
		return Sent{Block: block, Time: target.Time}, nil
	case !errors.Is(err, errNoBundles):
		return Sent{}, err
	}
	p.logger.Warn("[eez] builder has no eth_sendBundle, sending through the mempool", "event", "eez.poster.mempool",
		"block", block, "txs", len(raw))
	for i, tx := range raw {
		if err := p.node.SendRawTransaction(ctx, tx); err != nil {
			if i == 0 {
				return Sent{}, fmt.Errorf("mempool refused the posting: %w", err)
			}
			p.logger.Warn("[eez] mempool refused a bundled transaction", "event", "eez.poster.mempool.refused",
				"index", i, "hash", txs[i].Hash(), "err", err)
		}
	}
	return Sent{Block: block}, nil
}

// bundle sends raw to the builder for block, pinned to timestamp time unless
// zero. Any answer without an error is an acceptance.
func (p *Poster) bundle(ctx context.Context, raw [][]byte, block, time uint64) error {
	hexes := make([]hexutil.Bytes, len(raw))
	for i, tx := range raw {
		hexes[i] = tx
	}
	params := map[string]any{"txs": hexes, "blockNumber": hexutil.Uint64(block)}
	if time != 0 {
		params["minTimestamp"], params["maxTimestamp"] = time, time
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_sendBundle", "params": []any{params}})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.builder, bytes.NewReader(body))
	if err != nil {
		return l1.RedactTransport(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("eth_sendBundle: %w", l1.RedactTransport(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("eth_sendBundle: HTTP %s", resp.Status)
	}
	var answer struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxAnswer)).Decode(&answer); err != nil {
		return fmt.Errorf("eth_sendBundle answer: %w", err)
	}
	switch {
	case answer.Error == nil:
		return nil
	case answer.Error.Code == methodNotFound:
		return errNoBundles
	}
	return fmt.Errorf("eth_sendBundle: %s (%d)", answer.Error.Message, answer.Error.Code)
}
