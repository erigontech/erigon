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
	"sync"
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
	// maxPooled is the largest transaction other than a blob one that an L1
	// pool admits, 128 KiB.
	maxPooled = 128 * 1024
	// replaceBump is the percentage of a pooled posting's fees its
	// replacement pays, over the 10% a pool requires of both tip and fee
	// cap; maxBump caps the fees compounding bumps reach, as a multiple of
	// fresh ones.
	replaceBump = 115
	maxBump     = 4
)

// nodeTimeout is the default bound of a poster's calls to the L1 node.
var nodeTimeout = 5 * time.Second

var errNoBundles = errors.New("builder has no eth_sendBundle")

// ErrUnsure reports a send that failed without an answer refusing it: the
// posting may have reached L1 all the same.
var ErrUnsure = errors.New("posting may have reached L1")

// ErrBundleRefused reports a bundle the builder answered with an error.
var ErrBundleRefused = errors.New("eth_sendBundle refused")

// L1 is the L1 node a poster reads from and sends to.
type L1 interface {
	ChainID(ctx context.Context) (uint64, error)
	Head(ctx context.Context) (*l1.Header, error)
	Nonce(ctx context.Context, addr common.Address) (uint64, error)
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
	// nodeTimeout bounds the poster's calls to the L1 node, whose websocket
	// transport has no timeout of its own: Sign's reads together, Send's
	// calls each.
	nodeTimeout time.Duration
	logger      log.Logger

	// mu guards last, which Send records off the goroutine that signs.
	mu   sync.Mutex
	last signed
}

// signed is the last posting a poster signed.
type signed struct {
	nonce       uint64
	tip, feeCap uint256.Int
	// held reports that a pool may hold a transaction at nonce: the posting
	// went through one, or one refused it.
	held bool
}

// New returns a poster of key's calls to the contract at to, tipping
// priority, whose bundles go to the eth_sendBundle endpoint builder.
func New(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, priority uint64, builder string, node L1, logger log.Logger) (*Poster, error) {
	cctx, cancel := context.WithTimeout(ctx, nodeTimeout)
	id, err := node.ChainID(cctx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("L1 chain id: %w", err)
	}
	p := &Poster{
		key:         key,
		from:        crypto.PubkeyToAddress(key.PublicKey),
		to:          to,
		node:        node,
		builder:     builder,
		client:      &http.Client{Timeout: sendTimeout},
		nodeTimeout: nodeTimeout,
		logger:      logger,
	}
	p.chainID.SetUint64(id)
	p.priority.SetUint64(priority)
	p.signer = types.LatestSignerForChainID(&p.chainID)
	return p, nil
}

// From returns the poster's address.
func (p *Poster) From() common.Address { return p.from }

// ChainID returns the id of the L1 chain the poster signs for.
func (p *Poster) ChainID() uint64 { return p.chainID.Uint64() }

// Sign returns the poster's EIP-1559 call with input and gas limit gas, at
// its confirmed nonce, capping the fee at twice the latest base fee plus the
// tip. A posting a pool may hold at that nonce, after a base fee rise above
// its fee cap or from before a restart, is outbid by replaceBump percent,
// compounding while pools refuse, up to maxBump times the fresh fees: at one
// nonce, at most one posting lands.
func (p *Poster) Sign(ctx context.Context, input []byte, gas uint64) (types.Transaction, error) {
	ctx, cancel := context.WithTimeout(ctx, p.nodeTimeout)
	defer cancel()
	nonce, err := p.node.Nonce(ctx, p.from)
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
	tip := p.priority
	p.mu.Lock()
	if last := p.last; last.held && last.nonce == nonce {
		tip, *feeCap = outbid(&p.priority, feeCap, &last.tip, &last.feeCap)
		p.logger.Warn("[eez] posting replaces the one a pool may hold at its nonce", "event", "eez.poster.replacement",
			"nonce", nonce, "tip", &tip, "feeCap", feeCap, "lastTip", &last.tip, "lastFeeCap", &last.feeCap)
	}
	p.last = signed{nonce: nonce, tip: tip, feeCap: *feeCap}
	p.mu.Unlock()
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
		TipCap:   tip,
		FeeCap:   *feeCap,
	}
	return types.SignTx(tx, *p.signer, p.key)
}

// outbid returns the fees of a posting replacing one with lastTip and
// lastCap: replaceBump percent over them, no lower than the fresh tip and
// feeCap, no higher than maxBump times them, the cap no lower than the tip.
func outbid(tip, feeCap, lastTip, lastCap *uint256.Int) (uint256.Int, uint256.Int) {
	bump := func(fresh, last *uint256.Int) uint256.Int {
		var v uint256.Int
		if _, overflow := v.MulOverflow(last, uint256.NewInt(replaceBump)); overflow {
			v.SetAllOne()
		} else {
			v.Div(&v, uint256.NewInt(100))
			v.AddUint64(&v, 1)
		}
		if v.Lt(fresh) {
			v = *fresh
		}
		var ceiling uint256.Int
		if _, overflow := ceiling.MulOverflow(fresh, uint256.NewInt(maxBump)); !overflow && v.Gt(&ceiling) {
			v = ceiling
		}
		return v
	}
	t, c := bump(tip, lastTip), bump(feeCap, lastCap)
	if c.Lt(&t) {
		c = t
	}
	return t, c
}

// hold records that a pool may hold a transaction at the nonce of tx, the
// last posting signed: the next one there outbids it.
func (p *Poster) hold(tx types.Transaction) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.last.nonce == tx.GetNonce() {
		p.last.held = true
	}
}

// Target is the L1 block a bundle is for.
type Target struct {
	// Block is the L1 block; zero, or without Time a block L1 has built,
	// aims two blocks above the L1 head.
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
	// Pooled reports a bundle that may be in the L1 node's pool and land
	// after Block, until the next posting replaces it at its nonce: one sent
	// through the mempool, or one whose send failed without an answer.
	Pooled bool
}

// Send hands txs, the poster's call first, to the builder for target, the
// others allowed to revert. A
// builder without eth_sendBundle has them sent in order to the L1 node's pool
// instead, which keeps neither their order nor the target and suits only a
// development L1: the call refused there, or over the size a pool admits,
// fails Send, a later transaction is left out. A send failing without an
// answer refusing it fails with ErrUnsure, the returned Sent describing it.
func (p *Poster) Send(ctx context.Context, txs []types.Transaction, target Target) (Sent, error) {
	if len(txs) == 0 {
		return Sent{}, errors.New("bundle of no transactions")
	}
	raw, err := types.MarshalTransactionsBinary(txs)
	if err != nil {
		return Sent{}, err
	}
	block := target.Block
	// A target set before a catch-up and its proof may name a block L1 has
	// built since; only a timestamp pin keeps it.
	if block == 0 || target.Time == 0 {
		hctx, cancel := context.WithTimeout(ctx, p.nodeTimeout)
		head, err := p.node.Head(hctx)
		cancel()
		if err != nil {
			return Sent{}, err
		}
		if block <= head.Number {
			block = head.Number + nextBlockSlack
		}
	}
	err = p.bundle(ctx, raw, block, target.Time)
	switch {
	case err == nil:
		return Sent{Block: block, Time: target.Time}, nil
	case errors.Is(err, ErrUnsure):
		return Sent{Block: block, Pooled: true}, err
	case !errors.Is(err, errNoBundles):
		return Sent{}, err
	}
	if size := len(raw[0]); size > maxPooled {
		p.logger.Error("[eez] posting over the size an L1 pool admits; settling it needs a builder with eth_sendBundle", "event", "eez.poster.mempool.oversized",
			"size", size, "limit", maxPooled)
		return Sent{}, fmt.Errorf("posting of %d bytes over the %d an L1 pool admits: settling it needs a builder with eth_sendBundle", size, maxPooled)
	}
	p.logger.Warn("[eez] builder has no eth_sendBundle, sending through the mempool", "event", "eez.poster.mempool",
		"block", block, "txs", len(raw))
	// Taken, refused or unanswered, the posting leaves its nonce to a pool's
	// replacement rule.
	p.hold(txs[0])
	for i, tx := range raw {
		sctx, cancel := context.WithTimeout(ctx, p.nodeTimeout)
		err := p.node.SendRawTransaction(sctx, tx)
		cancel()
		switch {
		case err == nil:
		case i > 0:
			p.logger.Warn("[eez] mempool refused a bundled transaction", "event", "eez.poster.mempool.refused",
				"index", i, "hash", txs[i].Hash(), "err", err)
		case errors.Is(err, l1.ErrRefused):
			return Sent{}, fmt.Errorf("mempool refused the posting: %w", err)
		default:
			return Sent{Block: block, Pooled: true}, fmt.Errorf("%w: mempool: %w", ErrUnsure, err)
		}
	}
	return Sent{Block: block, Pooled: true}, nil
}

// bundle sends raw to the builder for block, pinned to timestamp time unless
// zero. Any answer without an error is an acceptance.
func (p *Poster) bundle(ctx context.Context, raw [][]byte, block, time uint64) error {
	hexes := make([]hexutil.Bytes, len(raw))
	for i, tx := range raw {
		hexes[i] = tx
	}
	params := map[string]any{"txs": hexes, "blockNumber": hexutil.Uint64(block)}
	// A reverting trigger leaves the posting standing; the posting may not revert.
	if len(raw) > 1 {
		reverting := make([]common.Hash, len(raw)-1)
		for i, tx := range raw[1:] {
			reverting[i] = crypto.Keccak256Hash(tx)
		}
		params["revertingTxHashes"] = reverting
	}
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
		return fmt.Errorf("%w: eth_sendBundle: %w", ErrUnsure, l1.RedactTransport(err))
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 5:
		// A gateway may have passed the bundle on.
		return fmt.Errorf("%w: eth_sendBundle: HTTP %s", ErrUnsure, resp.Status)
	case resp.StatusCode/100 != 2:
		return fmt.Errorf("eth_sendBundle: HTTP %s", resp.Status)
	}
	var answer struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxAnswer)).Decode(&answer); err != nil {
		return fmt.Errorf("%w: eth_sendBundle answer: %w", ErrUnsure, err)
	}
	switch {
	case answer.Error == nil:
		return nil
	case answer.Error.Code == methodNotFound:
		return errNoBundles
	}
	return fmt.Errorf("%w: %s (%d)", ErrBundleRefused, answer.Error.Message, answer.Error.Code)
}
