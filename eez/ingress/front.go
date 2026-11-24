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

// Package ingress serves the composer's cross-chain fronts: a JSON-RPC
// endpoint per source chain that holds the eth_sendRawTransaction calls for
// the next Sync block and forwards the other eth_ methods to the chain.
package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/composer"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
)

const (
	maxBody         = 10 << 20
	forwardTimeout  = 30 * time.Second
	sendRaw         = "eth_sendRawTransaction"
	composerInfo    = "eez_composerInfo"
	codeParse       = -32700
	codeInvalid     = -32600
	codeNoMethod    = -32601
	codeBadParams   = -32602
	codeServerError = -32000
)

// StartingUp is the refusal of a submission before the front is ready.
const StartingUp = "eez node is starting up; cross-chain submissions are not accepted yet"

// Source is the chain a front's transactions are signed for, read at its
// latest block.
type Source interface {
	Code(ctx context.Context, addr common.Address) ([]byte, error)
	Balance(ctx context.Context, addr common.Address) (*uint256.Int, error)
	PendingNonce(ctx context.Context, addr common.Address) (uint64, error)
}

// Front is the cross-chain front of one source chain.
type Front struct {
	// Direction is the way every transaction sent here crosses.
	Direction composer.Direction
	ChainID   uint64
	Pool      *composer.HeldPool
	Source    Source
	// Upstream is the source chain's JSON-RPC endpoint.
	Upstream string
	// GasCap returns the source chain's EIP-7825 transaction gas limit, false
	// when it has none.
	GasCap func() (uint64, bool, error)
	// Info answers eez_composerInfo.
	Info   json.RawMessage
	Logger log.Logger

	ready  atomic.Bool
	client http.Client
}

// Ready opens the front to submissions.
func (f *Front) Ready() { f.ready.Store(true) }

type request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func reply(w http.ResponseWriter, id json.RawMessage, result any, e *rpcError) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	out := map[string]any{"jsonrpc": "2.0", "id": id}
	if e != nil {
		out["error"] = e
	} else {
		out["result"] = result
	}
	enc, err := json.Marshal(out)
	if err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_, _ = w.Write(enc)
}

func submission(method string) bool { return strings.HasPrefix(method, sendRaw) }

func (f *Front) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.WriteHeader(http.StatusNoContent)
		return
	case http.MethodPost:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if len(body) > maxBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	var batch []request
	if json.Unmarshal(body, &batch) == nil {
		if len(batch) == 0 {
			reply(w, nil, nil, &rpcError{Code: codeInvalid, Message: "empty batch"})
			return
		}
		for _, req := range batch {
			if submission(req.Method) {
				reply(w, nil, nil, &rpcError{Code: codeInvalid, Message: "send a cross-chain eth_sendRawTransaction singly, not in a JSON-RPC batch"})
				return
			}
			if !strings.HasPrefix(req.Method, "eth_") {
				reply(w, nil, nil, &rpcError{Code: codeNoMethod, Message: "the cross-chain front forwards only eth_ methods"})
				return
			}
		}
		f.forward(w, r.Context(), body)
		return
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil {
		reply(w, nil, nil, &rpcError{Code: codeParse, Message: "parse error"})
		return
	}
	switch {
	case req.Method == composerInfo:
		if p := bytes.TrimSpace(req.Params); len(p) != 0 && !bytes.Equal(p, []byte("[]")) && !bytes.Equal(p, []byte("{}")) && !bytes.Equal(p, []byte("null")) {
			reply(w, req.ID, nil, &rpcError{Code: codeBadParams, Message: composerInfo + " takes no parameters", Data: req.Params})
			return
		}
		reply(w, req.ID, f.Info, nil)
		return
	case submission(req.Method) && !f.ready.Load():
		reply(w, req.ID, nil, &rpcError{Code: codeServerError, Message: StartingUp})
		return
	case submission(req.Method) && req.Method != sendRaw:
		reply(w, req.ID, nil, &rpcError{Code: codeNoMethod, Message: "only eth_sendRawTransaction is routed by the cross-chain front"})
		return
	case req.Method == sendRaw:
		if f.intercept(w, r.Context(), req) {
			return
		}
	case !strings.HasPrefix(req.Method, "eth_"):
		reply(w, req.ID, nil, &rpcError{Code: codeNoMethod, Message: "the cross-chain front forwards only eth_ methods"})
		return
	}
	f.forward(w, r.Context(), body)
}

// intercept holds the transaction of req, reporting false for one that is
// not a cross-chain call, which the source chain takes as it is.
func (f *Front) intercept(w http.ResponseWriter, ctx context.Context, req request) bool {
	var params []hexutil.Bytes
	if json.Unmarshal(req.Params, &params) != nil || len(params) == 0 {
		return false
	}
	tx, err := types.DecodeTransaction(params[0])
	if err != nil || tx.GetTo() == nil {
		return false
	}
	hash, err := f.admit(ctx, params[0], tx)
	if err != nil {
		f.Logger.Warn("[eez] cross-chain transaction refused at the front", "event", "eez.ingress.refused", "direction", f.Direction, "err", err)
		reply(w, req.ID, nil, &rpcError{Code: codeServerError, Message: err.Error()})
		return true
	}
	reply(w, req.ID, hash, nil)
	return true
}

// admit holds tx, raw as received, when its envelope, sender and target can
// make a cross-chain call.
func (f *Front) admit(ctx context.Context, raw []byte, tx types.Transaction) (common.Hash, error) {
	// The hash the sender is answered is the transaction's.
	var canonical bytes.Buffer
	if err := tx.MarshalBinary(&canonical); err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return common.Hash{}, errors.New("non-canonical transaction encoding: send the EIP-2718 envelope")
	}
	if len(raw) > composer.MaxHeldTxSize {
		return common.Hash{}, fmt.Errorf("%w: %d bytes", composer.ErrHeldTooLarge, len(raw))
	}
	switch tx.Type() {
	case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType:
	default:
		return common.Hash{}, fmt.Errorf("unsupported transaction type %d: cross-chain fronts take legacy (EIP-155), EIP-2930 and EIP-1559 transactions", tx.Type())
	}
	if !tx.Protected() {
		return common.Hash{}, errors.New("missing source-chain id: replay-unprotected transactions are refused")
	}
	if id := tx.GetChainID(); !id.IsUint64() || id.Uint64() != f.ChainID {
		return common.Hash{}, fmt.Errorf("wrong source-chain id %s for the %s front: expected %d", id, f.Direction, f.ChainID)
	}
	gasCap, capped, err := f.GasCap()
	if err != nil {
		return common.Hash{}, fmt.Errorf("source-chain transaction gas limit lookup failed: %w", err)
	}
	if capped && tx.GetGasLimit() > gasCap {
		return common.Hash{}, fmt.Errorf("declared gas limit %d exceeds the transaction gas limit cap %d", tx.GetGasLimit(), gasCap)
	}
	cost, overflow := new(uint256.Int).MulOverflow(uint256.NewInt(tx.GetGasLimit()), tx.GetFeeCap())
	if !overflow {
		_, overflow = cost.AddOverflow(cost, tx.GetValue())
	}
	if overflow {
		return common.Hash{}, errors.New("upfront cost overflows: value + gas limit × max fee")
	}
	from, err := tx.Sender(*types.LatestSignerForChainID(tx.GetChainID()))
	if err != nil {
		return common.Hash{}, fmt.Errorf("signature recovery failed: %w", err)
	}
	sender, to := from.Value(), *tx.GetTo()
	code, err := f.Source.Code(ctx, to)
	if err != nil {
		return common.Hash{}, fmt.Errorf("source-chain code lookup failed for target %s: %w", to, err)
	}
	if len(code) == 0 {
		return common.Hash{}, fmt.Errorf("target %s has no code on the source chain: a cross-chain call targets a deployed contract or proxy; create the proxy first (createCrossChainProxy on the manager) or use the bridge, then resubmit", to)
	}
	balance, err := f.Source.Balance(ctx, sender)
	if err != nil {
		return common.Hash{}, fmt.Errorf("source-chain balance lookup failed: %w", err)
	}
	if balance.Lt(cost) {
		return common.Hash{}, fmt.Errorf("insufficient balance for %s: have %s, need %s (value + gas limit × max fee)", sender, balance, cost)
	}
	onChain, err := f.Source.PendingNonce(ctx, sender)
	if err != nil {
		return common.Hash{}, fmt.Errorf("source-chain nonce lookup failed: %w", err)
	}
	h := &composer.Held{Raw: raw, Hash: crypto.Keccak256Hash(raw), Sender: sender, Nonce: tx.GetNonce(), Direction: f.Direction}
	h.MaxFee.Set(tx.GetFeeCap())
	h.Tip.Set(tx.GetTipCap())
	if err := f.Pool.Push(h, onChain); err != nil {
		if nonce, ok := errors.AsType[*composer.NonceError](err); ok {
			return common.Hash{}, fmt.Errorf("invalid nonce %d for %s: expected next unreserved nonce %d (source-chain nonce %d)", h.Nonce, sender, nonce.Expected, nonce.OnChain)
		}
		return common.Hash{}, fmt.Errorf("for %s nonce %d: %w", sender, h.Nonce, err)
	}
	f.Logger.Info("[eez] cross-chain transaction held for the next Sync slot", "event", "eez.ingress.held",
		"hash", h.Hash, "sender", sender, "nonce", h.Nonce, "direction", f.Direction)
	return h.Hash, nil
}

// forward relays body to the source chain.
func (f *Front) forward(w http.ResponseWriter, ctx context.Context, body []byte) {
	ctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.Upstream, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		f.Logger.Warn("[eez] cross-chain front upstream failed", "event", "eez.ingress.upstream", "err", l1.RedactTransport(err))
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// Info returns eez_composerInfo's answer: the EEZ contracts on each side and
// the chain ids of the networks the composer bridges.
func Info(registry, l2 common.Address, l1ChainID, l2ChainID uint64, version string) (json.RawMessage, error) {
	return json.Marshal(map[string]any{
		"eezContracts":      map[string]common.Address{"eezRegistryAddress": registry, "eezL2Address": l2},
		"supportedNetworks": map[string]uint64{"eezL1": l1ChainID, "eezL2": l2ChainID},
		"version":           version,
	})
}
