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
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// States reads L1's state after a canonical block.
type States interface {
	// ViewState runs read on the header of the canonical block with hash and
	// the state after it.
	ViewState(ctx context.Context, hash common.Hash, read func(*types.Header, state.StateReader) error) error
}

var (
	_ States = (*Native)(nil)
	_ States = (*RPC)(nil)
)

func (r *Native) ViewState(ctx context.Context, hash common.Hash, read func(*types.Header, state.StateReader) error) error {
	return r.stateAt(ctx, hash, func(_ kv.TemporalTx, h *types.Header, st state.StateReader) error { return read(h, st) })
}

func (r *RPC) ViewState(ctx context.Context, hash common.Hash, read func(*types.Header, state.StateReader) error) error {
	var raw json.RawMessage
	if err := r.client.CallContext(ctx, &raw, "eth_getBlockByHash", hash, false); err != nil {
		return mapError(err, fmt.Sprintf("block %x", hash))
	}
	if isNull(raw) {
		return fmt.Errorf("%w: block %x", ErrNotFound, hash)
	}
	h := new(types.Header)
	if err := json.Unmarshal(raw, h); err != nil {
		return fmt.Errorf("block %x: %w", hash, err)
	}
	if h.Hash() != hash {
		return fmt.Errorf("%w: header of block %x hashes to %x", ErrInconsistent, hash, h.Hash())
	}
	return read(h, &rpcState{ctx: ctx, rpc: r, at: canonicalAt(hash), accounts: map[common.Address]*rpcAccount{}, slots: map[slotKey]uint256.Int{}})
}

type rpcAccount struct {
	account *accounts.Account
	code    []byte
}

type slotKey struct {
	addr common.Address
	key  common.Hash
}

// rpcState is a state reader over a node's JSON-RPC at one block, caching
// what it read.
type rpcState struct {
	ctx      context.Context
	rpc      *RPC
	at       map[string]any
	mu       sync.Mutex
	accounts map[common.Address]*rpcAccount
	slots    map[slotKey]uint256.Int
}

func (s *rpcState) account(addr common.Address) (*rpcAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.accounts[addr]; ok {
		return a, nil
	}
	var balance hexutil.Big
	var nonce hexutil.Uint64
	var code hexutil.Bytes
	for _, c := range []struct {
		method string
		out    any
	}{{"eth_getBalance", &balance}, {"eth_getTransactionCount", &nonce}, {"eth_getCode", &code}} {
		if err := s.rpc.client.CallContext(s.ctx, c.out, c.method, addr, s.at); err != nil {
			return nil, mapError(err, fmt.Sprintf("%s(%x)", c.method, addr))
		}
	}
	a := &rpcAccount{}
	if len(code) != 0 {
		a.code = code
	}
	if b := balance.ToInt(); b.Sign() != 0 || nonce != 0 || len(code) != 0 {
		acc := accounts.NewAccount()
		acc.Nonce = uint64(nonce)
		if acc.Balance.SetFromBig(b) {
			return nil, fmt.Errorf("%w: balance of %x", ErrInconsistent, addr)
		}
		if len(code) != 0 {
			acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
			// A deployed contract is in its first life.
			acc.Incarnation = 1
		}
		a.account = &acc
	}
	s.accounts[addr] = a
	return a, nil
}

func (s *rpcState) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	return s.ReadAccountByPlainAddr(address.Value())
}

func (s *rpcState) ReadAccountByPlainAddr(address common.Address) (*accounts.Account, error) {
	a, err := s.account(address)
	if err != nil || a.account == nil {
		return nil, err
	}
	acc := *a.account
	return &acc, nil
}

func (s *rpcState) HasAccount(address common.Address) (bool, error) {
	a, err := s.account(address)
	return err == nil && a.account != nil, err
}

func (s *rpcState) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return s.ReadAccountData(address)
}

func (s *rpcState) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	k := slotKey{address.Value(), key.Value()}
	s.mu.Lock()
	v, ok := s.slots[k]
	s.mu.Unlock()
	if !ok {
		var word common.Hash
		if err := s.rpc.client.CallContext(s.ctx, &word, "eth_getStorageAt", k.addr, k.key, s.at); err != nil {
			return uint256.Int{}, false, mapError(err, fmt.Sprintf("storage %x of %x", k.key, k.addr))
		}
		v.SetBytes32(word[:])
		s.mu.Lock()
		s.slots[k] = v
		s.mu.Unlock()
	}
	return v, !v.IsZero(), nil
}

func (s *rpcState) ReadAccountCode(address accounts.Address) ([]byte, error) {
	a, err := s.account(address.Value())
	if err != nil {
		return nil, err
	}
	return a.code, nil
}

func (s *rpcState) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := s.ReadAccountCode(address)
	return len(code), err
}

func (s *rpcState) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	a, err := s.account(address.Value())
	if err != nil || a.account == nil {
		return 0, err
	}
	return a.account.Incarnation, nil
}

func (s *rpcState) SetTrace(bool, string) {}
func (s *rpcState) Trace() bool           { return false }
func (s *rpcState) TracePrefix() string   { return "" }
