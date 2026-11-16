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

package ingress

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/composer"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

const chainID = 3151908

var (
	target   = common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3")
	codeless = common.HexToAddress("0xc0de")
)

// source is a chain where target has code, every sender a balance and nonce.
type source struct {
	mu      sync.Mutex
	balance uint64
	nonce   uint64
	failed  error
	reads   int
}

func (s *source) read() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return s.failed
}

func (s *source) Code(_ context.Context, a common.Address) ([]byte, error) {
	if a == target {
		return []byte{0x60}, s.read()
	}
	return nil, s.read()
}

func (s *source) Balance(context.Context, common.Address) (*uint256.Int, error) {
	return uint256.NewInt(s.balance), s.read()
}

func (s *source) PendingNonce(context.Context, common.Address) (uint64, error) {
	return s.nonce, s.read()
}

// upstream is the source chain's RPC, answering every request with ok.
type upstream struct {
	mu     sync.Mutex
	bodies []string
}

func (u *upstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	u.mu.Lock()
	u.bodies = append(u.bodies, string(body))
	u.mu.Unlock()
	_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":"upstream"}`)
}

type harness struct {
	front    *Front
	src      *source
	up       *upstream
	url      string
	key      *ecdsa.PrivateKey
	cap      uint64
	capped   bool
	capError error
}

func newFront(t *testing.T, direction composer.Direction) *harness {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	h := &harness{src: &source{balance: math.MaxUint64}, up: &upstream{}, key: key, cap: 16_777_216, capped: true}
	up := httptest.NewServer(h.up)
	t.Cleanup(up.Close)
	h.front = &Front{
		Direction: direction, ChainID: chainID, Pool: composer.NewHeldPool(), Source: h.src, Upstream: up.URL,
		GasCap: func() (uint64, bool, error) { return h.cap, h.capped, h.capError },
		Info:   json.RawMessage(`{"version":"test"}`), Logger: log.New(),
	}
	h.front.Ready()
	srv := httptest.NewServer(h.front)
	t.Cleanup(srv.Close)
	h.url = srv.URL
	return h
}

type answer struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	} `json:"error"`
}

func (h *harness) post(t *testing.T, body string) (int, answer, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, h.url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var a answer
	_ = json.Unmarshal(raw, &a)
	return resp.StatusCode, a, string(raw)
}

func (h *harness) send(t *testing.T, raw []byte) answer {
	t.Helper()
	_, a, _ := h.post(t, `{"jsonrpc":"2.0","id":7,"method":"eth_sendRawTransaction","params":["`+hexutil.Encode(raw)+`"]}`)
	return a
}

func (h *harness) sign(t *testing.T, tx types.Transaction, id uint64) []byte {
	t.Helper()
	var signer *types.Signer
	if id != 0 {
		signer = types.LatestSignerForChainID(uint256.NewInt(id))
	} else {
		signer = types.LatestSignerForChainID(nil)
	}
	signed, err := types.SignTx(tx, *signer, h.key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, signed.MarshalBinary(&buf))
	return buf.Bytes()
}

func dynamic(nonce, gas uint64, to *common.Address) *types.DynamicFeeTransaction {
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: to}, ChainID: *uint256.NewInt(chainID)}
	tx.FeeCap.SetUint64(100)
	tx.TipCap.SetUint64(10)
	return tx
}

func legacy(nonce uint64, price uint64, to *common.Address) *types.LegacyTx {
	tx := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 100_000, To: to}}
	tx.GasPrice.SetUint64(price)
	return tx
}

func ptr(a common.Address) *common.Address { return &a }

func TestFrontHolds(t *testing.T) {
	for _, c := range []struct {
		name string
		tx   types.Transaction
	}{
		{"EIP-1559", dynamic(0, 100_000, ptr(target))},
		{"EIP-155 legacy", legacy(0, 100, ptr(target))},
		{"EIP-2930", &types.AccessListTx{LegacyTx: *legacy(0, 100, ptr(target)), ChainID: *uint256.NewInt(chainID)}},
		{"gas at the cap", dynamic(0, 16_777_216, ptr(target))},
	} {
		for _, direction := range []composer.Direction{composer.Inbound, composer.Outbound} {
			t.Run(c.name+" "+direction.String(), func(t *testing.T) {
				h := newFront(t, direction)
				raw := h.sign(t, c.tx, chainID)
				a := h.send(t, raw)
				require.Nil(t, a.Error)
				var hash common.Hash
				require.NoError(t, json.Unmarshal(a.Result, &hash))
				require.Equal(t, crypto.Keccak256Hash(raw), hash)
				held := h.front.Pool.PopN(math.MaxInt)
				require.Len(t, held, 1)
				require.Equal(t, raw, held[0].Raw)
				require.Equal(t, crypto.PubkeyToAddress(h.key.PublicKey), held[0].Sender)
				require.Equal(t, c.tx.GetFeeCap(), &held[0].MaxFee)
				require.Equal(t, c.tx.GetTipCap(), &held[0].Tip)
				require.Equal(t, direction, held[0].Direction)
				require.Empty(t, h.up.bodies)
			})
		}
	}
	t.Run("an uncapped source takes any gas", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		h.capped = false
		require.Nil(t, h.send(t, h.sign(t, dynamic(0, 30_000_000, ptr(target)), chainID)).Error)
		require.Equal(t, 1, h.front.Pool.Len())
	})
	t.Run("the same transaction twice", func(t *testing.T) {
		h := newFront(t, composer.Outbound)
		raw := h.sign(t, dynamic(0, 100_000, ptr(target)), chainID)
		require.Nil(t, h.send(t, raw).Error)
		require.Nil(t, h.send(t, raw).Error)
		require.Equal(t, 1, h.front.Pool.Len())
	})
	t.Run("legacy price is cap and tip", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		require.Nil(t, h.send(t, h.sign(t, legacy(0, 100, ptr(target)), chainID)).Error)
		a := h.send(t, h.sign(t, legacy(0, 105, ptr(target)), chainID))
		require.NotNil(t, a.Error)
		require.Contains(t, a.Error.Message, "underpriced")
		require.Nil(t, h.send(t, h.sign(t, legacy(0, 110, ptr(target)), chainID)).Error)
		held := h.front.Pool.PopN(math.MaxInt)
		require.Len(t, held, 1)
		require.Equal(t, uint256.NewInt(110), &held[0].MaxFee)
		require.Equal(t, uint256.NewInt(110), &held[0].Tip)
	})
}

func TestFrontRefuses(t *testing.T) {
	down := errors.New("node down")
	for _, c := range []struct {
		name  string
		tx    func(h *harness) []byte
		setup func(h *harness)
		want  []string
		// reads reports that the refusal needed the source chain.
		reads bool
	}{
		{
			name: "gas over the cap", tx: func(h *harness) []byte { return h.sign(t, dynamic(0, 16_777_217, ptr(target)), chainID) },
			want: []string{"16777217", "16777216"},
		},
		{name: "wrong chain", tx: func(h *harness) []byte {
			tx := dynamic(0, 100_000, ptr(target))
			tx.ChainID.SetUint64(1)
			return h.sign(t, tx, 1)
		}, want: []string{"wrong source-chain id 1"}},
		{
			name: "unprotected legacy", tx: func(h *harness) []byte { return h.sign(t, legacy(0, 100, ptr(target)), 0) },
			want: []string{"missing source-chain id"},
		},
		{name: "blob", tx: func(h *harness) []byte {
			return h.sign(t, &types.BlobTx{DynamicFeeTransaction: *dynamic(0, 100_000, ptr(target)), BlobVersionedHashes: []common.Hash{{1}}}, chainID)
		}, want: []string{"unsupported transaction type 3"}},
		{name: "set code", tx: func(h *harness) []byte {
			return h.sign(t, &types.SetCodeTransaction{DynamicFeeTransaction: *dynamic(0, 100_000, ptr(target)), Authorizations: []types.Authorization{{}}}, chainID)
		}, want: []string{"unsupported transaction type 4"}},
		{name: "cost overflow", tx: func(h *harness) []byte {
			tx := dynamic(0, 100_000, ptr(target))
			tx.Value.SetAllOne()
			return h.sign(t, tx, chainID)
		}, want: []string{"overflow"}},
		{name: "a non-canonical encoding", tx: func(h *harness) []byte {
			wrapped, err := rlp.EncodeToBytes(h.sign(t, dynamic(0, 100_000, ptr(target)), chainID))
			require.NoError(t, err)
			return wrapped
		}, want: []string{"canonical"}},
		{
			name: "codeless target", tx: func(h *harness) []byte { return h.sign(t, dynamic(0, 100_000, ptr(codeless)), chainID) },
			want: []string{"has no code", "createCrossChainProxy"}, reads: true,
		},
		{
			name: "short balance", tx: func(h *harness) []byte { return h.sign(t, dynamic(0, 100_000, ptr(target)), chainID) },
			setup: func(h *harness) { h.src.balance = 100_000*100 - 1 }, want: []string{"insufficient balance"}, reads: true,
		},
		{
			name: "nonce gap", tx: func(h *harness) []byte { return h.sign(t, dynamic(2, 100_000, ptr(target)), chainID) },
			want: []string{"invalid nonce 2", "next unreserved nonce 0"}, reads: true,
		},
		{
			name: "source down", tx: func(h *harness) []byte { return h.sign(t, dynamic(0, 100_000, ptr(target)), chainID) },
			setup: func(h *harness) { h.src.failed = down }, want: []string{"node down"}, reads: true,
		},
		{
			name: "rules unknown", tx: func(h *harness) []byte { return h.sign(t, dynamic(0, 100_000, ptr(target)), chainID) },
			setup: func(h *harness) { h.capError = down }, want: []string{"gas limit lookup failed"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newFront(t, composer.Inbound)
			if c.setup != nil {
				c.setup(h)
			}
			a := h.send(t, c.tx(h))
			require.NotNil(t, a.Error)
			require.Equal(t, codeServerError, a.Error.Code)
			for _, w := range c.want {
				require.Contains(t, a.Error.Message, w)
			}
			require.Zero(t, h.front.Pool.Len())
			require.Empty(t, h.up.bodies)
			if !c.reads {
				require.Zero(t, h.src.reads, "refused before reading the source chain")
			}
		})
	}
}

// TestFrontReservations: a nonce drained for a batch is not taken again, and
// the chain continues above it.
func TestFrontReservations(t *testing.T) {
	h := newFront(t, composer.Inbound)
	require.Nil(t, h.send(t, h.sign(t, dynamic(0, 100_000, ptr(target)), chainID)).Error)
	require.Len(t, h.front.Pool.PopN(math.MaxInt), 1)
	a := h.send(t, h.sign(t, func() types.Transaction {
		tx := dynamic(0, 100_000, ptr(target))
		tx.FeeCap.SetUint64(200)
		tx.TipCap.SetUint64(20)
		return tx
	}(), chainID))
	require.NotNil(t, a.Error)
	require.Contains(t, a.Error.Message, "invalid nonce 0")
	require.Contains(t, a.Error.Message, "next unreserved nonce 1")
	require.NotNil(t, h.send(t, h.sign(t, dynamic(2, 100_000, ptr(target)), chainID)).Error)
	require.Nil(t, h.send(t, h.sign(t, dynamic(1, 100_000, ptr(target)), chainID)).Error)
}

func TestFrontRouting(t *testing.T) {
	t.Run("a creation goes to the source chain", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		a := h.send(t, h.sign(t, dynamic(0, 100_000, nil), chainID))
		require.JSONEq(t, `"upstream"`, string(a.Result))
		require.Len(t, h.up.bodies, 1)
		require.Zero(t, h.front.Pool.Len())
	})
	t.Run("an undecodable transaction goes to the source chain", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		a := h.send(t, []byte{0x02, 0x01})
		require.JSONEq(t, `"upstream"`, string(a.Result))
		require.Len(t, h.up.bodies, 1)
	})
	t.Run("eth methods are forwarded verbatim", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		body := `{"jsonrpc":"2.0","id":3,"method":"eth_getBalance","params":["0x01","latest"]}`
		_, a, _ := h.post(t, body)
		require.JSONEq(t, `"upstream"`, string(a.Result))
		require.Equal(t, []string{body}, h.up.bodies)
		batch := `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`
		h.post(t, batch)
		require.Equal(t, batch, h.up.bodies[1])
	})
	for name, body := range map[string]string{
		"another namespace":         `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":[]}`,
		"another namespace batched": `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"admin_peers"}]`,
		"a submission batched":      `[{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]}]`,
		"a submission variant":      `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransactionSync","params":["0x00"]}`,
		"not JSON":                  `{"jsonrpc"`,
	} {
		t.Run(name, func(t *testing.T) {
			h := newFront(t, composer.Inbound)
			_, a, raw := h.post(t, body)
			require.NotNil(t, a.Error, raw)
			require.Empty(t, h.up.bodies)
		})
	}
	t.Run("submissions wait for the front", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		h.front.ready.Store(false)
		a := h.send(t, h.sign(t, dynamic(0, 100_000, ptr(target)), chainID))
		require.NotNil(t, a.Error)
		require.Contains(t, a.Error.Message, "starting up")
		a = h.send(t, h.sign(t, dynamic(0, 100_000, nil), chainID))
		require.NotNil(t, a.Error, "a creation too")
		require.Zero(t, h.front.Pool.Len())
		require.Empty(t, h.up.bodies)
		require.Zero(t, h.src.reads)
	})
	t.Run("composer info is answered here, before the front is ready", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		h.front.ready.Store(false)
		for _, params := range []string{``, `,"params":[]`, `,"params":{}`} {
			_, a, raw := h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eez_composerInfo"`+params+`}`)
			require.Nil(t, a.Error, raw)
			require.JSONEq(t, `{"version":"test"}`, string(a.Result))
		}
		_, a, _ := h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eez_composerInfo","params":[1]}`)
		require.NotNil(t, a.Error)
		require.Equal(t, codeBadParams, a.Error.Code)
		require.JSONEq(t, `[1]`, string(a.Error.Data))
		require.Empty(t, h.up.bodies)
	})
	t.Run("methods other than POST", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, h.url, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
		req, err = http.NewRequestWithContext(t.Context(), http.MethodOptions, h.url, nil)
		require.NoError(t, err)
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		require.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	})
	t.Run("a body over 10 MiB", func(t *testing.T) {
		h := newFront(t, composer.Inbound)
		status, _, _ := h.post(t, `{"x":"`+strings.Repeat("a", maxBody)+`"}`)
		require.Equal(t, http.StatusRequestEntityTooLarge, status)
	})
}

// TestFrontAnswersCrossOrigin: an answer the front gives itself is
// readable cross-origin, as a forwarded one is.
func TestFrontAnswersCrossOrigin(t *testing.T) {
	h := newFront(t, composer.Inbound)
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"eez_composerInfo"}`,
		`{"jsonrpc":"2.0","id":1,"method":"net_version"}`,
		`[]`,
	} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, h.url, strings.NewReader(body))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"), body)
	}
	_, a, _ := h.post(t, `[]`)
	require.NotNil(t, a.Error)
	require.Equal(t, -32600, a.Error.Code)
}

// TestFrontRedactsItsUpstream: a failed forward logs no part of the
// upstream's URL past its host.
func TestFrontRedactsItsUpstream(t *testing.T) {
	h := newFront(t, composer.Inbound)
	var logs bytes.Buffer
	h.front.Logger = log.New()
	h.front.Logger.SetHandler(log.StreamHandler(&logs, log.TerminalFormatNoColor()))
	h.front.Upstream = "http://127.0.0.1:1/v1/secret-key"
	status, _, _ := h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`)
	require.Equal(t, http.StatusBadGateway, status)
	require.Contains(t, logs.String(), "eez.ingress.upstream")
	require.NotContains(t, logs.String(), "secret-key")
}

func TestInfo(t *testing.T) {
	info, err := Info(common.HexToAddress("0x01"), common.HexToAddress("0x4200000000000000000000000000000000000007"), 3151908, 6290, "v1")
	require.NoError(t, err)
	require.JSONEq(t, `{"eezContracts":{"eezRegistryAddress":"0x0000000000000000000000000000000000000001","eezL2Address":"0x4200000000000000000000000000000000000007"},"supportedNetworks":{"eezL1":3151908,"eezL2":6290},"version":"v1"}`, string(info))
}
