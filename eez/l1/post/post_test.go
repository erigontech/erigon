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

package post

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
)

const gwei = 1_000_000_000

type node struct {
	head    uint64
	nonce   uint64
	baseFee *uint256.Int
	balance *uint256.Int
	failed  error
	// refuse fails the sends with these indexes.
	refuse map[int]bool
	sends  int
	sent   [][]byte
}

func (n *node) ChainID(context.Context) (uint64, error) { return 3151908, nil }

func (n *node) Head(context.Context) (*l1.Header, error) {
	return &l1.Header{Number: n.head}, n.failed
}

func (n *node) PendingNonce(context.Context, common.Address) (uint64, error) {
	return n.nonce, n.failed
}

func (n *node) BaseFee(context.Context) (*uint256.Int, error) { return n.baseFee, n.failed }

func (n *node) Balance(context.Context, common.Address) (*uint256.Int, error) {
	if n.balance == nil {
		return nil, errors.New("no balance")
	}
	return n.balance, nil
}

func (n *node) SendRawTransaction(_ context.Context, raw []byte) error {
	i := n.sends
	n.sends++
	if n.refuse[i] {
		return fmt.Errorf("transaction %d refused", i)
	}
	n.sent = append(n.sent, raw)
	return nil
}

var eez = common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3")

func poster(t *testing.T, n *node, builder string) *Poster {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	p, err := New(t.Context(), key, eez, 10*gwei, builder, n, log.New())
	require.NoError(t, err)
	require.Equal(t, crypto.PubkeyToAddress(key.PublicKey), p.From())
	return p
}

func TestSign(t *testing.T) {
	input := []byte{0xe4, 0xa4, 0x80, 0xe0, 0, 1}
	for _, c := range []struct {
		name    string
		baseFee uint64
		balance *uint256.Int
		feeCap  uint64
	}{
		{"funded", 5 * gwei, uint256.NewInt(1 << 62), 20 * gwei},
		{"no base fee", 0, uint256.NewInt(1 << 62), 10 * gwei},
		{"underfunded", 5 * gwei, uint256.NewInt(1), 20 * gwei},
		{"balance unknown", 5 * gwei, nil, 20 * gwei},
	} {
		t.Run(c.name, func(t *testing.T) {
			n := &node{nonce: 7, baseFee: uint256.NewInt(c.baseFee), balance: c.balance}
			p := poster(t, n, "")
			tx, err := p.Sign(t.Context(), input, 1_000_000)
			require.NoError(t, err)
			require.Equal(t, uint8(types.DynamicFeeTxType), tx.Type())
			require.Equal(t, uint256.NewInt(3151908), tx.GetChainID())
			require.Equal(t, uint64(7), tx.GetNonce())
			require.Equal(t, uint64(1_000_000), tx.GetGasLimit())
			require.Equal(t, uint256.NewInt(c.feeCap), tx.GetFeeCap())
			require.Equal(t, uint256.NewInt(10*gwei), tx.GetTipCap())
			require.Equal(t, eez, *tx.GetTo())
			require.True(t, tx.GetValue().IsZero())
			require.Equal(t, input, tx.GetData())
			from, err := tx.Sender(*types.LatestSignerForChainID(uint256.NewInt(3151908)))
			require.NoError(t, err)
			require.Equal(t, p.From(), from.Value())
		})
	}
	t.Run("fee cap saturates", func(t *testing.T) {
		half := new(uint256.Int).Rsh(new(uint256.Int).SetAllOne(), 1)
		// Twice the first overflows; twice the second does not, the tip on it does.
		for _, base := range []*uint256.Int{new(uint256.Int).AddUint64(half, 1), new(uint256.Int).Sub(half, uint256.NewInt(gwei))} {
			n := &node{baseFee: base, balance: uint256.NewInt(1)}
			tx, err := poster(t, n, "").Sign(t.Context(), input, 1_000_000)
			require.NoError(t, err)
			require.Equal(t, new(uint256.Int).SetAllOne(), tx.GetFeeCap())
		}
	})
	t.Run("node failure", func(t *testing.T) {
		failure := errors.New("node down")
		n := &node{baseFee: uint256.NewInt(gwei), failed: failure}
		_, err := poster(t, n, "").Sign(t.Context(), input, 1_000_000)
		require.ErrorIs(t, err, failure)
	})
}

// builder answers eth_sendBundle with answer, keeping the requests.
type builder struct {
	status int
	answer string
	bodies []string
}

func (b *builder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	b.bodies = append(b.bodies, string(body))
	if b.status != 0 {
		w.WriteHeader(b.status)
		return
	}
	_, _ = io.WriteString(w, b.answer)
}

func serve(t *testing.T, b *builder) string {
	t.Helper()
	s := httptest.NewServer(b)
	t.Cleanup(s.Close)
	return s.URL
}

func requireJSON(t *testing.T, want, got string) {
	t.Helper()
	var w, g any
	require.NoError(t, json.Unmarshal([]byte(want), &w))
	require.NoError(t, json.Unmarshal([]byte(got), &g), got)
	require.Equal(t, w, g)
}

func TestSend(t *testing.T) {
	const (
		accepted    = `{"jsonrpc":"2.0","id":1,"result":{"bundleHash":"0x01"}}`
		null        = `{"jsonrpc":"2.0","id":1,"result":null}`
		rejected    = `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"bundle rejected"}}`
		unsupported = `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_sendBundle does not exist"}}`
	)
	for _, c := range []struct {
		name    string
		txs     int
		target  Target
		status  int
		answer  string
		refuse  map[int]bool
		params  string // the bundle's request params; empty when none is sent
		sent    Sent
		mempool int // transactions the mempool took
		tries   int // transactions sent to the mempool
		err     string
	}{
		{
			name: "pinned", txs: 1, target: Target{Block: 100, Time: 1234}, answer: accepted,
			params: `{"txs":["%s"],"blockNumber":"0x64","minTimestamp":1234,"maxTimestamp":1234}`, sent: Sent{Block: 100, Time: 1234},
		},
		{
			name: "block", txs: 1, target: Target{Block: 100}, answer: null,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, sent: Sent{Block: 100},
		},
		{
			name: "next block", txs: 1, answer: accepted,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, sent: Sent{Block: 100},
		},
		{
			name: "in order", txs: 2, target: Target{Block: 100}, answer: accepted,
			params: `{"txs":["%s","%s"],"blockNumber":"0x64"}`, sent: Sent{Block: 100},
		},
		{
			name: "rejected", txs: 1, target: Target{Block: 100}, answer: rejected,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, err: "bundle rejected",
		},
		{
			name: "builder failure", txs: 1, target: Target{Block: 100}, status: http.StatusBadGateway,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, err: "502",
		},
		{
			name: "mempool", txs: 2, target: Target{Block: 100, Time: 1234}, answer: unsupported,
			params: `{"txs":["%s","%s"],"blockNumber":"0x64","minTimestamp":1234,"maxTimestamp":1234}`, sent: Sent{Block: 100}, mempool: 2, tries: 2,
		},
		{
			name: "mempool refuses the call", txs: 2, target: Target{Block: 100}, answer: unsupported, refuse: map[int]bool{0: true},
			params: `{"txs":["%s","%s"],"blockNumber":"0x64"}`, tries: 1, err: "transaction 0 refused",
		},
		{
			name: "mempool refuses a later one", txs: 2, target: Target{Block: 100}, answer: unsupported, refuse: map[int]bool{1: true},
			params: `{"txs":["%s","%s"],"blockNumber":"0x64"}`, sent: Sent{Block: 100}, mempool: 1, tries: 2,
		},
		{name: "nothing to send", target: Target{Block: 100}, answer: accepted, err: "no transactions"},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := &builder{status: c.status, answer: c.answer}
			n := &node{head: 98, baseFee: uint256.NewInt(gwei), balance: uint256.NewInt(1 << 62), refuse: c.refuse}
			p := poster(t, n, serve(t, b))
			var txs []types.Transaction
			var raw []any
			for i := range c.txs {
				tx, err := p.Sign(t.Context(), []byte{byte(i)}, 100_000)
				require.NoError(t, err)
				enc, err := types.MarshalTransactionsBinary(types.Transactions{tx})
				require.NoError(t, err)
				txs, raw = append(txs, tx), append(raw, hexutil.Encode(enc[0]))
			}
			sent, err := p.Send(t.Context(), txs, c.target)
			if c.params == "" {
				require.Empty(t, b.bodies)
			} else {
				require.Len(t, b.bodies, 1)
				requireJSON(t, `{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[`+fmt.Sprintf(c.params, raw...)+`]}`, b.bodies[0])
			}
			if c.err != "" {
				require.ErrorContains(t, err, c.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, c.sent, sent)
			}
			require.Equal(t, c.tries, n.sends)
			require.Len(t, n.sent, c.mempool)
			for i, s := range n.sent {
				require.Equal(t, raw[i], hexutil.Encode(s))
			}
		})
	}
}

func TestSendAfterAMovedHead(t *testing.T) {
	b := &builder{answer: `{"jsonrpc":"2.0","id":1,"result":null}`}
	n := &node{head: 98, baseFee: uint256.NewInt(gwei)}
	p := poster(t, n, serve(t, b))
	tx, err := p.Sign(t.Context(), nil, 100_000)
	require.NoError(t, err)
	n.head = 200
	sent, err := p.Send(t.Context(), []types.Transaction{tx}, Target{})
	require.NoError(t, err)
	require.Equal(t, Sent{Block: 202}, sent)
	require.True(t, strings.Contains(b.bodies[0], `"0xca"`), b.bodies[0])
}
