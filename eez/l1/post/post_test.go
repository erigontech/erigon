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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	// refuse fails the sends with these indexes with a refusal, down
	// without an answer.
	refuse, down map[int]bool
	// hangChainID, hangHead, hangNonce and hangSend have those calls wait
	// for their context.
	hangChainID, hangHead, hangNonce, hangSend bool
	// refuseTip refuses a transaction tipping less, as a pool holding its
	// nonce does.
	refuseTip uint64
	sends     int
	sent      [][]byte
}

func (n *node) ChainID(ctx context.Context) (uint64, error) {
	if n.hangChainID {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return 3151908, nil
}

func (n *node) Head(ctx context.Context) (*l1.Header, error) {
	if n.hangHead {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &l1.Header{Number: n.head}, n.failed
}

func (n *node) Nonce(ctx context.Context, _ common.Address) (uint64, error) {
	if n.hangNonce {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return n.nonce, n.failed
}

func (n *node) BaseFee(context.Context) (*uint256.Int, error) { return n.baseFee, n.failed }

func (n *node) Balance(context.Context, common.Address) (*uint256.Int, error) {
	if n.balance == nil {
		return nil, errors.New("no balance")
	}
	return n.balance, nil
}

func (n *node) SendRawTransaction(ctx context.Context, raw []byte) error {
	i := n.sends
	n.sends++
	if n.hangSend {
		<-ctx.Done()
		return ctx.Err()
	}
	if n.refuseTip > 0 {
		tx, err := types.DecodeWrappedTransaction(raw)
		if err != nil {
			return err
		}
		if tx.GetTipCap().Uint64() < n.refuseTip {
			return fmt.Errorf("%w: replacement transaction underpriced", l1.ErrRefused)
		}
	}
	if n.refuse[i] {
		return fmt.Errorf("%w: transaction %d refused", l1.ErrRefused, i)
	}
	if n.down[i] {
		return fmt.Errorf("transaction %d: connection reset", i)
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

// TestSignReplaces: a posting signed at the nonce of one a pool may hold bids
// enough over it to replace it, compounding until a pool takes it, up to a
// ceiling; a bundle that missed its block is not outbid, and a new nonce
// starts afresh.
func TestSignReplaces(t *testing.T) {
	const (
		accepted    = `{"jsonrpc":"2.0","id":1,"result":{"bundleHash":"0x01"}}`
		unsupported = `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_sendBundle does not exist"}}`
	)
	fees := func(tx types.Transaction) [2]uint64 {
		return [2]uint64{tx.GetTipCap().Uint64(), tx.GetFeeCap().Uint64()}
	}
	// 5 gwei of base fee: a tip of 10 gwei and a fee cap of 20.
	fresh := [2]uint64{10 * gwei, 20 * gwei}
	outbids := func(t *testing.T, last, next types.Transaction) {
		t.Helper()
		require.GreaterOrEqual(t, next.GetTipCap().Uint64()*100, last.GetTipCap().Uint64()*115)
		require.GreaterOrEqual(t, next.GetFeeCap().Uint64()*100, last.GetFeeCap().Uint64()*115)
		require.GreaterOrEqual(t, next.GetFeeCap().Uint64(), next.GetTipCap().Uint64())
	}
	setup := func(t *testing.T, answer string, refuseTip uint64) (*node, *Poster, func(types.Transaction) error) {
		n := &node{nonce: 7, head: 98, baseFee: uint256.NewInt(5 * gwei), balance: uint256.NewInt(1 << 62), refuseTip: refuseTip}
		p := poster(t, n, serve(t, &builder{answer: answer}))
		send := func(tx types.Transaction) error {
			_, err := p.Send(t.Context(), []types.Transaction{tx}, Target{Block: 100})
			return err
		}
		return n, p, send
	}
	sign := func(t *testing.T, p *Poster) types.Transaction {
		t.Helper()
		tx, err := p.Sign(t.Context(), []byte{1}, 100_000)
		require.NoError(t, err)
		return tx
	}

	t.Run("a bundle that missed its block", func(t *testing.T) {
		_, p, send := setup(t, accepted, 0)
		require.NoError(t, send(sign(t, p)))
		again := sign(t, p)
		require.Equal(t, uint64(7), again.GetNonce())
		require.Equal(t, fresh, fees(again))
	})
	t.Run("a posting through the mempool", func(t *testing.T) {
		n, p, send := setup(t, unsupported, 0)
		first := sign(t, p)
		require.Equal(t, fresh, fees(first))
		require.NoError(t, send(first))
		again := sign(t, p)
		require.Equal(t, uint64(7), again.GetNonce(), "the confirmed nonce")
		outbids(t, first, again)
		n.nonce = 8
		require.Equal(t, fresh, fees(sign(t, p)), "a new nonce starts afresh")
	})
	t.Run("a pool holding the nonce", func(t *testing.T) {
		n, p, send := setup(t, unsupported, 25*gwei)
		last := sign(t, p)
		for range 10 {
			err := send(last)
			if err == nil {
				break
			}
			require.ErrorIs(t, err, l1.ErrRefused)
			next := sign(t, p)
			outbids(t, last, next)
			last = next
		}
		require.GreaterOrEqual(t, last.GetTipCap().Uint64(), uint64(25*gwei))
		require.Equal(t, n.sent[len(n.sent)-1], func() []byte {
			raw, err := types.MarshalTransactionsBinary(types.Transactions{last})
			require.NoError(t, err)
			return raw[0]
		}(), "the pool took the last")
	})
	t.Run("the ceiling", func(t *testing.T) {
		_, p, send := setup(t, unsupported, 1<<62)
		var last types.Transaction
		for range 30 {
			last = sign(t, p)
			require.ErrorIs(t, send(last), l1.ErrRefused)
		}
		require.Equal(t, [2]uint64{4 * fresh[0], 4 * fresh[1]}, fees(last))
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
		input   int // the posting's input size, else one byte
		target  Target
		status  int
		answer  string
		refuse  map[int]bool
		down    map[int]bool
		dead    bool   // the builder does not listen
		params  string // the bundle's request params; empty when none is sent
		unsure  bool   // the error leaves the posting possibly sent
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
			params: `{"txs":["%s","%s"],"blockNumber":"0x64","revertingTxHashes":["HASH1"]}`, sent: Sent{Block: 100},
		},
		{
			name: "rejected", txs: 1, target: Target{Block: 100}, answer: rejected,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, err: "bundle rejected",
		},
		{
			name: "builder failure", txs: 1, target: Target{Block: 100}, status: http.StatusBadGateway,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, err: "502", unsure: true, sent: Sent{Block: 100, Pooled: true},
		},
		{
			name: "builder refuses the request", txs: 1, target: Target{Block: 100}, status: http.StatusBadRequest,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, err: "400",
		},
		{
			name: "unreadable answer", txs: 1, target: Target{Block: 100}, answer: `{"jsonrpc":`,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, err: "answer", unsure: true, sent: Sent{Block: 100, Pooled: true},
		},
		{
			name: "builder unreachable", txs: 1, target: Target{Block: 100}, dead: true,
			err: "eth_sendBundle", unsure: true, sent: Sent{Block: 100, Pooled: true},
		},
		{
			name: "mempool", txs: 2, target: Target{Block: 100, Time: 1234}, answer: unsupported,
			params: `{"txs":["%s","%s"],"blockNumber":"0x64","minTimestamp":1234,"maxTimestamp":1234,"revertingTxHashes":["HASH1"]}`, sent: Sent{Block: 100, Pooled: true}, mempool: 2, tries: 2,
		},
		{
			name: "mempool refuses the call", txs: 2, target: Target{Block: 100}, answer: unsupported, refuse: map[int]bool{0: true},
			params: `{"txs":["%s","%s"],"blockNumber":"0x64","revertingTxHashes":["HASH1"]}`, tries: 1, err: "transaction 0 refused",
		},
		{
			name: "mempool unreachable", txs: 2, target: Target{Block: 100}, answer: unsupported, down: map[int]bool{0: true},
			params: `{"txs":["%s","%s"],"blockNumber":"0x64","revertingTxHashes":["HASH1"]}`, tries: 1, err: "connection reset", unsure: true, sent: Sent{Block: 100, Pooled: true},
		},
		{
			name: "mempool refuses a later one", txs: 2, target: Target{Block: 100}, answer: unsupported, refuse: map[int]bool{1: true},
			params: `{"txs":["%s","%s"],"blockNumber":"0x64","revertingTxHashes":["HASH1"]}`, sent: Sent{Block: 100, Pooled: true}, mempool: 1, tries: 2,
		},
		{
			name: "a bundle over the pool's limit", txs: 1, input: 128 * 1024, target: Target{Block: 100}, answer: accepted,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, sent: Sent{Block: 100},
		},
		{
			name: "the mempool takes nothing over its limit", txs: 1, input: 128 * 1024, target: Target{Block: 100}, answer: unsupported,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, err: "eth_sendBundle",
		},
		{
			name: "a block L1 has passed", txs: 1, target: Target{Block: 90}, answer: accepted,
			params: `{"txs":["%s"],"blockNumber":"0x64"}`, sent: Sent{Block: 100},
		},
		{
			name: "the block L1 builds next", txs: 1, target: Target{Block: 99}, answer: accepted,
			params: `{"txs":["%s"],"blockNumber":"0x63"}`, sent: Sent{Block: 99},
		},
		{
			name: "a passed block pinned to its time", txs: 1, target: Target{Block: 90, Time: 1234}, answer: accepted,
			params: `{"txs":["%s"],"blockNumber":"0x5a","minTimestamp":1234,"maxTimestamp":1234}`, sent: Sent{Block: 90, Time: 1234},
		},
		{name: "nothing to send", target: Target{Block: 100}, answer: accepted, err: "no transactions"},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := &builder{status: c.status, answer: c.answer}
			n := &node{head: 98, baseFee: uint256.NewInt(gwei), balance: uint256.NewInt(1 << 62), refuse: c.refuse, down: c.down}
			url := serve(t, b)
			if c.dead {
				url = "http://127.0.0.1:1"
			}
			p := poster(t, n, url)
			var txs []types.Transaction
			var raw []any
			for i := range c.txs {
				input := []byte{byte(i)}
				if i == 0 && c.input > 0 {
					input = bytes.Repeat([]byte{0xff}, c.input)
				}
				tx, err := p.Sign(t.Context(), input, 100_000)
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
				params := fmt.Sprintf(c.params, raw...)
				if len(txs) > 1 {
					params = strings.ReplaceAll(params, "HASH1", txs[1].Hash().Hex())
				}
				requireJSON(t, `{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[`+params+`]}`, b.bodies[0])
			}
			if c.err != "" {
				require.ErrorContains(t, err, c.err)
				require.Equal(t, c.unsure, errors.Is(err, ErrUnsure), "unsure")
				if c.unsure {
					require.Equal(t, c.sent, sent)
				}
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

// TestPosterOutlivesAHungNode: the poster's calls to an L1 node that stops
// answering end within its bound. Signing fails, as does a head read before
// anything is sent; a submission may have reached the pool.
func TestPosterOutlivesAHungNode(t *testing.T) {
	const unsupported = `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_sendBundle does not exist"}}`
	for name, c := range map[string]struct {
		hangNonce, hangHead, hangSend bool
		target                        Target
		unsure                        bool
	}{
		"nonce read": {hangNonce: true},
		"head read":  {hangHead: true},
		"submission": {hangSend: true, target: Target{Block: 100, Time: 1234}, unsure: true},
	} {
		t.Run(name, func(t *testing.T) {
			n := &node{head: 98, baseFee: uint256.NewInt(gwei), balance: uint256.NewInt(1 << 62)}
			p := poster(t, n, serve(t, &builder{answer: unsupported}))
			p.nodeTimeout = 50 * time.Millisecond
			var tx types.Transaction
			if !c.hangNonce {
				var err error
				tx, err = p.Sign(t.Context(), []byte{1}, 100_000)
				require.NoError(t, err)
			}
			n.hangNonce, n.hangHead, n.hangSend = c.hangNonce, c.hangHead, c.hangSend
			done := make(chan error, 1)
			go func() {
				if c.hangNonce {
					_, err := p.Sign(t.Context(), []byte{1}, 100_000)
					done <- err
					return
				}
				_, err := p.Send(t.Context(), []types.Transaction{tx}, c.target)
				done <- err
			}()
			select {
			case err := <-done:
				require.Error(t, err)
				require.Equal(t, c.unsure, errors.Is(err, ErrUnsure), "unsure")
			case <-time.After(5 * time.Second):
				t.Fatal("the poster waits on the L1 node")
			}
		})
	}
}

// TestNewOutlivesAHungNode: an L1 node that never answers its chain id fails
// New within the poster's bound, rather than holding the node's start.
func TestNewOutlivesAHungNode(t *testing.T) {
	defer func(d time.Duration) { nodeTimeout = d }(nodeTimeout)
	nodeTimeout = 50 * time.Millisecond
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := New(t.Context(), key, eez, 10*gwei, "http://127.0.0.1:1", &node{hangChainID: true}, log.New())
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("New waits on the L1 node")
	}
	p := poster(t, &node{}, "http://127.0.0.1:1")
	require.Equal(t, uint64(3151908), p.ChainID())
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
