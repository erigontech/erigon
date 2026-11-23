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

package prove

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove/provepb"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// attesterKey is the devnet attester's key, a well-known development key.
const attesterKey = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"

var secp256k1N, _ = new(big.Int).SetString("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141", 16)

func sign(t *testing.T, key *ecdsa.PrivateKey, hash []byte) []byte {
	t.Helper()
	sig, err := crypto.Sign(hash, key)
	require.NoError(t, err)
	sig[64] += 27
	return sig
}

// highS is sig with s replaced by N − s, which recovers to the same signer.
func highS(sig []byte) []byte {
	s := new(big.Int).Sub(secp256k1N, new(big.Int).SetBytes(sig[32:64]))
	out := bytes.Clone(sig)
	s.FillBytes(out[32:64])
	out[64] ^= 27 ^ 28
	return out
}

func withV(sig []byte, v byte) []byte {
	out := bytes.Clone(sig)
	out[64] = v
	return out
}

func TestAttest(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := crypto.PubkeyToAddress(key.PublicKey)
	hash := bytes.Repeat([]byte{0x7c}, 32)
	sig := sign(t, key, hash)
	for _, c := range []struct {
		name     string
		sig      []byte
		hash     []byte
		attester common.Address
		ok       bool
	}{
		{"valid", sig, hash, signer, true},
		{"other signer", sig, hash, common.HexToAddress("0xff"), false},
		{"other hash", sig, bytes.Repeat([]byte{0x7d}, 32), signer, false},
		{"short signature", make([]byte, 64), hash, signer, false},
		{"short hash", sig, hash[:31], signer, false},
		{"zero signature", make([]byte, 65), hash, signer, false},
		{"raw recovery id", withV(sig, sig[64]-27), hash, signer, false},
		{"v 29", withV(sig, 29), hash, signer, false},
		{"high s", highS(sig), hash, signer, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := attest(c.sig, c.hash, c.attester)
			if !c.ok {
				require.ErrorIs(t, err, ErrAttestation)
				return
			}
			require.NoError(t, err)
			require.Equal(t, common.BytesToHash(c.hash), got)
		})
	}
}

type oracle struct {
	RollupID uint64         `json:"rollup_id"`
	From     uint64         `json:"from_block"`
	To       uint64         `json:"to_block"`
	VKey     common.Hash    `json:"proof_system_vkey"`
	Hash     common.Hash    `json:"public_inputs_hash"`
	Attester common.Address `json:"attester"`
	Proof    string         `json:"mined_proof"`
}

type fixture struct {
	oracle
	proof  []byte
	batch  *protocol.ProofSystemBatch
	anchor *types.Header
	blocks map[uint64]*types.Block
	rlp    map[uint64][]byte
	hashes map[uint64][2]common.Hash
	wit    map[uint64]*Witness
}

// anchor402 is a devnet window settled by an anchor-only batch.
func anchor402(t *testing.T) *fixture { return loadFixture(t, "captured-devnet-anchor-402") }

// loadFixture loads a recorded window; its first block's witness holds the
// anchor's header.
func loadFixture(t *testing.T, name string) *fixture {
	t.Helper()
	w, err := testfixtures.LoadWindow(name)
	require.NoError(t, err)
	f := &fixture{blocks: map[uint64]*types.Block{}, rlp: map[uint64][]byte{}, hashes: map[uint64][2]common.Hash{}, wit: map[uint64]*Witness{}}
	enc, err := json.Marshal(w.Oracle)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(enc, &f.oracle))
	f.proof = common.FromHex(f.Proof)
	f.batch, err = protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	for _, b := range w.Blocks {
		block := new(types.Block)
		require.NoError(t, rlp.DecodeBytes(b.RLP, block), b.Number)
		f.blocks[b.Number], f.rlp[b.Number], f.hashes[b.Number] = block, b.RLP, [2]common.Hash{b.Hash, b.ParentHash}
		wit := &Witness{State: plain(b.Witness.State), Codes: plain(b.Witness.Codes), Keys: plain(b.Witness.Keys), Headers: plain(b.Witness.Headers)}
		f.wit[b.Number] = wit
	}
	require.Len(t, f.blocks, int(f.To-f.From+1))
	for _, enc := range f.wit[f.From].Headers {
		h := new(types.Header)
		require.NoError(t, rlp.DecodeBytes(enc, h))
		if h.Hash() == f.blocks[f.From].ParentHash() {
			f.anchor = h
		}
	}
	require.NotNil(t, f.anchor)
	return f
}

func plain(in []hexutil.Bytes) [][]byte {
	out := make([][]byte, len(in))
	for i, x := range in {
		out[i] = x
	}
	return out
}

func (f *fixture) window() *Window {
	return &Window{
		RollupID: f.RollupID, Batch: f.batch, Anchor: f.anchor, From: f.From, To: f.To,
		Block: func(_ context.Context, n uint64) (*types.Block, *Witness, error) {
			return f.blocks[n], f.wit[n], nil
		},
	}
}

func TestAttestTheMinedProof(t *testing.T) {
	f := anchor402(t)
	got, err := attest(f.proof, f.Hash[:], f.Attester)
	require.NoError(t, err)
	require.Equal(t, f.Hash, got)
}

func TestPublicInputsHash(t *testing.T) {
	f := anchor402(t)
	got, err := publicInputsHash(f.batch, f.VKey)
	require.NoError(t, err)
	require.Equal(t, f.Hash, got)
	other := *f.batch
	other.CallData = append(bytes.Clone(other.CallData), 0)
	got, err = publicInputsHash(&other, f.VKey)
	require.NoError(t, err)
	require.NotEqual(t, f.Hash, got)
}

// sessionID is the ID the test prover gives every session.
var sessionID = bytes.Repeat([]byte{0x5e}, 32)

// prover serves ProveStream as a baseline prover: each block is validated
// with the state root of its header, each Finalize answered by answer.
type prover struct {
	provepb.UnimplementedProverServer
	mu sync.Mutex
	// streams counts the streams opened, calls the Finalize requests.
	streams, calls int
	// frames are the requests of the last stream.
	frames []*provepb.ClientFrame
	// aborted counts streams the client ended before an answer.
	aborted int
	// answer answers Finalize call: a proof, a rejection, or a status
	// ending the stream.
	answer func(call int) (*provepb.Proof, *provepb.Rejected, error)
	// reply, when set, replaces the reply to request i of a stream; nil
	// sends none, errEnd ends the stream with no status.
	reply func(i int, f *provepb.ClientFrame, r *provepb.ServerFrame) (*provepb.ServerFrame, error)
}

var errEnd = errors.New("end the stream")

func (p *prover) ProveStream(s provepb.Prover_ProveStreamServer) error {
	p.mu.Lock()
	p.streams++
	p.mu.Unlock()
	var frames []*provepb.ClientFrame
	defer func() {
		p.mu.Lock()
		p.frames = frames
		p.mu.Unlock()
	}()
	for i := 0; ; i++ {
		f, err := s.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.mu.Lock()
				p.aborted++
				p.mu.Unlock()
			}
			return err
		}
		frames = append(frames, f)
		r := &provepb.ServerFrame{SessionId: sessionID, RequestId: f.RequestId, Epoch: 1}
		var done bool
		switch k := f.Kind.(type) {
		case *provepb.ClientFrame_Begin:
			r.Kind = &provepb.ServerFrame_Ready{Ready: &provepb.Ready{ValidatedThrough: k.Begin.AnchorNumber, ValidatedHash: k.Begin.AnchorHash}}
		case *provepb.ClientFrame_Block:
			b := new(types.Block)
			if err := rlp.DecodeBytes(k.Block.Data.Rlp, b); err != nil {
				return err
			}
			root := b.Root()
			r.Kind = &provepb.ServerFrame_Validated{Validated: &provepb.Validated{Number: k.Block.Data.Number, Hash: k.Block.Data.Hash, PostStateRoot: root[:]}}
		case *provepb.ClientFrame_Finalize:
			p.mu.Lock()
			p.calls++
			call := p.calls
			p.mu.Unlock()
			proof, rej, err := p.answer(call)
			if err != nil {
				return err
			}
			if rej != nil {
				r.Kind = &provepb.ServerFrame_Rejected{Rejected: rej}
			} else {
				r.Kind, done = &provepb.ServerFrame_Proof{Proof: proof}, true
			}
		default:
			r.Kind = &provepb.ServerFrame_Rejected{Rejected: &provepb.Rejected{Code: int32(codes.Unimplemented)}}
		}
		if p.reply != nil {
			if r, err = p.reply(i, f, r); errors.Is(err, errEnd) {
				return nil
			} else if err != nil {
				return err
			}
			if r == nil {
				continue
			}
		}
		if err := s.Send(r); err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (p *prover) last() (int, []*provepb.ClientFrame) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, p.frames
}

func serve(t *testing.T, p provepb.ProverServer) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	provepb.RegisterProverServer(s, p)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return "http://" + lis.Addr().String()
}

func dial(t *testing.T, url string, f *fixture) *Client {
	t.Helper()
	c, err := Dial(url, f.Attester, f.VKey)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (f *fixture) proves(int) (*provepb.Proof, *provepb.Rejected, error) {
	return &provepb.Proof{PublicInputsHash: f.Hash[:], Proof: f.proof}, nil, nil
}

func rejects(code codes.Code) func(int) (*provepb.Proof, *provepb.Rejected, error) {
	return func(int) (*provepb.Proof, *provepb.Rejected, error) {
		return nil, &provepb.Rejected{Code: int32(code), Message: "prover says no"}, nil
	}
}

func TestProveStreamsTheWindow(t *testing.T) {
	f := anchor402(t)
	p := &prover{answer: f.proves}
	c := dial(t, serve(t, p), f)
	sig, err := c.Prove(t.Context(), f.window())
	require.NoError(t, err)
	require.Equal(t, f.proof, sig)

	_, frames := p.last()
	blocks := int(f.To - f.From + 1)
	require.Len(t, frames, 2+blocks)
	for i, fr := range frames {
		require.Equal(t, uint64(i+1), fr.RequestId)
		if i == 0 {
			require.Empty(t, fr.SessionId)
			require.Zero(t, fr.Epoch)
			continue
		}
		require.Equal(t, sessionID, fr.SessionId)
		require.Equal(t, uint64(1), fr.Epoch)
	}
	begin := frames[0].GetBegin()
	require.NotNil(t, begin)
	anchorHash := f.anchor.Hash()
	require.Equal(t, f.RollupID, begin.RollupId)
	require.Equal(t, f.From-1, begin.AnchorNumber)
	require.Equal(t, anchorHash[:], begin.AnchorHash)
	require.Equal(t, f.anchor.Root[:], begin.AnchorStateRoot)
	for i, fr := range frames[1 : 1+blocks] {
		n := f.From + uint64(i)
		b := fr.GetBlock().GetData()
		require.NotNil(t, b, n)
		require.Equal(t, n, b.Number)
		hash, parent := f.hashes[n][0], f.hashes[n][1]
		require.Equal(t, hash[:], b.Hash, n)
		require.Equal(t, parent[:], b.ParentHash, n)
		require.Equal(t, f.rlp[n], b.Rlp, n)
		w := f.wit[n]
		require.Equal(t, w.State, b.Witness.State, n)
		require.Equal(t, w.Codes, b.Witness.Codes, n)
		require.Equal(t, w.Keys, b.Witness.Keys, n)
		require.Equal(t, w.Headers, b.Witness.Headers, n)
	}
	fin := frames[1+blocks].GetFinalize()
	require.NotNil(t, fin)
	require.Equal(t, f.From, fin.FromBlock)
	require.Equal(t, f.To, fin.ToBlock)
	terminal := f.hashes[f.To][0]
	require.Equal(t, terminal[:], fin.TerminalHash)
	unproven := *f.batch
	unproven.Proofs = nil
	calldata, err := protocol.EncodePostAndVerifyBatch(&unproven)
	require.NoError(t, err)
	require.Equal(t, calldata, fin.Settlement.AbiCalldata)
	require.Empty(t, fin.Settlement.Blobs)
	require.Nil(t, fin.Settlement.PostingContext)
}

func TestProveRefusesAForeignAttestation(t *testing.T) {
	f := anchor402(t)
	key, err := crypto.HexToECDSA(attesterKey)
	require.NoError(t, err)
	require.Equal(t, f.Attester, crypto.PubkeyToAddress(key.PublicKey))
	other, err := crypto.GenerateKey()
	require.NoError(t, err)
	elsewhere := bytes.Repeat([]byte{0x7d}, 32)
	for _, c := range []struct {
		name  string
		proof *provepb.Proof
	}{
		{"other signer", &provepb.Proof{PublicInputsHash: f.Hash[:], Proof: sign(t, other, f.Hash[:])}},
		{"other batch", &provepb.Proof{PublicInputsHash: elsewhere, Proof: sign(t, key, elsewhere)}},
		{"signature over another hash", &provepb.Proof{PublicInputsHash: f.Hash[:], Proof: sign(t, key, elsewhere)}},
		{"high s", &provepb.Proof{PublicInputsHash: f.Hash[:], Proof: highS(f.proof)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := &prover{answer: func(int) (*provepb.Proof, *provepb.Rejected, error) { return c.proof, nil, nil }}
			_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
			require.ErrorIs(t, err, ErrAttestation)
			require.False(t, Retryable(err))
		})
	}
}

// TestProveStatus: only an unavailable prover, a timeout and an abort are
// worth the same request again; a session the prover lost is an abort.
func TestProveStatus(t *testing.T) {
	f := anchor402(t)
	retryable := map[codes.Code]bool{codes.Unavailable: true, codes.DeadlineExceeded: true, codes.Aborted: true, codes.NotFound: true}
	for code := codes.Canceled; code <= codes.Unauthenticated; code++ {
		want := code
		if code == codes.NotFound {
			want = codes.Aborted
		}
		t.Run("rejected "+code.String(), func(t *testing.T) {
			_, err := dial(t, serve(t, &prover{answer: rejects(code)}), f).Prove(t.Context(), f.window())
			require.Error(t, err)
			require.Equal(t, want, status.Code(err), err)
			require.Equal(t, retryable[code], Retryable(err))
		})
		if code == codes.NotFound {
			continue
		}
		t.Run("ended "+code.String(), func(t *testing.T) {
			p := &prover{answer: func(int) (*provepb.Proof, *provepb.Rejected, error) {
				return nil, nil, status.Error(code, "prover says no")
			}}
			_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
			require.Error(t, err)
			require.Equal(t, code, status.Code(err), err)
			require.Equal(t, retryable[code], Retryable(err))
		})
	}
	t.Run("rejected OK", func(t *testing.T) {
		_, err := dial(t, serve(t, &prover{answer: rejects(codes.OK)}), f).Prove(t.Context(), f.window())
		require.Equal(t, codes.Unknown, status.Code(err), err)
	})
	t.Run("no prover", func(t *testing.T) {
		lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		url := "http://" + lis.Addr().String()
		require.NoError(t, lis.Close())
		_, err = dial(t, url, f).Prove(t.Context(), f.window())
		require.Error(t, err)
		require.True(t, Retryable(err), err)
	})
	t.Run("prover deadline", func(t *testing.T) {
		release := make(chan struct{})
		p := &prover{answer: func(int) (*provepb.Proof, *provepb.Rejected, error) {
			<-release
			return nil, nil, status.Error(codes.Internal, "released")
		}}
		url := serve(t, p)
		t.Cleanup(func() { close(release) })
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err := dial(t, url, f).Prove(ctx, f.window())
		require.Error(t, err)
		require.True(t, Retryable(err), err)
	})
}

// TestProveChecksTheReplies: a reply that does not answer its request, or
// answers it for other blocks, fails the request for good.
func TestProveChecksTheReplies(t *testing.T) {
	f := anchor402(t)
	block := 1 // request index of the window's first block
	for _, c := range []struct {
		name  string
		reply func(i int, r *provepb.ServerFrame)
	}{
		{"ready at another anchor", func(i int, r *provepb.ServerFrame) {
			if i == 0 {
				r.GetReady().ValidatedThrough++
			}
		}},
		{"ready at another hash", func(i int, r *provepb.ServerFrame) {
			if i == 0 {
				r.GetReady().ValidatedHash = bytes.Repeat([]byte{1}, 32)
			}
		}},
		// On every reply, so that only Ready's check can catch it.
		{"short session ID", func(_ int, r *provepb.ServerFrame) { r.SessionId = r.SessionId[:31] }},
		{"epoch zero", func(_ int, r *provepb.ServerFrame) { r.Epoch = 0 }},
		{"begin answered by a validation", func(i int, r *provepb.ServerFrame) {
			if i == 0 {
				r.Kind = &provepb.ServerFrame_Validated{Validated: &provepb.Validated{}}
			}
		}},
		{"validated as another block", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.GetValidated().Number++
			}
		}},
		{"validated under another hash", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.GetValidated().Hash = bytes.Repeat([]byte{1}, 32)
			}
		}},
		{"validated to another root", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.GetValidated().PostStateRoot = bytes.Repeat([]byte{1}, 32)
			}
		}},
		{"block answered by readiness", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.Kind = &provepb.ServerFrame_Ready{Ready: &provepb.Ready{}}
			}
		}},
		{"reply to another request", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.RequestId++
			}
		}},
		{"reply of another session", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.SessionId = bytes.Repeat([]byte{1}, 32)
			}
		}},
		{"reply of another epoch", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.Epoch = 2
			}
		}},
		{"rejection of another session", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.SessionId = bytes.Repeat([]byte{1}, 32)
				r.Kind = &provepb.ServerFrame_Rejected{Rejected: &provepb.Rejected{Code: int32(codes.Unavailable)}}
			}
		}},
		{"rejection of another epoch", func(i int, r *provepb.ServerFrame) {
			if i == block {
				r.Epoch = 2
				r.Kind = &provepb.ServerFrame_Rejected{Rejected: &provepb.Rejected{Code: int32(codes.Unavailable)}}
			}
		}},
		{"finalize answered by a validation", func(i int, r *provepb.ServerFrame) {
			if r.GetProof() != nil {
				r.Kind = &provepb.ServerFrame_Validated{Validated: &provepb.Validated{}}
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := &prover{answer: f.proves, reply: func(i int, _ *provepb.ClientFrame, r *provepb.ServerFrame) (*provepb.ServerFrame, error) {
				c.reply(i, r)
				return r, nil
			}}
			_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
			require.ErrorIs(t, err, ErrReply)
			require.False(t, Retryable(err))
		})
	}
	t.Run("unbound rejection", func(t *testing.T) {
		p := &prover{answer: f.proves, reply: func(i int, _ *provepb.ClientFrame, r *provepb.ServerFrame) (*provepb.ServerFrame, error) {
			if i == block {
				return &provepb.ServerFrame{RequestId: r.RequestId, Kind: &provepb.ServerFrame_Rejected{Rejected: &provepb.Rejected{Code: int32(codes.ResourceExhausted)}}}, nil
			}
			return r, nil
		}}
		_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
	})
	t.Run("oversized reply", func(t *testing.T) {
		p := &prover{answer: func(int) (*provepb.Proof, *provepb.Rejected, error) {
			return nil, &provepb.Rejected{Code: int32(codes.FailedPrecondition), Message: string(bytes.Repeat([]byte{'x'}, maxReply))}, nil
		}}
		_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		require.False(t, Retryable(err))
	})
}

// TestProveWaitsForEachReply: a prover that stops answering times the
// request out after one exchange's wait, however long the caller waits.
func TestProveWaitsForEachReply(t *testing.T) {
	f := anchor402(t)
	defer func(d time.Duration) { exchange = d }(exchange)
	exchange = 200 * time.Millisecond
	p := &prover{answer: f.proves, reply: func(i int, _ *provepb.ClientFrame, r *provepb.ServerFrame) (*provepb.ServerFrame, error) {
		if i == 2 {
			return nil, nil
		}
		return r, nil
	}}
	start := time.Now()
	_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), err)
	require.True(t, Retryable(err))
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestProveRefusesABrokenWindow(t *testing.T) {
	f := anchor402(t)
	failure := errors.New("store failed")
	for _, c := range []struct {
		name   string
		block  func(ctx context.Context, n uint64) (*types.Block, *Witness, error)
		anchor *types.Header
		is     error
		short  uint64
	}{
		// Linked blocks, each a height above the one asked for.
		{"blocks of other heights", func(_ context.Context, n uint64) (*types.Block, *Witness, error) {
			return f.blocks[n+1], f.wit[n+1], nil
		}, nil, nil, 1},
		{"broken parent link", func(_ context.Context, n uint64) (*types.Block, *Witness, error) {
			if n == f.From+2 {
				h := f.blocks[n].Header()
				h.ParentHash = common.Hash{1}
				return f.blocks[n].WithSeal(h), f.wit[n], nil
			}
			return f.blocks[n], f.wit[n], nil
		}, nil, nil, 0},
		{"anchor not the parent", nil, func() *types.Header {
			h := types.CopyHeader(f.anchor)
			h.Extra = []byte{1}
			return h
		}(), nil, 0},
		{"missing witness", func(_ context.Context, n uint64) (*types.Block, *Witness, error) {
			if n == f.To {
				return f.blocks[n], nil, nil
			}
			return f.blocks[n], f.wit[n], nil
		}, nil, nil, 0},
		{"store failure", func(_ context.Context, n uint64) (*types.Block, *Witness, error) {
			if n == f.To {
				return nil, nil, failure
			}
			return f.blocks[n], f.wit[n], nil
		}, nil, failure, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := &prover{answer: f.proves}
			w := f.window()
			if c.block != nil {
				w.Block = c.block
			}
			if c.anchor != nil {
				w.Anchor = c.anchor
			}
			w.To -= c.short
			_, err := dial(t, serve(t, p), f).Prove(t.Context(), w)
			require.Error(t, err)
			if c.is != nil {
				require.ErrorIs(t, err, c.is)
			}
			require.False(t, Retryable(err), err)
			calls, _ := p.last()
			require.Zero(t, calls, "the prover finalized a broken window")
			require.Eventually(t, func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.aborted == 1
			}, 5*time.Second, time.Millisecond, "the prover saw a complete window")
		})
	}
}

func TestProveByRetries(t *testing.T) {
	f := anchor402(t)
	p := &prover{answer: func(call int) (*provepb.Proof, *provepb.Rejected, error) {
		if call == 1 {
			return nil, &provepb.Rejected{Code: int32(codes.Unavailable), Message: "warming up"}, nil
		}
		return f.proves(call)
	}}
	sig, err := dial(t, serve(t, p), f).ProveBy(t.Context(), f.window(), time.Now().Add(2*time.Second), 0)
	require.NoError(t, err)
	require.Equal(t, f.proof, sig)
	calls, frames := p.last()
	require.Equal(t, 2, calls)
	require.Len(t, frames, 2+int(f.To-f.From+1), "a retry sends the whole window again")
	require.NotNil(t, frames[0].GetBegin(), "a retry begins a new session")
	require.Empty(t, frames[0].SessionId)
}

// TestProveTakesAnEndAsAnOutage: a stream ending with no status before
// its reply leaves the request unanswered, worth trying again.
func TestProveTakesAnEndAsAnOutage(t *testing.T) {
	f := anchor402(t)
	p := &prover{answer: f.proves, reply: func(i int, _ *provepb.ClientFrame, r *provepb.ServerFrame) (*provepb.ServerFrame, error) {
		if i == 2 {
			return nil, errEnd
		}
		return r, nil
	}}
	_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
	require.Equal(t, codes.Unavailable, status.Code(err), err)
	require.True(t, Retryable(err))
}

// TestProveKeepsAnEarlyStatus: a prover ending the stream before the window
// ends fails the request with its own status.
func TestProveKeepsAnEarlyStatus(t *testing.T) {
	f := anchor402(t)
	p := &prover{answer: f.proves, reply: func(i int, _ *provepb.ClientFrame, r *provepb.ServerFrame) (*provepb.ServerFrame, error) {
		if i == 2 {
			return nil, status.Error(codes.Unavailable, "draining")
		}
		return r, nil
	}}
	_, err := dial(t, serve(t, p), f).Prove(t.Context(), f.window())
	require.Equal(t, codes.Unavailable, status.Code(err), err)
	require.True(t, Retryable(err))
}

// TestProveOutlastsAStore: a store read cut by the request's deadline is a
// timeout like any other.
func TestProveOutlastsAStore(t *testing.T) {
	f := anchor402(t)
	p := &prover{answer: f.proves}
	w := f.window()
	w.Block = func(ctx context.Context, n uint64) (*types.Block, *Witness, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := dial(t, serve(t, p), f).Prove(ctx, w)
	require.True(t, Retryable(err), err)
}

func TestProveRefusesAnUnprovableRequest(t *testing.T) {
	f := anchor402(t)
	for _, c := range []struct {
		name  string
		shape func(w *Window)
	}{
		{"window from genesis", func(w *Window) { w.From = 0 }},
		{"window upside down", func(w *Window) { w.To = w.From - 1 }},
		{"no anchor", func(w *Window) { w.Anchor = nil }},
		{"anchor of another height", func(w *Window) { w.Anchor = f.blocks[f.From].Header() }},
		{"live batch", func(w *Window) {
			b := *w.Batch
			b.BlockNumber = protocol.LiveBlockNumber
			w.Batch = &b
		}},
		{"bound sender", func(w *Window) {
			b := *w.Batch
			b.BindMsgSenderInPublicInput = true
			w.Batch = &b
		}},
		{"two proof systems", func(w *Window) {
			b := *w.Batch
			b.ProofSystems = append(slices.Clone(b.ProofSystems), common.Address{1})
			b.RollupIDsWithProofSystems = []protocol.RollupIDWithProofSystems{{RollupID: 1, ProofSystemIndexes: []uint64{0, 1}}}
			w.Batch = &b
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := &prover{answer: f.proves}
			w := f.window()
			c.shape(w)
			_, err := dial(t, serve(t, p), f).Prove(t.Context(), w)
			require.Error(t, err)
			require.False(t, Retryable(err))
			require.Never(t, func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.streams > 0
			}, 200*time.Millisecond, 10*time.Millisecond, "a stream reached the prover")
		})
	}
}

func TestDial(t *testing.T) {
	for _, url := range []string{"http://127.0.0.1:50061", "http://127.0.0.1:50061/", "https://prover.example:443"} {
		c, err := Dial(url, common.Address{}, common.Hash{})
		require.NoError(t, err, url)
		require.NoError(t, c.Close())
	}
	for _, url := range []string{"127.0.0.1:50061", "http://127.0.0.1", "http://127.0.0.1:50061/prove", "grpc://127.0.0.1:50061", "http://127.0.0.1:50061?x=1"} {
		_, err := Dial(url, common.Address{}, common.Hash{})
		require.Error(t, err, url)
	}
	// User info, path and query may carry an API key.
	for _, url := range []string{"http://user:secret@prover.example/KEY", "ftp://user:secret@prover.example:1", "http://prover.example:1/KEY?k=KEY", "http://user:secret@[prover/KEY"} {
		_, err := Dial(url, common.Address{}, common.Hash{})
		require.Error(t, err, url)
		require.NotContains(t, err.Error(), "secret", url)
		require.NotContains(t, err.Error(), "KEY", url)
	}
}

// TestProveAfterAnOutage: a prover back from an outage takes the next
// request within its budget, however long the outage.
func TestProveAfterAnOutage(t *testing.T) {
	f := anchor402(t)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	c := dial(t, "http://"+addr, f)
	for end := time.Now().Add(6 * time.Second); time.Now().Before(end); time.Sleep(300 * time.Millisecond) {
		_, err := c.Prove(t.Context(), f.window())
		require.True(t, Retryable(err), err)
	}
	lis, err = (&net.ListenConfig{}).Listen(t.Context(), "tcp", addr)
	require.NoError(t, err)
	s := grpc.NewServer()
	provepb.RegisterProverServer(s, &prover{answer: f.proves})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	sig, err := c.ProveBy(t.Context(), f.window(), time.Now().Add(time.Second), 0)
	require.NoError(t, err)
	require.Equal(t, f.proof, sig)
}

// TestProveConnectsSlowly: a connection slower than the redial backoff, as
// to a remote prover over TLS, is waited for.
func TestProveConnectsSlowly(t *testing.T) {
	f := anchor402(t)
	back := strings.TrimPrefix(serve(t, &prover{answer: f.proves}), "http://")
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	// Each connection reaches the prover after 300 ms.
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				time.Sleep(300 * time.Millisecond)
				b, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", back)
				if err != nil {
					return
				}
				defer b.Close()
				go func() { _, _ = io.Copy(b, c) }()
				_, _ = io.Copy(c, b)
			}()
		}
	}()
	sig, err := dial(t, "http://"+lis.Addr().String(), f).Prove(t.Context(), f.window())
	require.NoError(t, err)
	require.Equal(t, f.proof, sig)
}
