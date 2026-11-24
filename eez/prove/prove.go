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

// Package prove asks a prover to attest a settlement window over
// prove.v1.Prover/ProveStream, as a baseline session.
//
// proto/prove.proto is copied unchanged from eez-association/eez-rollup0 at
// 4e80655 (crates/eez-control-rpc/proto/prove.proto), MIT licensed; provepb
// is generated from it by the root Makefile's grpc target. The session
// follows that revision's crates/eez-control-rpc/SPEC.md.
package prove

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/url"
	"slices"
	"time"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove/provepb"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

const (
	maxMessage = 1 << 30
	// maxReply bounds a decoded ServerFrame, as the reference composer does
	// (SPEC §2): a larger reply fails the request.
	maxReply     = 64 << 10
	firstBackoff = 100 // ms
	maxBackoff   = 1000
)

// exchange bounds the wait for each reply (SPEC §5).
var exchange = 5 * time.Second

var (
	// ErrAttestation reports a proof that does not attest the window.
	ErrAttestation = errors.New("prover response does not attest the window")
	// ErrReply reports a reply that does not answer its request.
	ErrReply = errors.New("prover reply does not answer the request")
)

// Witness is a block's execution witness.
type Witness struct {
	State, Codes, Keys, Headers [][]byte
}

// Window is a run of consecutive blocks a batch settles.
type Window struct {
	RollupID uint64
	// Batch settles the window; its proofs are left out of the request.
	Batch *protocol.ProofSystemBatch
	// Anchor is block From-1, the parent the batch settles from.
	Anchor   *types.Header
	From, To uint64
	// Block returns window block n with its witness.
	Block func(ctx context.Context, n uint64) (*types.Block, *Witness, error)
}

// Client asks a prover for the attestation of a window.
type Client struct {
	conn     *grpc.ClientConn
	prover   provepb.ProverClient
	attester common.Address
	vkey     common.Hash
}

// Dial returns a client of the prover at url, http://host:port or
// https://host:port, whose attestations recover to attester; vkey is the
// proof system's verification key in the public inputs.
func Dial(rawURL string, attester common.Address, vkey common.Hash) (*Client, error) {
	target, secure, err := Target(rawURL)
	if err != nil {
		return nil, err
	}
	creds := insecure.NewCredentials()
	if secure {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessage), grpc.MaxCallRecvMsgSize(maxReply)),
		// A slot's budget is seconds: a prover back from an outage is
		// redialled at once, not after a backoff grown over the outage.
		// Without a minimum, a connection attempt would have only the
		// backoff's 100 ms.
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second}, MinConnectTimeout: exchange}))
	if err != nil {
		return nil, fmt.Errorf("prover %s: %w", target, err)
	}
	return &Client{conn: conn, prover: provepb.NewProverClient(conn), attester: attester, vkey: vkey}, nil
}

// Target returns the host:port of a prover's URL, http://host:port or
// https://host:port, and whether it takes TLS. Its errors print the URL's
// scheme and host only: user info, path and query may carry an API key.
func Target(rawURL string) (string, bool, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false, errors.New("prover url is not a URL")
	}
	if _, port, err := net.SplitHostPort(u.Host); err != nil || port == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return "", false, fmt.Errorf("prover url %s://%s: want scheme://host:port alone", u.Scheme, u.Host)
	}
	switch u.Scheme {
	case "http":
		return u.Host, false, nil
	case "https":
		return u.Host, true, nil
	}
	return "", false, fmt.Errorf("prover url %s://%s: want http or https", u.Scheme, u.Host)
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Prove returns the attester's 65-byte signature over the public inputs hash
// of w's batch, from one session: Begin at the anchor, each block validated
// in turn, then Finalize over the window.
func (c *Client) Prove(ctx context.Context, w *Window) ([]byte, error) {
	if w.From == 0 || w.To < w.From {
		return nil, fmt.Errorf("window %d..%d", w.From, w.To)
	}
	if w.Anchor == nil || w.Anchor.Number.Uint64() != w.From-1 {
		return nil, fmt.Errorf("window %d..%d without block %d as its anchor", w.From, w.To, w.From-1)
	}
	if w.Batch.BlockNumber != 0 {
		return nil, fmt.Errorf("batch of blockNumber %d: only timeless batches are proven", w.Batch.BlockNumber)
	}
	unproven := *w.Batch
	unproven.Proofs = nil
	calldata, err := protocol.EncodePostAndVerifyBatch(&unproven)
	if err != nil {
		return nil, err
	}
	want, err := publicInputsHash(&unproven, c.vkey)
	if err != nil {
		return nil, err
	}
	c.conn.ResetConnectBackoff()
	// Ending ctx aborts the stream, so the prover never takes a partial window.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.prover.ProveStream(ctx)
	if err != nil {
		return nil, err
	}
	s := &session{stream: stream, cancel: cancel}
	parent, root := w.Anchor.Hash(), w.Anchor.Root
	if err := s.begin(w.RollupID, w.From-1, parent, root); err != nil {
		return nil, err
	}
	for n := w.From; n <= w.To; n++ {
		chunk, root, err := blockChunk(ctx, w, n, parent)
		if err != nil {
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, fmt.Errorf("window block %d: %w", n, err)
		}
		if err := s.block(chunk, root); err != nil {
			return nil, err
		}
		parent = common.BytesToHash(chunk.Hash)
	}
	proof, err := s.finalize(&provepb.Finalize{FromBlock: w.From, ToBlock: w.To, TerminalHash: parent[:], Settlement: &provepb.SettlementInput{AbiCalldata: calldata}})
	if err != nil {
		return nil, err
	}
	got, err := attest(proof.Proof, proof.PublicInputsHash, c.attester)
	if err != nil {
		return nil, err
	}
	if got != want {
		return nil, fmt.Errorf("%w: signed %x, the batch's public inputs hash is %x", ErrAttestation, got, want)
	}
	return proof.Proof, nil
}

// session is one stream's binding to the prover, request by request.
type session struct {
	stream  grpc.BidiStreamingClient[provepb.ClientFrame, provepb.ServerFrame]
	cancel  context.CancelFunc
	id      []byte
	epoch   uint64
	request uint64
}

// begin binds the session to its anchor.
func (s *session) begin(rollupID, number uint64, hash, root common.Hash) error {
	r, err := s.exchange(&provepb.ClientFrame{Kind: &provepb.ClientFrame_Begin{Begin: &provepb.Begin{RollupId: rollupID, AnchorNumber: number, AnchorHash: hash[:], AnchorStateRoot: root[:]}}})
	if err != nil {
		return err
	}
	ready := r.GetReady()
	switch {
	case ready == nil:
		return fmt.Errorf("%w: Begin answered by %T", ErrReply, r.GetKind())
	case len(r.SessionId) != 32 || r.Epoch == 0:
		return fmt.Errorf("%w: session of %d-byte ID, epoch %d", ErrReply, len(r.SessionId), r.Epoch)
	case ready.ValidatedThrough != number || !bytes.Equal(ready.ValidatedHash, hash[:]):
		return fmt.Errorf("%w: ready at %d %x, not the anchor %d %x", ErrReply, ready.ValidatedThrough, ready.ValidatedHash, number, hash)
	}
	s.id, s.epoch = r.SessionId, r.Epoch
	return nil
}

// block has the prover validate b, which leaves state root root.
func (s *session) block(b *provepb.BlockWitness, root common.Hash) error {
	r, err := s.exchange(&provepb.ClientFrame{Kind: &provepb.ClientFrame_Block{Block: &provepb.Block{Data: b}}})
	if err != nil {
		return err
	}
	v := r.GetValidated()
	switch {
	case v == nil:
		return fmt.Errorf("%w: block %d answered by %T", ErrReply, b.Number, r.GetKind())
	case v.Number != b.Number || !bytes.Equal(v.Hash, b.Hash) || !bytes.Equal(v.PostStateRoot, root[:]):
		return fmt.Errorf("%w: block %d %x of root %x validated as %d %x of root %x", ErrReply, b.Number, b.Hash, root, v.Number, v.Hash, v.PostStateRoot)
	}
	return nil
}

// finalize asks for the proof of f's range.
func (s *session) finalize(f *provepb.Finalize) (*provepb.Proof, error) {
	r, err := s.exchange(&provepb.ClientFrame{Kind: &provepb.ClientFrame_Finalize{Finalize: f}})
	if err != nil {
		return nil, err
	}
	if r.GetProof() == nil {
		return nil, fmt.Errorf("%w: Finalize answered by %T", ErrReply, r.GetKind())
	}
	return r.GetProof(), nil
}

// exchange sends f as the session's next request and returns its reply; a
// rejection is returned as its status.
func (s *session) exchange(f *provepb.ClientFrame) (*provepb.ServerFrame, error) {
	s.request++
	f.SessionId, f.RequestId, f.Epoch = s.id, s.request, s.epoch
	t := time.AfterFunc(exchange, s.cancel)
	r, err := s.send(f)
	// A timer that fired has cancelled the stream, whatever came back.
	if !t.Stop() {
		return nil, status.Errorf(codes.DeadlineExceeded, "no reply to request %d within %v", f.RequestId, exchange)
	}
	if err != nil {
		return nil, err
	}
	// A rejection the prover cannot bind to the session carries none.
	unbound := r.GetRejected() != nil && len(r.SessionId) == 0 && r.Epoch == 0
	switch {
	case r.RequestId != f.RequestId:
		return nil, fmt.Errorf("%w: reply to request %d, not %d", ErrReply, r.RequestId, f.RequestId)
	case s.id != nil && !unbound && (!bytes.Equal(r.SessionId, s.id) || r.Epoch != s.epoch):
		return nil, fmt.Errorf("%w: reply of session %x epoch %d, not %x epoch %d", ErrReply, r.SessionId, r.Epoch, s.id, s.epoch)
	}
	if rej := r.GetRejected(); rej != nil {
		return nil, rejection(rej)
	}
	return r, nil
}

func (s *session) send(f *provepb.ClientFrame) (*provepb.ServerFrame, error) {
	if err := s.stream.Send(f); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	// After io.EOF from Send, Recv returns the status that ended the stream.
	r, err := s.stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, status.Errorf(codes.Unavailable, "stream ended before the reply to request %d", f.RequestId)
	}
	return r, err
}

// rejection is the status of a rejected request. A session the prover no
// longer holds is lost like a dropped stream: a new session can begin.
func rejection(r *provepb.Rejected) error {
	code := codes.Code(r.Code)
	if code == codes.OK {
		code = codes.Unknown
	}
	if code == codes.NotFound {
		return status.Errorf(codes.Aborted, "session lost: %s", r.Message)
	}
	return status.Error(code, r.Message)
}

// blockChunk is window block n, the child of parent, with its state root.
func blockChunk(ctx context.Context, w *Window, n uint64, parent common.Hash) (*provepb.BlockWitness, common.Hash, error) {
	block, wit, err := w.Block(ctx, n)
	switch {
	case err != nil:
		return nil, common.Hash{}, err
	case block == nil || block.NumberU64() != n:
		return nil, common.Hash{}, errors.New("not the block of that height")
	case wit == nil:
		return nil, common.Hash{}, errors.New("no witness")
	case block.ParentHash() != parent:
		return nil, common.Hash{}, fmt.Errorf("parent %x, not the previous block %x", block.ParentHash(), parent)
	}
	enc, err := rlp.EncodeToBytes(block)
	if err != nil {
		return nil, common.Hash{}, err
	}
	hash, parentHash := block.Hash(), block.ParentHash()
	return &provepb.BlockWitness{
		Number:     n,
		Hash:       hash[:],
		ParentHash: parentHash[:],
		Rlp:        enc,
		Witness:    &provepb.ExecutionWitness{State: wit.State, Codes: wit.Codes, Keys: wit.Keys, Headers: wit.Headers},
	}, block.Root(), nil
}

// ProveBy proves w, repeating the request after a transient failure while a
// request can start by start. Each request has per, or with per zero what is
// left before start.
func (c *Client) ProveBy(ctx context.Context, w *Window, start time.Time, per time.Duration) ([]byte, error) {
	return retry(ctx, start, per, func(ctx context.Context) ([]byte, error) { return c.Prove(ctx, w) })
}

// Retryable reports a transient failure, which the same request may not meet
// again.
func Retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	}
	return false
}

func retry(ctx context.Context, start time.Time, per time.Duration, attempt func(context.Context) ([]byte, error)) ([]byte, error) {
	if time.Now().After(start) {
		return nil, status.Error(codes.DeadlineExceeded, "too late to start a proof request")
	}
	for n := 1; ; n++ {
		limit := per
		if limit == 0 {
			limit = time.Until(start)
		}
		actx, cancel := context.WithTimeout(ctx, limit)
		proof, err := attempt(actx)
		cancel()
		switch {
		case err == nil:
			return proof, nil
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case !Retryable(err):
			return nil, err
		}
		wait := backoff(n, rand.Uint64())
		if !time.Now().Add(wait).Before(start) {
			return nil, err
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		if !time.Now().Before(start) {
			return nil, err
		}
	}
}

// backoff is the wait after failed attempt: doubling from 100 ms up to 1 s,
// within ±20% by seed.
func backoff(attempt int, seed uint64) time.Duration {
	base := min(uint64(firstBackoff)<<min(max(attempt-1, 0), 31), maxBackoff)
	jitter := base / 5
	return time.Duration(base-jitter+seed%(2*jitter+1)) * time.Millisecond
}

// attest returns hash when sig is the attester's strict signature over it:
// 65 bytes r‖s‖v, v 27 or 28, s in the lower half of the curve order.
func attest(sig, hash []byte, attester common.Address) (common.Hash, error) {
	if len(sig) != 65 || len(hash) != 32 {
		return common.Hash{}, fmt.Errorf("%w: %d-byte signature over a %d-byte hash", ErrAttestation, len(sig), len(hash))
	}
	v := sig[64]
	if v != 27 && v != 28 {
		return common.Hash{}, fmt.Errorf("%w: v %d", ErrAttestation, v)
	}
	r, s := new(uint256.Int).SetBytes(sig[:32]), new(uint256.Int).SetBytes(sig[32:64])
	if !crypto.TransactionSignatureIsValid(v-27, r, s, false) {
		return common.Hash{}, fmt.Errorf("%w: not a canonical signature", ErrAttestation)
	}
	pub, err := crypto.SigToPub(hash, append(bytes.Clone(sig[:64]), v-27))
	if err != nil {
		return common.Hash{}, fmt.Errorf("%w: %w", ErrAttestation, err)
	}
	if signer := crypto.PubkeyToAddress(*pub); signer != attester {
		return common.Hash{}, fmt.Errorf("%w: signed by %x, not the attester %x", ErrAttestation, signer, attester)
	}
	return common.BytesToHash(hash), nil
}

// publicInputsHash is the hash the attester signs for b, whose one proof
// system has verification key vkey.
func publicInputsHash(b *protocol.ProofSystemBatch, vkey common.Hash) (common.Hash, error) {
	if b.BindMsgSenderInPublicInput {
		return common.Hash{}, errors.New("batch binds its sender, which is the poster's to set")
	}
	n := len(b.RollupIDsWithProofSystems)
	vkeys := make([][]common.Hash, n)
	for i, r := range b.RollupIDsWithProofSystems {
		vkeys[i] = slices.Repeat([]common.Hash{vkey}, len(r.ProofSystemIndexes))
	}
	p, err := protocol.NewPublicInputs(b, nil, make([][]byte, n), vkeys, common.Address{})
	if err != nil {
		return common.Hash{}, err
	}
	hashes, err := p.ProofSystemHashes(len(b.ProofSystems))
	if err != nil {
		return common.Hash{}, err
	}
	if len(hashes) != 1 {
		return common.Hash{}, fmt.Errorf("batch of %d proof systems, not one", len(hashes))
	}
	return hashes[0], nil
}
