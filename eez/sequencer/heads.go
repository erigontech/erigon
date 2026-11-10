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

package sequencer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

const (
	// blockReadRetry and blockReadDeadline bound the wait for the block of a
	// head, which can trail the head.
	blockReadRetry    = 50 * time.Millisecond
	blockReadDeadline = time.Second
)

// HeadBlocks reads the blocks of the heads to sign.
type HeadBlocks interface {
	// CanonicalBlock returns the canonical block with hash, nil when the
	// chain holds none or another block has replaced it.
	CanonicalBlock(ctx context.Context, hash common.Hash) (*types.Block, error)
}

// Publisher gossips signed heads.
type Publisher interface {
	Publish(ctx context.Context, e seqsig.Envelope) error
}

// SignerConfig configures a HeadSigner.
type SignerConfig struct {
	Key    *ecdsa.PrivateKey
	Domain seqsig.Domain
	// EpochMs is the signing process's start time in milliseconds; zero
	// takes the time the signer is handed its first head, once boot ends.
	EpochMs uint64
	Blocks  HeadBlocks
	// Publisher, when set, gossips the envelopes signed.
	Publisher Publisher
}

// HeadSigner signs every head the chain moves to, with its block attached,
// under the process's epoch and the next counter, keeps the latest envelope
// and hands it to the publisher. It signs the newest head it is handed: a
// head replaced before its block is readable is never signed. An envelope
// the publisher has not taken when the next is signed is dropped.
type HeadSigner struct {
	cfg     SignerConfig
	address common.Address
	logger  log.Logger
	epochMs atomic.Uint64
	// counter is the counter of the latest head signed.
	counter uint64

	mu sync.Mutex
	// pending is the newest head handed and not yet taken.
	pending *common.Hash
	wake    chan struct{}

	latest atomic.Pointer[seqsig.Envelope]
	// outbox is the envelope the publisher has not taken yet.
	outbox  atomic.Pointer[seqsig.Envelope]
	publish chan struct{}
	dropped atomic.Uint64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewHeadSigner returns the signer of cfg; Start runs it.
func NewHeadSigner(cfg SignerConfig, logger log.Logger) (*HeadSigner, error) {
	if cfg.Key == nil || cfg.Blocks == nil {
		return nil, errors.New("head signer: key and blocks are required")
	}
	s := &HeadSigner{
		cfg:     cfg,
		address: crypto.PubkeyToAddress(cfg.Key.PublicKey),
		logger:  logger,
		wake:    make(chan struct{}, 1),
		publish: make(chan struct{}, 1),
	}
	s.epochMs.Store(cfg.EpochMs)
	return s, nil
}

// Address returns the address of the signing key.
func (s *HeadSigner) Address() common.Address { return s.address }

// EpochMs returns the epoch the heads are signed under, zero before the
// first head.
func (s *HeadSigner) EpochMs() uint64 { return s.epochMs.Load() }

// Moved hands the signer head, the hash of the head the chain moved to,
// replacing a head not yet taken. It never blocks.
func (s *HeadSigner) Moved(head common.Hash) {
	s.mu.Lock()
	s.pending = &head
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Latest returns the envelope of the latest head signed, false before the
// first.
func (s *HeadSigner) Latest() (seqsig.Envelope, bool) {
	e := s.latest.Load()
	if e == nil {
		return seqsig.Envelope{}, false
	}
	return *e, true
}

// Start runs the signer in the background.
func (s *HeadSigner) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Go(func() { s.run(ctx) })
	if s.cfg.Publisher != nil {
		go s.publishLoop(ctx)
	}
	return nil
}

// Stop stops the signer and waits for it. A publication in flight is not
// waited for: the overlay, which stops after the signer, ends it.
func (s *HeadSigner) Stop() error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *HeadSigner) run(ctx context.Context) {
	var last common.Hash
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		head, ok := s.take()
		if !ok || head == last {
			continue
		}
		if s.epochMs.Load() == 0 {
			s.epochMs.Store(uint64(time.Now().UnixMilli()))
			s.logger.Info("[eez] signing heads", "signer", s.address, "epochMs", s.epochMs.Load())
		}
		b := s.block(ctx, head)
		if b == nil {
			loadMetrics().headsSkipped.Inc()
			continue
		}
		e, err := s.sign(b)
		if err != nil {
			loadMetrics().headsSkipped.Inc()
			s.logger.Error("[eez] signing the head failed", "event", "eez.node.head_signer.sign_failed", "number", b.NumberU64(), "hash", head, "err", err)
			continue
		}
		last = head
		s.latest.Store(&e)
		if s.cfg.Publisher != nil {
			s.queue(&e)
		}
		loadMetrics().headsSigned.Inc()
		s.logger.Debug("[eez] head signed", "event", "eez.node.head_signer.signed", "number", e.Head.Number, "hash", head,
			"epochMs", e.Head.EpochMs, "counter", e.Head.Counter)
	}
}

// queue hands e to the publisher, in place of an envelope it has not taken.
func (s *HeadSigner) queue(e *seqsig.Envelope) {
	if s.outbox.Swap(e) != nil {
		s.dropped.Add(1)
		loadMetrics().headsDropped.Inc()
	}
	select {
	case s.publish <- struct{}{}:
	default:
	}
}

// publishLoop publishes the envelopes queued. However long a publication
// takes, the signer goes on signing.
func (s *HeadSigner) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.publish:
		}
		e := s.outbox.Swap(nil)
		if e == nil || ctx.Err() != nil {
			continue
		}
		if err := s.cfg.Publisher.Publish(ctx, *e); err != nil && ctx.Err() == nil {
			s.logger.Warn("[eez] signed head not published", "event", "eez.p2p.publish.failed", "number", e.Head.Number,
				"counter", e.Head.Counter, "err", err)
		}
	}
}

// take returns the pending head and clears it.
func (s *HeadSigner) take() (common.Hash, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pending
	s.pending = nil
	if p == nil {
		return common.Hash{}, false
	}
	return *p, true
}

// replaced reports whether a newer head waits.
func (s *HeadSigner) replaced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending != nil
}

// block returns the block of head, nil when head is not to be signed: its
// read failed, a newer head replaced it before the block was readable, or the
// block stayed unreadable for blockReadDeadline.
func (s *HeadSigner) block(ctx context.Context, head common.Hash) *types.Block {
	deadline := time.Now().Add(blockReadDeadline)
	for {
		b, err := s.cfg.Blocks.CanonicalBlock(ctx, head)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				s.logger.Error("[eez] reading the head's block failed, not signing it", "event", "eez.node.head_signer.read_failed", "hash", head, "err", err)
			}
			return nil
		case b != nil:
			return b
		case s.replaced():
			s.logger.Info("[eez] head replaced before its block was readable, skipping it", "event", "eez.node.head_signer.superseded", "hash", head)
			return nil
		case !time.Now().Before(deadline):
			s.logger.Warn("[eez] head's block not readable, not signing it", "event", "eez.node.head_signer.block_unreadable", "hash", head)
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(blockReadRetry):
		}
	}
}

// sign returns the envelope of b under the next counter.
func (s *HeadSigner) sign(b *types.Block) (seqsig.Envelope, error) {
	enc, err := rlp.EncodeToBytes(b)
	if err != nil {
		return seqsig.Envelope{}, err
	}
	h := seqsig.Head{
		RollupID:   s.cfg.Domain.RollupID,
		Number:     b.NumberU64(),
		BlockHash:  b.Hash(),
		ParentHash: b.ParentHash(),
		EpochMs:    s.epochMs.Load(),
		Counter:    s.counter + 1,
	}
	sig, err := seqsig.Sign(h, s.cfg.Domain, s.cfg.Key)
	if err != nil {
		return seqsig.Envelope{}, err
	}
	s.counter++
	return seqsig.Envelope{Head: h, Signature: sig, Block: enc}, nil
}
