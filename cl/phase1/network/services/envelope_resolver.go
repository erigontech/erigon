package services

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	maxEnvelopeResolverJobs        = 128
	maxConcurrentEnvelopeResolvers = 4
	envelopeResolverDeadline       = 12 * time.Second
	envelopeResolverRetryInterval  = 500 * time.Millisecond
)

type executionPayloadEnvelopeRequester interface {
	SendExecutionPayloadEnvelopesByRootReq(context.Context, [][32]byte) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error)
	BanPeer(string)
}

type executionPayloadEnvelopeResolver interface {
	ResolveExecutionPayloadEnvelope(common.Hash)
	HasPendingExecutionPayloadEnvelope(common.Hash) bool
}

type envelopeResolver struct {
	ctx       context.Context
	requester executionPayloadEnvelopeRequester
	processor *executionPayloadService
	sem       chan struct{}
	mu        sync.Mutex
	jobs      map[common.Hash]struct{}
	deadline  time.Duration
	retry     time.Duration
}

func newEnvelopeResolver(ctx context.Context, requester executionPayloadEnvelopeRequester, processor *executionPayloadService) *envelopeResolver {
	if requester == nil {
		return nil
	}
	return &envelopeResolver{
		ctx:       ctx,
		requester: requester,
		processor: processor,
		sem:       make(chan struct{}, maxConcurrentEnvelopeResolvers),
		jobs:      make(map[common.Hash]struct{}),
		deadline:  envelopeResolverDeadline,
		retry:     envelopeResolverRetryInterval,
	}
}

func (r *envelopeResolver) ResolveExecutionPayloadEnvelope(root common.Hash) {
	r.mu.Lock()
	if len(r.jobs) >= maxEnvelopeResolverJobs {
		r.mu.Unlock()
		return
	}
	if _, exists := r.jobs[root]; exists {
		r.mu.Unlock()
		return
	}
	r.jobs[root] = struct{}{}
	r.mu.Unlock()

	go r.resolve(root)
}

func (r *envelopeResolver) resolve(root common.Hash) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Error("Execution payload envelope resolver recovered from panic", "err", recovered)
		}
		r.mu.Lock()
		delete(r.jobs, root)
		r.mu.Unlock()
	}()

	select {
	case r.sem <- struct{}{}:
		defer func() { <-r.sem }()
	case <-r.ctx.Done():
		return
	}

	ctx, cancel := context.WithTimeout(r.ctx, r.deadline)
	defer cancel()
	ticker := time.NewTicker(r.retry)
	defer ticker.Stop()

	for {
		if r.processor.forkchoiceStore.HasEnvelope(root) {
			return
		}
		if r.processor.hasPendingEnvelopeRoot(root) {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
		envelopes, pid, err := r.requester.SendExecutionPayloadEnvelopesByRootReq(ctx, [][32]byte{root})
		if err == nil && r.processResponses(ctx, root, pid, envelopes) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *envelopeResolver) processResponses(ctx context.Context, root common.Hash, pid string, envelopes []*cltypes.SignedExecutionPayloadEnvelope) bool {
	for _, envelope := range envelopes {
		if envelope == nil || envelope.Message == nil || envelope.Message.BeaconBlockRoot != root {
			r.requester.BanPeer(pid)
			continue
		}
		err := r.processor.ProcessMessage(ctx, nil, envelope)
		if err == nil || r.processor.forkchoiceStore.HasEnvelope(root) {
			return true
		}
		if errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) || errors.Is(err, forkchoice.ErrELPayloadValidationUnavailable) {
			return false
		}
		if !errors.Is(err, ErrIgnore) {
			r.requester.BanPeer(pid)
		}
	}
	return false
}
