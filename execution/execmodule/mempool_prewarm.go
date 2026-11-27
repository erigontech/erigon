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

package execmodule

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/types"
)

type mempoolPrewarm struct {
	provide    exec.MempoolTransactions
	mu         sync.Mutex
	active     int
	generation uint64
	cancel     context.CancelFunc
	done       chan struct{}
}

func (e *ExecModule) SetMempoolPrewarm(provide exec.MempoolTransactions) {
	if !dbg.MempoolPrewarm || e.readAheader == nil || provide == nil {
		return
	}
	e.mempoolPrewarm = &mempoolPrewarm{provide: provide}
	if build := e.builderFunc; build != nil {
		e.builderFunc = func(ctx context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			end := e.BeginPayload()
			defer end()
			return build(ctx, params, interrupt)
		}
	}
}

func (p *mempoolPrewarm) stopLocked() {
	if p.cancel != nil {
		p.cancel()
		<-p.done
		p.cancel, p.done = nil, nil
	}
}

func (e *ExecModule) BeginPayload() func() {
	p := e.mempoolPrewarm
	if p == nil {
		return nil
	}
	p.mu.Lock()
	p.active++
	p.generation++
	p.stopLocked()
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	}
}

func (e *ExecModule) stopMempoolPrewarm() {
	if p := e.mempoolPrewarm; p != nil {
		p.mu.Lock()
		p.stopLocked()
		p.mu.Unlock()
	}
}

func (e *ExecModule) prepareMempoolPrewarm() func(*types.Header) {
	p := e.mempoolPrewarm
	if p == nil {
		return nil
	}
	p.mu.Lock()
	generation := p.generation
	p.mu.Unlock()
	return func(head *types.Header) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.active != 0 || p.generation != generation || e.backgroundCtx.Err() != nil {
			return
		}
		ctx, cancel := context.WithCancel(e.backgroundCtx)
		done := make(chan struct{})
		p.cancel, p.done = cancel, done
		go func() {
			defer close(done)
			defer cancel()
			e.readAheader.PrewarmMempool(ctx, e.db, e.blockReader, e.config, e.engine, head, p.provide, e.logger)
		}()
	}
}
