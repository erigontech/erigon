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

package execctx

import (
	"slices"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/cache"
)

const maxLentCodeBuf = 256 * 1024

type stateGetter struct {
	sd      *SharedDomains
	tx      kv.TemporalTx
	view    cache.ReadView
	m       kv.GetLatestMetrics
	codeBuf []byte
}

var _ execctxapi.StateGetter = (*stateGetter)(nil)

// GetLatest never writes to a process-wide metrics accumulator shared by concurrent readers.
func (g *stateGetter) GetLatest(name kv.Domain, k []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	metrics, start := opts.Metrics()
	if metrics == nil {
		metrics = g.m
	}
	return g.sd.getLatest(name, g.tx, k, metrics, start, opts.MaxStep(), g.view, getLatestOptions{})
}

func (g *stateGetter) GetCode(addr []byte, txNum uint64) ([]byte, bool, error) {
	code, ok, err := g.sd.getCode(g.tx, g.view, addr, txNum, g.codeBuf)
	g.codeBuf = slices.Grow(g.codeBuf[:0], min(len(code), maxLentCodeBuf))
	return code, ok, err
}

func (g *stateGetter) GetCodeSize(addr []byte, txNum uint64) (int, bool, error) {
	return g.sd.getCodeSize(g.tx, g.view, addr, txNum)
}

func (g *stateGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return g.tx.StepsInFiles(entitySet...)
}

// TemporalTxStateGetter exposes execution reads over a temporal transaction.
type TemporalTxStateGetter struct {
	kv.TemporalTx
	stateCache *cache.StateCache
	view       cache.ReadView
	stepSize   uint64
}

var _ execctxapi.StateGetter = (*TemporalTxStateGetter)(nil)

// NewTemporalTxStateGetter wraps tx with execution-read methods.
func NewTemporalTxStateGetter(tx kv.TemporalTx) *TemporalTxStateGetter {
	return &TemporalTxStateGetter{TemporalTx: tx}
}

// NewCachedTemporalTxStateGetter reads through the shared state cache. It is the
// getter for readers with no SharedDomains, which would otherwise take every
// account, storage and code read to the domain.
func NewCachedTemporalTxStateGetter(tx kv.TemporalTx, stateCache *cache.StateCache) *TemporalTxStateGetter {
	g := &TemporalTxStateGetter{TemporalTx: tx}
	if stateCache == nil || !dbg.UseStateCache {
		return g
	}
	// A writable tx's visible end comes from the SharedDomains flush memo, not
	// from the tx, so only read-only txs can vouch for a fill here.
	if _, writable := tx.(kv.TemporalRwTx); writable {
		return g
	}
	frontier := txCacheFrontier(tx)
	if frontier == nil {
		return g
	}
	g.stateCache = stateCache
	g.view = stateCache.View(frontier)
	g.stepSize = tx.Debug().StepSize()
	return g
}

// txCacheFrontier binds fill authority to the transaction's durable state
// version, the same contract SharedDomains readers get.
func txCacheFrontier(tx kv.TemporalTx) cache.Frontier {
	generationTx := cacheGenerationTx(tx)
	if generationTx == nil {
		return nil
	}
	stateVersion, err := rawdb.GetStateVersion(generationTx)
	if err != nil {
		return nil
	}
	return cache.FrontierWithStateVersion(cache.FrontierFunc(tx.Debug().DomainVisibleEnd), stateVersion)
}

func (g *TemporalTxStateGetter) GetLatest(name kv.Domain, k []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	// A bounded read observes a staged unwind, so it neither hits nor fills.
	if g.stateCache == nil || opts.MaxStep() != kv.NoStepBound {
		return g.TemporalTx.GetLatest(name, k, opts)
	}
	if v, txNum, ok := g.view.GetWithTxNum(name, k); ok {
		return v, kv.Step(txNum / g.stepSize), nil
	}
	v, step, err := g.TemporalTx.GetLatest(name, k, opts)
	if err == nil && g.stateCache.Caches(name) {
		g.view.Fill(name, k, v, step.LastTxNum(g.stepSize))
	}
	return v, step, err
}

func (g *TemporalTxStateGetter) GetCode(addr []byte, _ uint64) ([]byte, bool, error) {
	code, _, err := g.GetLatest(kv.CodeDomain, addr, kv.GetLatestOptions{})
	return code, len(code) > 0, err
}

func (g *TemporalTxStateGetter) GetCodeSize(addr []byte, _ uint64) (int, bool, error) {
	if g.stateCache != nil {
		if code, ok := g.view.Get(kv.CodeDomain, addr); ok {
			return len(code), len(code) > 0, nil
		}
	}
	size, found, err := g.GetLatestValSize(kv.CodeDomain, addr)
	return size, found && size > 0, err
}
