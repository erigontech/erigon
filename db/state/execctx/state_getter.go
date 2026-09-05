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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/cache"
)

// maxLentCodeBuf bounds what codeBuf keeps between uses, so one huge value does
// not pin a large buffer for the getter's lifetime.
const maxLentCodeBuf = 128 * 1024

type stateGetter struct {
	sd   *SharedDomains
	tx   kv.TemporalTx
	view cache.ReadView
	m    kv.GetLatestMetrics
	// codeBuf is lent to the code read and never escapes: the value handed back
	// is the cache's copy. One getter serves one worker, like m.
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
	// Grow for next time by allocating: code belongs to the cache, not to us.
	g.codeBuf = slices.Grow(g.codeBuf[:0], min(len(code), maxLentCodeBuf))
	return code, ok, err
}

func (g *stateGetter) GetCodeSize(addr []byte, txNum uint64) (int, bool, error) {
	return g.sd.getCodeSize(g.tx, g.view, addr, txNum)
}

func (g *stateGetter) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return g.sd.HasPrefix(name, prefix, g.tx)
}

func (g *stateGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return g.tx.StepsInFiles(entitySet...)
}

// TemporalTxStateGetter exposes execution reads over a temporal transaction.
type TemporalTxStateGetter struct {
	kv.TemporalTx
}

var _ execctxapi.StateGetter = (*TemporalTxStateGetter)(nil)

// NewTemporalTxStateGetter wraps tx with execution-read methods.
func NewTemporalTxStateGetter(tx kv.TemporalTx) *TemporalTxStateGetter {
	return &TemporalTxStateGetter{TemporalTx: tx}
}

func (g *TemporalTxStateGetter) GetLatest(name kv.Domain, k []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	return g.TemporalTx.GetLatest(name, k, opts)
}

func (g *TemporalTxStateGetter) GetCode(addr []byte, _ uint64) ([]byte, bool, error) {
	code, _, err := g.GetLatest(kv.CodeDomain, addr, kv.GetLatestOptions{})
	return code, len(code) > 0, err
}

func (g *TemporalTxStateGetter) GetCodeSize(addr []byte, _ uint64) (int, bool, error) {
	size, found, err := g.GetLatestValSize(kv.CodeDomain, addr)
	return size, found && size > 0, err
}
