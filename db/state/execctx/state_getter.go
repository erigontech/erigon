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
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/execution/cache"
)

// StateGetter provides execution-aware reads over SharedDomains.
type StateGetter struct {
	sd   *SharedDomains
	tx   kv.TemporalTx
	view cache.ReadView
	m    *kvmetrics.DomainMetrics
}

var _ execctxapi.StateGetter = (*StateGetter)(nil)

func (g *StateGetter) GetLatest(name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	return g.sd.getLatestMetered(name, g.tx, k, g.m, g.view)
}

func (g *StateGetter) GetLatestContext(ctx context.Context, name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	return g.sd.getLatestMetered(name, g.tx, k, kvmetrics.MetricsFromContext(ctx), g.view)
}

func (g *StateGetter) GetCode(addr []byte, txNum uint64) ([]byte, bool, error) {
	return g.sd.getCode(g.tx, g.view, addr, txNum)
}

func (g *StateGetter) GetCodeSize(addr []byte, txNum uint64) (int, bool, error) {
	return g.sd.getCodeSize(g.tx, g.view, addr, txNum)
}

func (g *StateGetter) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return g.sd.HasPrefix(name, prefix, g.tx)
}

func (g *StateGetter) StepsInFiles(entitySet ...kv.Domain) kv.Step {
	return g.tx.StepsInFiles(entitySet...)
}

// TemporalTxStateGetter adapts a temporal transaction using value-size reads.
type TemporalTxStateGetter struct {
	kv.TemporalTx
}

var _ execctxapi.StateGetter = (*TemporalTxStateGetter)(nil)

// NewTemporalTxStateGetter adapts a temporal transaction using value-size reads.
func NewTemporalTxStateGetter(tx kv.TemporalTx) *TemporalTxStateGetter {
	if tx == nil {
		return nil
	}
	return &TemporalTxStateGetter{TemporalTx: tx}
}

func (g *TemporalTxStateGetter) GetLatestContext(_ context.Context, name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	return g.GetLatest(name, k)
}

func (g *TemporalTxStateGetter) GetCode(addr []byte, _ uint64) ([]byte, bool, error) {
	code, _, err := g.GetLatest(kv.CodeDomain, addr)
	return code, len(code) > 0, err
}

func (g *TemporalTxStateGetter) GetCodeSize(addr []byte, _ uint64) (int, bool, error) {
	return g.GetLatestValSize(kv.CodeDomain, addr)
}
