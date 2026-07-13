// Copyright 2024 The Erigon Authors
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

package execmoduletester

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

// OverlayDB returns a read-only DB whose transactions route reads through the
// latest published SharedDomains: consensus-table reads see the block overlay
// and domain-state reads see the in-flight (not-yet-committed) values. Pass it
// to RPC-daemon APIs so their reads observe the tip under background commit
// instead of a lagging raw DB.
func (emt *ExecModuleTester) OverlayDB() kv.TemporalRoDB {
	return &sdRoDB{TemporalRoDB: emt.DB, publishedSD: emt.PublishedSD}
}

type sdRoDB struct {
	kv.TemporalRoDB
	publishedSD func() *execctx.SharedDomains
}

func (d *sdRoDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	base, err := d.TemporalRoDB.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	return wrapSDTx(base, d.publishedSD()), nil
}

func (d *sdRoDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	return d.BeginTemporalRo(ctx)
}

func (d *sdRoDB) ViewTemporal(ctx context.Context, f func(tx kv.TemporalTx) error) error {
	tx, err := d.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

func (d *sdRoDB) View(ctx context.Context, f func(tx kv.Tx) error) error {
	tx, err := d.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

// wrapSDTx layers domain- and table-level read-through onto base for the given
// published SD. With no published SD it returns base unchanged.
func wrapSDTx(base kv.TemporalTx, sd *execctx.SharedDomains) kv.TemporalTx {
	if sd == nil {
		return base
	}
	read := base
	if v := sd.BlockOverlayTemporalTx(base); v != nil {
		read = v
	}
	return &sdRoTx{TemporalTx: read, base: base, sd: sd}
}

// sdRoTx serves consensus-table reads from the block overlay (the embedded
// TemporalTx) and latest domain-state reads from the published SD's in-flight
// chain. Historical (GetAsOf) reads try the in-flight mem first, then fall back
// to committed state on the overlay tx.
type sdRoTx struct {
	kv.TemporalTx
	base kv.TemporalTx
	sd   *execctx.SharedDomains
}

func (t *sdRoTx) GetLatest(name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	return t.sd.GetLatest(name, t.base, k)
}

func (t *sdRoTx) HasPrefix(name kv.Domain, prefix []byte) ([]byte, []byte, bool, error) {
	return t.sd.HasPrefix(name, prefix, t.base)
}

func (t *sdRoTx) GetAsOf(name kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	if v, ok, err := t.sd.GetAsOf(name, k, ts); err != nil || ok {
		return v, ok, err
	}
	return t.TemporalTx.GetAsOf(name, k, ts)
}

func (t *sdRoTx) Rollback() {
	t.base.Rollback()
}
