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

package rpchelper

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
)

// PinnedRoTx pins a read tx to the overlay resolution made at acquisition:
// reads go through the pinned view (or the raw tx when no overlay was
// published), and Rollback releases the raw tx — a view does not own the
// underlying resources, so this is the one handle callers need.
type PinnedRoTx struct {
	kv.TemporalTx // the pinned read view, or the raw tx when overlay is nil
	raw           kv.TemporalTx
	overlay       *membatchwithdb.MemoryMutation
}

// PinToOverlay pins tx to the given overlay for the rest of the request: a
// read view when overlay is non-nil, an explicit no-overlay pin otherwise, so
// an overlay published mid-request cannot leak in through downstream wrap
// points. A tx already carrying a pinned view is returned unchanged.
func PinToOverlay(tx kv.TemporalTx, overlay *membatchwithdb.MemoryMutation) kv.TemporalTx {
	if membatchwithdb.CarriesOverlayView(tx) {
		return tx
	}
	view := tx
	if overlay != nil {
		view = overlay.NewReadView(tx)
	}
	return &PinnedRoTx{TemporalTx: view, raw: tx, overlay: overlay}
}

func (t *PinnedRoTx) Rollback() {
	t.raw.Rollback()
}

// OverlayView implements membatchwithdb.OverlayViewCarrier.
func (t *PinnedRoTx) OverlayView() (*membatchwithdb.MemoryMutation, bool) {
	return t.overlay, true
}

// BlockFilesRoTx keeps the tx-scoped block-files view across the pin, so a
// request cannot straddle a snapshot merge.
func (t *PinnedRoTx) BlockFilesRoTx() *blocksnapshots.View {
	if p, ok := t.TemporalTx.(membatchwithdb.HasBlockFilesRoTx); ok {
		return p.BlockFilesRoTx()
	}
	return nil
}

// Apply goes through the raw tx's guard (closed-tx checks) but hands the
// pinned handle to the callback, not the raw tx.
func (t *PinnedRoTx) Apply(ctx context.Context, f func(tx kv.Tx) error) error {
	return t.raw.Apply(ctx, func(kv.Tx) error { return f(t) })
}

// FreezeInfo delegates to the raw tx: the overlay read view in between does
// not support it.
func (t *PinnedRoTx) FreezeInfo() kv.FreezeInfo {
	return t.raw.FreezeInfo()
}

// UnderlyingTx exposes the fallback tx like the wrapped view would.
func (t *PinnedRoTx) UnderlyingTx() kv.TemporalTx {
	if p, ok := t.TemporalTx.(interface{ UnderlyingTx() kv.TemporalTx }); ok {
		return p.UnderlyingTx()
	}
	return t.raw
}

// Pin forwards the files-pin capability of the wrapped view (or the raw tx).
func (t *PinnedRoTx) Pin() kv.TemporalFilesPin {
	if p, ok := t.TemporalTx.(interface{ Pin() kv.TemporalFilesPin }); ok {
		return p.Pin()
	}
	return nil
}
