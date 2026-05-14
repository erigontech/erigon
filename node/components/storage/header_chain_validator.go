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

package storage

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// HeaderChainValidator verifies header[N].ParentHash == hash(header[N-1])
// across each retired headers.seg.
type HeaderChainValidator struct {
	DB          kv.RoDB
	BlockReader services.FullBlockReader
}

// Name implements validation.StepValidator.
func (HeaderChainValidator) Name() string { return "header_chain_continuity" }

// ValidateStep implements validation.StepValidator.
func (v HeaderChainValidator) ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 {
		return nil
	}
	// Only block-domain headers.seg files participate. Defer DB/BR
	// dependency check until we know we have something to do.
	type headerSeg struct{ from, to uint64 }
	var segs []headerSeg
	for _, f := range files {
		if f.Domain != "" {
			continue
		}
		typ, from, to, ok := snaptype.ParseRange(f.Name)
		if !ok || typ != statecfg.Headers {
			continue
		}
		segs = append(segs, headerSeg{from, to})
	}
	if len(segs) == 0 {
		return nil
	}
	if v.DB == nil {
		return fmt.Errorf("HeaderChainValidator: nil DB")
	}
	if v.BlockReader == nil {
		return fmt.Errorf("HeaderChainValidator: nil BlockReader")
	}

	tx, err := v.DB.BeginRo(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	for _, seg := range segs {
		if err := verifyHeaderChain(ctx, tx, v.BlockReader, seg.from, seg.to); err != nil {
			return fmt.Errorf("headers.seg [%d, %d): %w", seg.from, seg.to, err)
		}
	}
	return nil
}

// verifyHeaderChain walks blocks [from, to) and checks
// header[N].ParentHash == hash(header[N-1]). Block 0 (genesis) has
// no parent so a segment starting at 0 begins comparison at block 1.
func verifyHeaderChain(ctx context.Context, tx kv.Getter, br services.FullBlockReader, from, to uint64) error {
	if to <= from {
		return fmt.Errorf("invalid range [%d, %d)", from, to)
	}
	start := from
	if start == 0 {
		start = 1
	}
	prev, err := br.HeaderByNumber(ctx, tx, start-1)
	if err != nil {
		return fmt.Errorf("HeaderByNumber(%d): %w", start-1, err)
	}
	if prev == nil {
		// Block not yet imported into the chain DB. Transient at
		// fresh-bootstrap: the headers.seg is on disk but the EL
		// hasn't OpenSegments'd yet, so BlockReader can't see it.
		// Return ErrPause so the lifecycle driver retries on the
		// next sweep without ticking the per-file failure counter
		// — otherwise validation fails 5× before OpenSegments
		// finally fires and the file is quarantined permanently.
		return fmt.Errorf("missing header at block %d: %w", start-1, validation.ErrPause)
	}
	for n := start; n < to; n++ {
		h, err := br.HeaderByNumber(ctx, tx, n)
		if err != nil {
			return fmt.Errorf("HeaderByNumber(%d): %w", n, err)
		}
		if h == nil {
			return fmt.Errorf("missing header at block %d: %w", n, validation.ErrPause)
		}
		if h.ParentHash != prev.Hash() {
			return fmt.Errorf("parent-hash chain broken at block %d: header.parentHash=%x, hash(header[%d])=%x",
				n, h.ParentHash, n-1, prev.Hash())
		}
		prev = h
	}
	return nil
}
