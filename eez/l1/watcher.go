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
package l1

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrReorgTooDeep reports a reorg whose common ancestor is beyond the
	// watcher's depth.
	ErrReorgTooDeep = errors.New("L1 reorg deeper than the configured maximum")
	// ErrFinalityViolation reports a finalized block that changed or moved
	// back.
	ErrFinalityViolation = errors.New("L1 finality violated")
)

// DefaultReorgDepth is the reference client's bound, Ethereum's finality
// window.
const DefaultReorgDepth = 62

// Event is something the watcher observed on L1.
type Event interface{ isEvent() }

// NewHead reports that the canonical chain now ends at Head; every block from
// the previous head up to it is canonical.
type NewHead struct{ Head Header }

// Reorg reports that the canonical blocks above Ancestor were replaced.
type Reorg struct {
	Ancestor Header
	OldHead  Header
}

// Finalized reports a new finalized block.
type Finalized struct{ Head Header }

func (NewHead) isEvent()   {}
func (Reorg) isEvent()     {}
func (Finalized) isEvent() {}

// Watcher follows the L1 head and finality through a ring of the latest
// canonical (number, hash) pairs, and reports reorgs with their common
// ancestor. A reorg below the ring, or a finalized block that changes, is
// terminal.
type Watcher struct {
	reader    ChainReader
	depth     int
	ring      []Header
	finalized *Header
}

// NewWatcher returns a watcher that tolerates reorgs up to depth blocks.
func NewWatcher(reader ChainReader, depth int) *Watcher {
	return &Watcher{reader: reader, depth: max(depth, 1)}
}

// Seed starts the watcher at the canonical block number and its ancestors.
func (w *Watcher) Seed(ctx context.Context, number uint64) error {
	h, err := w.reader.HeaderByNumber(ctx, number)
	if err != nil {
		return err
	}
	window, err := w.ancestors(ctx, *h)
	if err != nil {
		return err
	}
	w.ring = window
	return nil
}

// Tip returns the last canonical block the watcher reported.
func (w *Watcher) Tip() Header { return w.ring[len(w.ring)-1] }

// ancestors returns head and up to depth-1 of its ancestors, oldest first.
// The window ends early at an ancestor the source does not serve, as a
// recording's first block.
func (w *Watcher) ancestors(ctx context.Context, head Header) ([]Header, error) {
	window := []Header{head}
	for len(window) < w.depth && window[0].Number > 0 {
		parent, err := w.reader.HeaderByHash(ctx, window[0].ParentHash)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		window = append([]Header{*parent}, window...)
	}
	return window, nil
}

func (w *Watcher) inRing(h Header) bool {
	for _, r := range w.ring {
		if r.Hash == h.Hash {
			return true
		}
	}
	return false
}

func (w *Watcher) push(hs ...Header) {
	w.ring = append(w.ring, hs...)
	if len(w.ring) > w.depth {
		w.ring = w.ring[len(w.ring)-w.depth:]
	}
}

func (w *Watcher) rewind(ancestor uint64) {
	for len(w.ring) > 0 && w.Tip().Number > ancestor {
		w.ring = w.ring[:len(w.ring)-1]
	}
}

// Poll reads the L1 head once and returns what changed since the last poll.
// On error nothing is committed and the next poll retries.
func (w *Watcher) Poll(ctx context.Context) ([]Event, error) {
	latest, err := w.reader.Head(ctx)
	if err != nil {
		return nil, err
	}
	tip := w.Tip()
	switch {
	case latest.Hash == tip.Hash:
		return nil, nil
	case latest.ParentHash == tip.Hash && latest.Number == tip.Number+1:
		w.push(*latest)
		return []Event{NewHead{Head: *latest}}, nil
	case latest.Number > tip.Number+uint64(w.depth):
		return w.jump(ctx, tip, *latest)
	}

	// Walk back from the head until a block the ring holds.
	walked := []Header{*latest}
	for !w.inRing(walked[0]) {
		if len(walked) > w.depth || walked[0].Number == 0 {
			return nil, fmt.Errorf("%w: no common ancestor within %d blocks of %d", ErrReorgTooDeep, w.depth, latest.Number)
		}
		parent, err := w.reader.HeaderByHash(ctx, walked[0].ParentHash)
		if err != nil {
			return nil, err
		}
		walked = append([]Header{*parent}, walked...)
	}
	ancestor := walked[0]
	var events []Event
	if ancestor.Hash != tip.Hash {
		events = append(events, Reorg{Ancestor: ancestor, OldHead: tip})
		w.rewind(ancestor.Number)
	}
	w.push(walked[1:]...)
	return append(events, NewHead{Head: *latest}), nil
}

// jump covers a gap wider than the ring: the old tip is checked by height,
// and a replaced tip is resolved against the ring by height too.
func (w *Watcher) jump(ctx context.Context, tip, latest Header) ([]Event, error) {
	var events []Event
	atTip, err := w.reader.HeaderByNumber(ctx, tip.Number)
	if err != nil {
		return nil, err
	}
	if atTip.Hash != tip.Hash {
		var ancestor *Header
		for i := len(w.ring) - 1; i >= 0 && ancestor == nil; i-- {
			canonical, err := w.reader.HeaderByNumber(ctx, w.ring[i].Number)
			if err != nil {
				return nil, err
			}
			if canonical.Hash == w.ring[i].Hash {
				ancestor = &w.ring[i]
			}
		}
		if ancestor == nil {
			return nil, fmt.Errorf("%w: every block of the ring below %d was replaced", ErrReorgTooDeep, tip.Number)
		}
		events = append(events, Reorg{Ancestor: *ancestor, OldHead: tip})
	}
	window, err := w.ancestors(ctx, latest)
	if err != nil {
		return nil, err
	}
	w.ring = window
	return append(events, NewHead{Head: latest}), nil
}

// PollFinalized reads the finalized block and reports it when it advanced.
func (w *Watcher) PollFinalized(ctx context.Context) ([]Event, error) {
	f, err := w.reader.Finalized(ctx)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if prev := w.finalized; prev != nil {
		switch {
		case f.Hash == prev.Hash:
			return nil, nil
		case f.Number <= prev.Number:
			return nil, fmt.Errorf("%w: finalized block %d (%x) after %d (%x)", ErrFinalityViolation, f.Number, f.Hash, prev.Number, prev.Hash)
		}
	}
	w.finalized = f
	return []Event{Finalized{Head: *f}}, nil
}

// Run polls every interval, and whenever heads delivers, and sends the
// events to out, reading finality every finalityEvery polls, until ctx ends
// or a terminal error occurs. Transient read errors are retried on the next
// poll. heads may be nil.
func (w *Watcher) Run(ctx context.Context, interval time.Duration, finalityEvery int, heads <-chan struct{}, out chan<- Event) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for n := 0; ; n++ {
		events, err := w.Poll(ctx)
		if err == nil && finalityEvery > 0 && n%finalityEvery == 0 {
			var fin []Event
			fin, err = w.PollFinalized(ctx)
			events = append(events, fin...)
		}
		if errors.Is(err, ErrReorgTooDeep) || errors.Is(err, ErrFinalityViolation) {
			return err
		}
		for _, e := range events {
			select {
			case out <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case <-ticker.C:
		case _, ok := <-heads:
			if !ok {
				heads = nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
