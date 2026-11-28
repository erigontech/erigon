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

package commitment

import (
	"context"
	"testing"
)

func forceDirectSpill(ut *Updates) { ut.directMemLimit = 0 }

func forEachMode(t *testing.T, fn func(t *testing.T, mode Mode)) {
	t.Helper()
	for _, mode := range []Mode{ModeDirect, ModeUpdate} {
		name := "ModeDirect"
		if mode == ModeUpdate {
			name = "ModeUpdate"
		}
		t.Run(name, func(t *testing.T) { fn(t, mode) })
	}
}

func testWarmuper(ctx context.Context, factory TrieContextFactory, workers int) *Warmuper {
	return NewWarmuper(ctx, WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
		NumWorkers: workers,
		MaxDepth:   64,
		LogPrefix:  "test",
	})
}

func nibs(vals ...byte) []byte {
	out := make([]byte, len(vals))
	copy(out, vals)
	return out
}

func keyHasherNoop(key []byte) []byte { return key }
