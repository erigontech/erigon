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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type stubVisibleEndTx struct {
	kv.TemporalTx
	viewID uint64
}

func (tx *stubVisibleEndTx) ViewID() uint64            { return tx.viewID }
func (tx *stubVisibleEndTx) Debug() kv.TemporalDebugTx { return stubVisibleEndDebug{viewID: tx.viewID} }

type stubVisibleEndDebug struct {
	kv.TemporalDebugTx
	viewID uint64
}

func (d stubVisibleEndDebug) DomainVisibleEnd(kv.Domain) (uint64, bool) {
	return d.viewID * 100, true
}

// Parallel-exec workers share one SharedDomains and one view, so the memo
// must tolerate concurrent gets interleaved with resets, and must re-derive
// after a sequential view rotation.
func TestDomainVisibleEndMemoConcurrent(t *testing.T) {
	t.Parallel()

	var memo domainVisibleEndMemo
	var wg sync.WaitGroup
	for range 8 {
		tx := &stubVisibleEndTx{viewID: 7}
		wg.Go(func() {
			for range 512 {
				for d := range kv.DomainLen {
					end, ok := memo.get(tx, d)
					if !ok || end != 700 {
						t.Errorf("domain %v: got (%d, %t)", d, end, ok)
						return
					}
				}
			}
		})
	}
	wg.Go(func() {
		for range 512 {
			memo.reset()
		}
	})
	wg.Wait()

	rotated := &stubVisibleEndTx{viewID: 8}
	end, ok := memo.get(rotated, kv.AccountsDomain)
	require.True(t, ok)
	require.Equal(t, uint64(800), end)
}
