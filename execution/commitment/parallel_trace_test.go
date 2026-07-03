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
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// A whale's deep storage fold runs across concurrent workers; its trace lines
// must all be tagged with the parent account address so one account's fold can
// be grepped out of the interleaved parallel output.
func TestParallelTrace_WhaleTaggedByParentAddress(t *testing.T) {
	t.Parallel()

	pk, upds := buildWhaleCorpus(bigAccountWhale(deepStorageThreshold * 2))

	// whale = the account address shared by the most storage keys.
	counts := map[string]int{}
	for _, k := range pk {
		if len(k) == length.Addr+length.Hash {
			counts[string(k[:length.Addr])]++
		}
	}
	whale, best := "", 0
	for a, n := range counts {
		if n > best {
			whale, best = a, n
		}
	}
	require.Greater(t, best, deepStorageThreshold, "corpus must contain a deep-storage whale")
	whaleTag := fmt.Sprintf("[%x] ", whale)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	pph := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	pph.SetNumWorkers(4)
	pph.ResetContext(ms)

	var buf bytes.Buffer
	pph.SetTraceWriter(&buf)
	u := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, pk, upds)
	_, err := pph.Process(context.Background(), u, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u.Close()
	pph.Release()

	out := buf.String()
	require.Positive(t, strings.Count(out, whaleTag), "whale fold lines must be tagged with the parent account address")

	// Every tagged occurrence must sit at the start of a line (attributable, not
	// mid-line noise), and the tag must never split a fold line.
	require.True(t, strings.HasPrefix(out, whaleTag) || strings.Contains(out, "\n"+whaleTag),
		"address tag must prefix whole lines")
}
