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

package commitmentdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/execution/commitment"
)

type pbinStateStubSD struct{}

func (s *pbinStateStubSD) SetTxNum(uint64)                                         {}
func (s *pbinStateStubSD) AsGetter(kv.TemporalTx) kv.TemporalGetter                { return nil }
func (s *pbinStateStubSD) AsPutDel(kv.TemporalTx) kv.TemporalPutDel                { return nil }
func (s *pbinStateStubSD) MergeMetrics(kvmetrics.Source, *kvmetrics.DomainMetrics) {}
func (s *pbinStateStubSD) StepSize() uint64                                        { return 1 }
func (s *pbinStateStubSD) Metrics() *kvmetrics.DomainMetrics                       { return nil }
func (s *pbinStateStubSD) HasSharedBranchCache() bool                              { return false }

func pbinStateTestCtx(t *testing.T, variant commitment.TrieVariant) *SharedDomainsCommitmentContext {
	t.Helper()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = variant
	sdc := NewSharedDomainsCommitmentContext(&pbinStateStubSD{}, commitment.ModeDirect, t.TempDir(), cfg)
	t.Cleanup(sdc.Close)
	return sdc
}

// TestPBinCommitmentStateHeaderMatchesHex pins the commitment-state record
// layout across variants: the 16-byte txNum‖blockNum header is read raw and
// variant-blind (DecodeTxBlockNums, LatestBlockNumWithCommitment), so the bin
// variant must produce it byte-identically to hex.
func TestPBinCommitmentStateHeaderMatchesHex(t *testing.T) {
	t.Parallel()

	const blockNum, txNum = uint64(41), uint64(4321)

	hexCtx := pbinStateTestCtx(t, commitment.VariantHexPatriciaTrie)
	hexState, err := hexCtx.encodeCommitmentState(blockNum, txNum)
	require.NoError(t, err)

	binCtx := pbinStateTestCtx(t, commitment.VariantBinPatriciaTrie)
	require.Equal(t, commitment.VariantBinPatriciaTrie, binCtx.variant)
	binState, err := binCtx.encodeCommitmentState(blockNum, txNum)
	require.NoError(t, err)

	require.Equal(t, hexState[:16], binState[:16], "the txNum‖blockNum header must stay byte-identical across variants")

	gotTx, gotBlock := DecodeTxBlockNums(binState)
	require.Equal(t, txNum, gotTx)
	require.Equal(t, blockNum, gotBlock)

	restoredBlock, restoredTx, err := binCtx.restorePatriciaState(binState)
	require.NoError(t, err)
	require.Equal(t, blockNum, restoredBlock)
	require.Equal(t, txNum, restoredTx)
}
