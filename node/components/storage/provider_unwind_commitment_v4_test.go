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
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// collectBranches feeds the given (key, value) pairs into a fresh
// etl.Collector — mirrors what commitmentdb.RecomputeAtTxNumWithoutSD's
// TrieContext.PutBranch does during production compute.
func collectBranches(t *testing.T, dir string, entries [][2][]byte) *etl.Collector {
	t.Helper()
	//nolint:gocritic // factory helper; caller owns the returned collector and Close's it.
	c := etl.NewCollectorWithAllocator("test-branches", dir, etl.SmallSortableBuffers, log.New())
	for _, e := range entries {
		require.NoError(t, c.Collect(e[0], e[1]))
	}
	return c
}

// TestWriteCommitmentBoundaryFileV4_EmitsBranchesThenAnchor verifies
// the v4 file layout: all branch (k, v) pairs in sorted order first,
// then (KeyCommitmentState, anchor) at the end. Every branch key is a
// hex-nibble prefix (bytes 0x00-0x0F) so all sort before "state"
// (0x73), making the anchor's tail position deterministic.
func TestWriteCommitmentBoundaryFileV4_EmitsBranchesThenAnchor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Deliberately unordered input — etl.Collector.Load sorts on Load.
	branches := collectBranches(t, dir, [][2][]byte{
		{{0x02, 0x03}, []byte("branch-B")},
		{{0x00, 0x01}, []byte("branch-A")},
		{{0x04, 0x05}, []byte("branch-C")},
	})
	defer branches.Close()

	anchor := []byte("encoded-commitment-state-blob")
	outPath := filepath.Join(dir, "v4.0-commitment.0-100.kv")

	require.NoError(t, WriteCommitmentBoundaryFileV4(
		ctx, branches, anchor, outPath, dir, seg.CompressNone, log.New(),
	))

	got := readKV(t, outPath)
	require.Len(t, got, 4, "expected 3 branches + 1 anchor entry")

	require.Equal(t, []byte{0x00, 0x01}, got[0][0])
	require.Equal(t, []byte("branch-A"), got[0][1])
	require.Equal(t, []byte{0x02, 0x03}, got[1][0])
	require.Equal(t, []byte("branch-B"), got[1][1])
	require.Equal(t, []byte{0x04, 0x05}, got[2][0])
	require.Equal(t, []byte("branch-C"), got[2][1])
	require.Equal(t, commitmentdb.KeyCommitmentState, got[3][0])
	require.Equal(t, anchor, got[3][1])
}

// TestWriteCommitmentBoundaryFileV4_EmptyBranches verifies that a
// compute yielding zero branches still produces a valid v4 file
// containing only the anchor. Zero-branch scenarios don't normally
// happen post-genesis but the primitive must not corrupt the file in
// that edge case — the anchor alone is enough for SD.SeekCommitment.
func TestWriteCommitmentBoundaryFileV4_EmptyBranches(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	branches := etl.NewCollectorWithAllocator("test-empty", dir, etl.SmallSortableBuffers, log.New())
	defer branches.Close()

	anchor := []byte("anchor-only")
	outPath := filepath.Join(dir, "v4.0-commitment.0-100.kv")

	require.NoError(t, WriteCommitmentBoundaryFileV4(
		ctx, branches, anchor, outPath, dir, seg.CompressNone, log.New(),
	))

	got := readKV(t, outPath)
	require.Len(t, got, 1)
	require.Equal(t, commitmentdb.KeyCommitmentState, got[0][0])
	require.Equal(t, anchor, got[0][1])
}

// TestWriteCommitmentBoundaryFileV4_KeyCommitmentStateSortsAfterBranches
// pins the byte-value invariant the emit relies on: KeyCommitmentState
// ("state" = 0x73 0x74 0x61 0x74 0x65) sorts strictly after any valid
// branch key (branch keys are hex-nibbled — each byte in 0x00-0x0F).
// If the commitment key set ever gains a member starting with a byte
// ≥ 0x73, the "append anchor last" strategy stops being correct and
// the emit needs merge-sort. Guard the assumption here so any such
// change surfaces loudly.
func TestWriteCommitmentBoundaryFileV4_KeyCommitmentStateSortsAfterBranches(t *testing.T) {
	require.Equal(t, byte(0x73), commitmentdb.KeyCommitmentState[0],
		"KeyCommitmentState first byte changed — v4 emit's tail-anchor ordering must be reviewed")
	maxBranchByte := byte(0x0F)
	require.Less(t, maxBranchByte, commitmentdb.KeyCommitmentState[0],
		"branch first-byte range (0x00-0x0F) must remain below KeyCommitmentState[0]")
	require.Equal(t, -1, bytes.Compare([]byte{maxBranchByte}, commitmentdb.KeyCommitmentState),
		"sanity: max-nibble byte prefix must sort below KeyCommitmentState")
}

// TestWriteCommitmentBoundaryFileV4_ArgumentValidation covers the
// early-return error paths — each guards a caller mistake that would
// otherwise produce a corrupt file or panic downstream.
func TestWriteCommitmentBoundaryFileV4_ArgumentValidation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	branches := etl.NewCollectorWithAllocator("test-validate", dir, etl.SmallSortableBuffers, log.New())
	defer branches.Close()
	anchor := []byte("a")
	outPath := filepath.Join(dir, "out.kv")

	err := WriteCommitmentBoundaryFileV4(ctx, nil, anchor, outPath, dir, seg.CompressNone, log.New())
	require.ErrorContains(t, err, "branches collector is required")

	err = WriteCommitmentBoundaryFileV4(ctx, branches, nil, outPath, dir, seg.CompressNone, log.New())
	require.ErrorContains(t, err, "anchor blob is required")

	err = WriteCommitmentBoundaryFileV4(ctx, branches, []byte{}, outPath, dir, seg.CompressNone, log.New())
	require.ErrorContains(t, err, "anchor blob is required")

	err = WriteCommitmentBoundaryFileV4(ctx, branches, anchor, "", dir, seg.CompressNone, log.New())
	require.ErrorContains(t, err, "newKVPath is required")
}
