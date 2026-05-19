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

package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// hexCompact emits a V1-encoded key with no terminator for the supplied nibbles.
// It just delegates to nibbles.HexToCompact — the test wrapper
// exists so calls read like keys not transforms.
func hexCompact(nibs ...byte) []byte {
	return nibbles.HexToCompact(nibs)
}

// v2Key emits a V2 canonical key for the supplied nibbles.
func v2Key(nibs ...byte) []byte {
	return nibbles.EncodeKeyV2(nibs)
}

// branchWithAccountKey returns a minimal valid BranchData containing exactly one
// fieldAccountAddr cell at nibble 0 with the supplied bytes as the plain-key
// payload. The varint length prefix uses a single byte (so keys must be < 128B).
//
// Layout: [touchMap=0x0001 BE][afterMap=0x0001 BE][fields=0x02][varint(len)][keyBytes]
// fieldAccountAddr = 2 (see execution/commitment/commitment.go:180).
func branchWithAccountKey(keyBytes []byte) []byte {
	require := func(cond bool, msg string) {
		if !cond {
			panic(msg)
		}
	}
	require(len(keyBytes) < 128, "branchWithAccountKey: keyBytes must fit single-byte varint")
	var buf bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint16(hdr[0:], 0x0001) // touchMap: nibble 0
	binary.BigEndian.PutUint16(hdr[2:], 0x0001) // afterMap: nibble 0
	buf.Write(hdr[:])
	buf.WriteByte(0x02)                // fields = fieldAccountAddr
	buf.WriteByte(byte(len(keyBytes))) // varint length (single-byte form for len < 128)
	buf.Write(keyBytes)
	return buf.Bytes()
}

// branchWithStorageKey is the storage-side variant of branchWithAccountKey.
// fieldStorageAddr = 4 (commitment.go:181).
func branchWithStorageKey(keyBytes []byte) []byte {
	if len(keyBytes) >= 128 {
		panic("branchWithStorageKey: keyBytes must fit single-byte varint")
	}
	var buf bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint16(hdr[0:], 0x0001)
	binary.BigEndian.PutUint16(hdr[2:], 0x0001)
	buf.Write(hdr[:])
	buf.WriteByte(0x04)
	buf.WriteByte(byte(len(keyBytes)))
	buf.Write(keyBytes)
	return buf.Bytes()
}

func TestDetectKeyEncoding_AllV1(t *testing.T) {
	// V1-encoded keys with non-zero, non-one parity byte — guaranteed
	// to fail DecodeKeyV2 (ErrV2KeyParity) on at least one sample.
	samples := []sampledPair{
		// pick nibble sequences whose V1 output ends with values > 0x01
		{k: hexCompact(1, 2, 3, 4), v: nil},         // ends with 0x34
		{k: hexCompact(5, 6, 7, 8, 9, 0xa), v: nil}, // ends with 0x9a
		{k: hexCompact(0xb, 0xc, 0xd, 0xe), v: nil}, // ends with 0xde
	}
	v2, err := detectKeyEncoding(samples)
	require.NoError(t, err)
	require.False(t, v2, "expected V1 vote")
}

func TestDetectKeyEncoding_AllV2(t *testing.T) {
	samples := []sampledPair{
		{k: v2Key(1, 2, 3, 4), v: nil},
		{k: v2Key(5, 6, 7, 8, 9, 0xa, 0xb, 0xc), v: nil},
		{k: v2Key(0, 0, 0, 0), v: nil},
		{k: v2Key(0xf, 0xe, 0xd, 0xc, 0xb), v: nil}, // odd-length triggers parity=1 path
	}
	v2, err := detectKeyEncoding(samples)
	require.NoError(t, err)
	require.True(t, v2, "expected V2 vote")
}

func TestDetectKeyEncoding_OneV1AmongV2(t *testing.T) {
	// Mixed file: one bad parity byte aborts the V2 vote and returns V1.
	bad := v2Key(1, 2, 3, 4)
	bad[len(bad)-1] = 0x05 // corrupt the parity byte → ErrV2KeyParity
	samples := []sampledPair{
		{k: v2Key(1, 2, 3, 4), v: nil},
		{k: v2Key(5, 6, 7, 8), v: nil},
		{k: bad, v: nil},
	}
	v2, err := detectKeyEncoding(samples)
	require.NoError(t, err)
	require.False(t, v2, "any non-canonical sample → V1 vote")
}

func TestDetectKeyEncoding_StateKeysOnly(t *testing.T) {
	samples := []sampledPair{
		{k: commitmentdb.KeyCommitmentState, v: []byte{0x01, 0x02}},
		{k: commitmentdb.KeyCommitmentState, v: nil},
	}
	_, err := detectKeyEncoding(samples)
	require.ErrorIs(t, err, errNoNonStateSamples)
}

func TestDetectKeyEncoding_KnownAmbiguous(t *testing.T) {
	// Hand-crafted V1 keys whose V1-encoded bytes ALSO satisfy V2 canonicality.
	// HexToCompact on even-length nibble path with no terminator
	// emits [0x00, packedBytes...]. If the final packed byte is 0x00, the V1
	// output ends with 0x00 — exactly the V2 parity=0 marker. This documents
	// the ambiguity: the detector cannot distinguish such V1 files from V2
	// without out-of-band context. The floor probability of every sample
	// collapsing this way under uniform nibble content is roughly 10⁻⁹⁶ at
	// 48 samples; in practice it requires deliberate input.
	v1AmbiguousKey := hexCompact(1, 2, 3, 4, 5, 6, 0, 0) // ends with 0x00 byte
	// sanity-check: this is genuinely a valid V1 encoding
	require.Equal(t, byte(0x00), v1AmbiguousKey[0])
	require.Equal(t, byte(0x00), v1AmbiguousKey[len(v1AmbiguousKey)-1])
	// and it decodes V2-canonically
	_, err := nibbles.DecodeKeyV2(v1AmbiguousKey)
	require.NoError(t, err, "ambiguous key should V2-decode without error")

	samples := []sampledPair{
		{k: v1AmbiguousKey, v: nil},
		{k: hexCompact(0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0, 0), v: nil},
	}
	v2, err := detectKeyEncoding(samples)
	require.NoError(t, err)
	require.True(t, v2,
		"detector classifies ambiguous V1 file as V2 — known limitation, "+
			"documented in detectKeyEncoding godoc")
}

func TestDetectSqueezeState_AllSqueezed(t *testing.T) {
	// All embedded plain-key fields are short (4-byte file offsets).
	samples := []sampledPair{
		{k: []byte("key1"), v: branchWithAccountKey([]byte{0x01, 0x02, 0x03, 0x04})},
		{k: []byte("key2"), v: branchWithAccountKey([]byte{0x05, 0x06, 0x07})},
		{k: []byte("key3"), v: branchWithStorageKey([]byte{0x08, 0x09})},
	}
	squeezed, err := detectSqueezeState(samples)
	require.NoError(t, err)
	require.True(t, squeezed)
}

func TestDetectSqueezeState_AllUnsqueezed(t *testing.T) {
	addr20 := bytes.Repeat([]byte{0xaa}, 20)
	addr52 := bytes.Repeat([]byte{0xbb}, 52)
	samples := []sampledPair{
		{k: []byte("key1"), v: branchWithAccountKey(addr20)},
		{k: []byte("key2"), v: branchWithAccountKey(addr20)},
		{k: []byte("key3"), v: branchWithStorageKey(addr52)},
	}
	squeezed, err := detectSqueezeState(samples)
	require.NoError(t, err)
	require.False(t, squeezed)
}

func TestDetectSqueezeState_PartialSqueezed(t *testing.T) {
	// One short key among full-length keys — decisive squeezed signal.
	addr20 := bytes.Repeat([]byte{0xcc}, 20)
	samples := []sampledPair{
		{k: []byte("key1"), v: branchWithAccountKey(addr20)},
		{k: []byte("key2"), v: branchWithAccountKey(addr20)},
		{k: []byte("key3"), v: branchWithAccountKey([]byte{0x01, 0x02, 0x03})}, // short
		{k: []byte("key4"), v: branchWithAccountKey(addr20)},
	}
	squeezed, err := detectSqueezeState(samples)
	require.NoError(t, err)
	require.True(t, squeezed)
}

func TestDetectSqueezeState_StateKeysOnly(t *testing.T) {
	samples := []sampledPair{
		{k: commitmentdb.KeyCommitmentState, v: []byte{0x01, 0x02, 0x03}},
		{k: commitmentdb.KeyCommitmentState, v: nil},
	}
	_, err := detectSqueezeState(samples)
	require.ErrorIs(t, err, errNoNonStateSamples)
}

func TestDetectSqueezeState_EmptyValuesSkipped(t *testing.T) {
	addr20 := bytes.Repeat([]byte{0xdd}, 20)
	samples := []sampledPair{
		{k: []byte("key1"), v: nil}, // skipped (empty value)
		{k: []byte("key2"), v: branchWithAccountKey(addr20)},
	}
	squeezed, err := detectSqueezeState(samples)
	require.NoError(t, err)
	require.False(t, squeezed)
}

// nibblePaths covers the keyXform fixtures: a handful of distinct nibble paths
// of different lengths and parities. Empty path is the trivial case; odd/even
// parities exercise both branches of the V2 codec.
func nibblePaths() [][]byte {
	return [][]byte{
		{},
		{0xa},
		{0x2, 0xf},
		{0x2, 0xf, 0xb},
		{0x2, 0xf, 0xb, 0x3},
		{0x0, 0x0, 0x0, 0x0},
		{0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc},
		{0xf, 0xe, 0xd, 0xc, 0xb}, // odd length
	}
}

func TestXform_KeyXform_V1ToV1_Passthrough(t *testing.T) {
	xf := keyXform(false, false)
	for _, p := range nibblePaths() {
		k := nibbles.HexToCompact(p)
		got, err := xf(k)
		require.NoError(t, err)
		require.True(t, bytes.Equal(k, got), "V1→V1 pass-through must return identity bytes")
	}
}

func TestXform_KeyXform_V2ToV2_Passthrough(t *testing.T) {
	xf := keyXform(true, true)
	for _, p := range nibblePaths() {
		k := nibbles.EncodeKeyV2(p)
		got, err := xf(k)
		require.NoError(t, err)
		require.True(t, bytes.Equal(k, got), "V2→V2 pass-through must return identity bytes")
	}
}

func TestXform_KeyXform_V1ToV2(t *testing.T) {
	xf := keyXform(false, true)
	for _, p := range nibblePaths() {
		v1 := nibbles.HexToCompact(p)
		got, err := xf(v1)
		require.NoError(t, err)
		// Asserting on the round-trip rather than the encoded bytes keeps the
		// test stable against any future change to the V1/V2 byte layout: a V1
		// key, transformed to V2, must V2-decode to the same nibble path the
		// V1 key encoded.
		decoded, err := nibbles.DecodeKeyV2(got)
		require.NoError(t, err, "V1→V2 output must decode as canonical V2")
		require.Equal(t, p, decoded, "V1→V2 must preserve the nibble path")
	}
}

func TestXform_KeyXform_V2ToV1(t *testing.T) {
	xf := keyXform(true, false)
	for _, p := range nibblePaths() {
		v2 := nibbles.EncodeKeyV2(p)
		got, err := xf(v2)
		require.NoError(t, err)
		// Same round-trip strategy: V2 → V1 → re-encode V1 → byte-equal V1.
		// HexToCompact is deterministic and canonical for the
		// no-terminator paths we use, so re-encoding closes the loop.
		want := nibbles.HexToCompact(p)
		require.True(t, bytes.Equal(want, got),
			"V2→V1 must reproduce HexToCompact(%x): got %x, want %x", p, got, want)
	}
}

func TestXform_KeyXform_V1V2_RoundTrip(t *testing.T) {
	// Composed round-trip: V1 → V2 → V1 must be byte-equal to the starting V1.
	v1ToV2 := keyXform(false, true)
	v2ToV1 := keyXform(true, false)
	for _, p := range nibblePaths() {
		start := nibbles.HexToCompact(p)
		mid, err := v1ToV2(start)
		require.NoError(t, err)
		end, err := v2ToV1(mid)
		require.NoError(t, err)
		require.True(t, bytes.Equal(start, end),
			"V1→V2→V1 round-trip differs for path %x: start=%x end=%x", p, start, end)
	}
}

func TestXform_KeyXform_V2ToV1_ErrorPropagates(t *testing.T) {
	// Corrupt the parity byte → DecodeKeyV2 returns ErrV2KeyParity.
	bad := nibbles.EncodeKeyV2([]byte{1, 2, 3, 4})
	bad[len(bad)-1] = 0x05
	xf := keyXform(true, false)
	_, err := xf(bad)
	require.Error(t, err, "V2→V1 must propagate DecodeKeyV2 errors")
}

func TestXform_BuildValueTransformer_PassThrough(t *testing.T) {
	// Matching axes return a nil transformer without touching account/storage —
	// dumpStepRangeToPath treats nil as "value pass-through" and the converter's
	// pure-key-encoding mode does not require account/storage files to exist.
	for _, sq := range []bool{false, true} {
		vt, err := buildValueTransformer(sq, sq, nil, nil, nil, MergeRange{}, 0, 0, 0, 0, "")
		require.NoError(t, err)
		require.Nil(t, vt,
			"buildValueTransformer(detected=%v, target=%v) with matching axes must return nil", sq, sq)
	}
}

// fakeVisibleFile is a minimal kv.VisibleFile implementation for preflightResume
// tests. preflightResume only consults StartRootNum/EndRootNum (divided by
// stepSize) to derive each input file's step range; Fullpath is unused on the
// happy path but is present so the type satisfies the interface.
type fakeVisibleFile struct {
	path  string
	start uint64
	end   uint64
}

func (f fakeVisibleFile) Fullpath() string     { return f.path }
func (f fakeVisibleFile) StartRootNum() uint64 { return f.start }
func (f fakeVisibleFile) EndRootNum() uint64   { return f.end }

// preflightTestStepSize matches the unit step used in these tests: each input
// "file" spans one step, so StartRootNum=N maps to step range [N, N+1).
const preflightTestStepSize uint64 = 1

// preflightTestAccessors is the full accessor set used by these tests
// (commitment domain configs with btree + hashmap + existence filter all
// enabled).
var preflightTestAccessors = []string{".bt", ".kvi", ".kvei"}

// fakeInputFiles builds n contiguous one-step input files starting from step
// `firstStep`. Their Fullpath strings are not consulted by preflightResume but
// are made unique for easier debugging.
func fakeInputFiles(firstStep uint64, n int) VisibleFiles {
	out := make(VisibleFiles, n)
	for i := 0; i < n; i++ {
		s := firstStep + uint64(i)
		out[i] = fakeVisibleFile{
			path:  fmt.Sprintf("/fake/v1-commitment.%d-%d.kv", s, s+1),
			start: s,
			end:   s + 1,
		}
	}
	return out
}

// writeCompleteShard creates an empty .kv and an empty file for every required
// accessor (extensions in `accessors`) for step range [from, to) inside dir.
// All files are zero-byte; preflightResume's completeness check is presence-
// based (see Findings in the implementation plan), so size does not matter.
func writeCompleteShard(t *testing.T, dir string, from, to uint64, accessors []string) {
	t.Helper()
	base := fmt.Sprintf("v1-commitment.%d-%d", from, to)
	require.NoError(t, os.WriteFile(filepath.Join(dir, base+".kv"), nil, 0o644))
	for _, ext := range accessors {
		require.NoError(t, os.WriteFile(filepath.Join(dir, base+ext), nil, 0o644))
	}
}

func TestPreflightResume_continueFalse_wipes(t *testing.T) {
	tmp := t.TempDir()
	rebuildDir := filepath.Join(tmp, "snap", "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))
	// Pre-populate with junk: a complete shard, an orphan, a subdir.
	writeCompleteShard(t, rebuildDir, 0, 1, preflightTestAccessors)
	require.NoError(t, os.WriteFile(filepath.Join(rebuildDir, "stray.txt"), []byte("noise"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(rebuildDir, "subdir"), 0o755))

	files := fakeInputFiles(0, 3)
	out, err := preflightResume(files, rebuildDir, preflightTestAccessors, preflightTestStepSize, false, log.New())
	require.NoError(t, err)
	require.Equal(t, files, out, "continueMode=false must return input files unchanged")
	// dir.RemoveAll deletes the rebuildDir itself; the caller (ConvertCommitmentFiles)
	// re-creates it with MkdirAll afterwards.
	_, statErr := os.Stat(rebuildDir)
	require.True(t, os.IsNotExist(statErr), "continueMode=false must wipe rebuildDir, got stat err: %v", statErr)
}

func TestPreflightResume_empty(t *testing.T) {
	tmp := t.TempDir()
	rebuildDir := filepath.Join(tmp, "snap", "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))

	files := fakeInputFiles(0, 4)
	out, err := preflightResume(files, rebuildDir, preflightTestAccessors, preflightTestStepSize, true, log.New())
	require.NoError(t, err)
	require.Equal(t, files, out, "empty rebuildDir + continueMode=true must return full input slice")

	// rebuildDir was not pre-populated, so the missing-dir branch is exercised
	// when the dir literally does not exist either.
	missingDir := filepath.Join(tmp, "does", "not", "exist")
	out2, err := preflightResume(files, missingDir, preflightTestAccessors, preflightTestStepSize, true, log.New())
	require.NoError(t, err)
	require.Equal(t, files, out2, "missing rebuildDir + continueMode=true must return full input slice")
}

func TestPreflightResume_partial(t *testing.T) {
	tmp := t.TempDir()
	rebuildDir := filepath.Join(tmp, "snap", "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))

	// 4 inputs spanning steps 0..4. Pre-populate complete shards for the first
	// two (the contiguous prefix); the remaining two must be returned.
	files := fakeInputFiles(0, 4)
	writeCompleteShard(t, rebuildDir, 0, 1, preflightTestAccessors)
	writeCompleteShard(t, rebuildDir, 1, 2, preflightTestAccessors)

	out, err := preflightResume(files, rebuildDir, preflightTestAccessors, preflightTestStepSize, true, log.New())
	require.NoError(t, err)
	require.Equal(t, files[2:], out, "expected suffix containing only the unconverted inputs")
}

func TestPreflightResume_allDone(t *testing.T) {
	tmp := t.TempDir()
	rebuildDir := filepath.Join(tmp, "snap", "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))

	files := fakeInputFiles(0, 3)
	for _, f := range files {
		writeCompleteShard(t, rebuildDir, f.StartRootNum(), f.EndRootNum(), preflightTestAccessors)
	}

	out, err := preflightResume(files, rebuildDir, preflightTestAccessors, preflightTestStepSize, true, log.New())
	require.NoError(t, err)
	require.Empty(t, out, "every input matched a complete shard → empty suffix")
}

func TestPreflightResume_incomplete(t *testing.T) {
	tmp := t.TempDir()
	rebuildDir := filepath.Join(tmp, "snap", "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))

	// Single input file, partial shard: .kv + .bt present, .kvi missing.
	files := fakeInputFiles(0, 1)
	base := "v1-commitment.0-1"
	kvPath := filepath.Join(rebuildDir, base+".kv")
	btPath := filepath.Join(rebuildDir, base+".bt")
	require.NoError(t, os.WriteFile(kvPath, nil, 0o644))
	require.NoError(t, os.WriteFile(btPath, nil, 0o644))

	out, err := preflightResume(files, rebuildDir, preflightTestAccessors, preflightTestStepSize, true, log.New())
	require.NoError(t, err)
	require.Equal(t, files, out, "incomplete shard must not be treated as done; file is returned for re-conversion")

	// Partial siblings are removed so the next run starts from a clean slate.
	_, statErr := os.Stat(kvPath)
	require.True(t, os.IsNotExist(statErr), "incomplete .kv should be removed, got: %v", statErr)
	_, statErr = os.Stat(btPath)
	require.True(t, os.IsNotExist(statErr), "incomplete .bt sibling should be removed, got: %v", statErr)
}

func TestPreflightResume_gap(t *testing.T) {
	tmp := t.TempDir()
	rebuildDir := filepath.Join(tmp, "snap", "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))

	// Inputs 0..3. Complete shards for [0,1) and [2,3) — missing [1,2) in the
	// middle. preflightResume must return a hard error naming the gap.
	files := fakeInputFiles(0, 3)
	writeCompleteShard(t, rebuildDir, 0, 1, preflightTestAccessors)
	writeCompleteShard(t, rebuildDir, 2, 3, preflightTestAccessors)

	out, err := preflightResume(files, rebuildDir, preflightTestAccessors, preflightTestStepSize, true, log.New())
	require.Error(t, err)
	require.Nil(t, out)
	require.Contains(t, err.Error(), "non-contiguous shards")
	require.Contains(t, err.Error(), "1-2", "error must name the missing range")
}

func TestPreflightResume_orphan(t *testing.T) {
	tmp := t.TempDir()
	rebuildDir := filepath.Join(tmp, "snap", "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))

	// One complete shard + assorted orphans (no -commitment. step-range marker).
	writeCompleteShard(t, rebuildDir, 0, 1, preflightTestAccessors)
	orphan1 := filepath.Join(rebuildDir, "stray.txt")
	orphan2 := filepath.Join(rebuildDir, "v1-domain.0-1.kv") // not -commitment.
	orphan3 := filepath.Join(rebuildDir, "README")
	require.NoError(t, os.WriteFile(orphan1, []byte("noise"), 0o644))
	require.NoError(t, os.WriteFile(orphan2, nil, 0o644))
	require.NoError(t, os.WriteFile(orphan3, nil, 0o644))

	files := fakeInputFiles(0, 2)
	out, err := preflightResume(files, rebuildDir, preflightTestAccessors, preflightTestStepSize, true, log.New())
	require.NoError(t, err)
	require.Equal(t, files[1:], out, "first input is done, second remains")

	for _, p := range []string{orphan1, orphan2, orphan3} {
		_, statErr := os.Stat(p)
		require.True(t, os.IsNotExist(statErr), "orphan %s should be removed, got: %v", p, statErr)
	}
	// The complete shard's files survive.
	_, statErr := os.Stat(filepath.Join(rebuildDir, "v1-commitment.0-1.kv"))
	require.NoError(t, statErr, "complete shard .kv must remain")
}
