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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// hexCompact emits a V1-encoded key with no terminator for the supplied nibbles.
// It just delegates to commitment.HexNibblesToCompactBytes — the test wrapper
// exists so calls read like keys not transforms.
func hexCompact(nibs ...byte) []byte {
	return commitment.HexNibblesToCompactBytes(nibs)
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
	// HexNibblesToCompactBytes on even-length nibble path with no terminator
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
		k := commitment.HexNibblesToCompactBytes(p)
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
		v1 := commitment.HexNibblesToCompactBytes(p)
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
		// HexNibblesToCompactBytes is deterministic and canonical for the
		// no-terminator paths we use, so re-encoding closes the loop.
		want := commitment.HexNibblesToCompactBytes(p)
		require.True(t, bytes.Equal(want, got),
			"V2→V1 must reproduce HexNibblesToCompactBytes(%x): got %x, want %x", p, got, want)
	}
}

func TestXform_KeyXform_V1V2_RoundTrip(t *testing.T) {
	// Composed round-trip: V1 → V2 → V1 must be byte-equal to the starting V1.
	v1ToV2 := keyXform(false, true)
	v2ToV1 := keyXform(true, false)
	for _, p := range nibblePaths() {
		start := commitment.HexNibblesToCompactBytes(p)
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
	// Matching axes return a nil transformer; dumpStepRangeToPath treats nil
	// as "value pass-through", so no closure allocation is needed.
	for _, sq := range []bool{false, true} {
		vt, err := buildValueTransformer(sq, sq, nil, nil, nil, nil, nil, MergeRange{})
		require.NoError(t, err)
		require.Nil(t, vt,
			"buildValueTransformer(detected=%v, target=%v) with matching axes must return nil", sq, sq)
	}
}
