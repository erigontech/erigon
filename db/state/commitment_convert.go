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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// ConvertOpts is the target encoding state requested from ConvertCommitmentFiles.
// Both axes are independent: TargetSqueeze controls the value codec
// (squeezed = plain keys replaced with file offsets), TargetNibblesV2 controls
// the key codec (V1 = HexNibblesToCompactBytes, V2 = nibbles.EncodeKeyV2).
type ConvertOpts struct {
	TargetSqueeze   bool
	TargetNibblesV2 bool
}

// fileState describes the detected current encoding of a commitment .kv file.
type fileState struct {
	keysV2   bool
	squeezed bool
}

// xformFns groups the per-pair transforms used by the converter pipeline.
// state is the dispatch hook for KeyCommitmentState (today: pass-through);
// it lives here so a future iteration can mutate the state blob without
// touching the main streaming loop.
type xformFns struct {
	key   func([]byte) ([]byte, error)
	state func([]byte, []byte) ([]byte, []byte, error)
}

// errSkip signals "file is already in the target encoding"; the orchestrator
// catches this and skips the file without counting it as an error.
// Mixed-detection, range-mismatch, etc. use fmt.Errorf — never compared.
var (
	errSkip              = errors.New("file already in target state")
	errMixedKeyEncoding  = errors.New("mixed key encoding within file")
	errMixedSqueezeState = errors.New("mixed squeeze state within file")
	errRangeMatch        = errors.New("no matching account/storage file for range")
	errNoNonStateSamples = errors.New("no non-state samples to vote with")
)

// sampledPair is one (k, v) read out of a commitment .kv file at a sampled offset.
// Used by the detection helpers.
type sampledPair struct {
	k, v []byte
}

// detectKeyEncoding votes V1 vs V2 by feeding every non-state key to
// nibbles.DecodeKeyV2 and observing whether it returns one of the four
// V2 canonicality errors.
//
//   - any sample returns an error → file is V1 (return false, nil)
//   - all samples decode canonically → file is V2 (return true, nil)
//   - zero non-state samples → return errNoNonStateSamples; caller widens
//
// V1 and V2 canonical forms overlap byte-wise — a real V1 key may also
// decode V2-canonically. Across 48 distributed samples the probability of
// every sample independently satisfying V2 canonicality on a real V1 file
// is vanishingly small (≈10⁻⁹⁶ under uniformly distributed nibble content
// given the trailing-byte + pad-nibble constraints). Distributed sampling
// (not consecutive reads) is the load-bearing assumption here because
// commitment files are prefix-sorted and consecutive entries cluster on
// shared prefixes whose byte distribution is not representative.
func detectKeyEncoding(samples []sampledPair) (bool, error) {
	sawAny := false
	for _, p := range samples {
		if bytes.Equal(p.k, commitmentdb.KeyCommitmentState) {
			continue
		}
		sawAny = true
		if _, err := nibbles.DecodeKeyV2(p.k); err != nil {
			return false, nil
		}
	}
	if !sawAny {
		return false, errNoNonStateSamples
	}
	return true, nil
}

// detectSqueezeState votes squeezed vs unsqueezed by looking for any embedded
// plain-key field shorter than binary.MaxVarintLen64 (10 bytes) in the sampled
// BranchData values. Real plain keys are exactly 20 bytes (account address) or
// 52 bytes (address + storage slot); a single field with length < 10 proves
// the value was squeezed (varint file offset). Asymmetric on purpose — one
// short field is decisive, "unsqueezed" requires every sampled plain-key
// field to be ≥ 10 bytes.
//
// Parse failures on a single BranchData are not decisive; the caller may
// have sampled a malformed or empty branch (unlikely in practice). The
// detector skips such samples and votes on what remains; zero usable
// non-state samples returns errNoNonStateSamples.
func detectSqueezeState(samples []sampledPair) (bool, error) {
	sawAny := false
	for _, p := range samples {
		if bytes.Equal(p.k, commitmentdb.KeyCommitmentState) {
			continue
		}
		if len(p.v) == 0 {
			continue
		}
		short := false
		_, err := commitment.BranchData(p.v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			if len(key) < binary.MaxVarintLen64 {
				short = true
			}
			return nil, nil
		})
		if err != nil {
			continue
		}
		sawAny = true
		if short {
			return true, nil
		}
	}
	if !sawAny {
		return false, errNoNonStateSamples
	}
	return false, nil
}

// detectFileState reads `samples` distributed (k, v) pairs from the commitment
// file and runs both detectors. Uses BT ordinal lookup when available (constant
// per-sample cost regardless of file size); falls back to stride-skip on a
// sequential seg.Reader if the file has no BT index.
//
// 48 distributed samples is the recommended count: enough to drive the V1/V2
// false-positive floor below 10⁻⁹⁶ while keeping detection well under a
// second on a multi-GB file via BT ordinal lookup.
func detectFileState(at *AggregatorRoTx, file VisibleFile, samples int) (fileState, error) {
	vf, ok := file.(visibleFile)
	if !ok {
		return fileState{}, fmt.Errorf("detectFileState: VisibleFile %q is not a state.visibleFile (got %T)", file.Fullpath(), file)
	}
	fi := vf.src
	if fi == nil || fi.decompressor == nil {
		return fileState{}, fmt.Errorf("detectFileState: %q has no decompressor", file.Fullpath())
	}
	dt := at.d[kv.CommitmentDomain]

	pairs, err := sampleCommitmentFile(dt, fi, samples)
	if err != nil {
		return fileState{}, fmt.Errorf("detectFileState: sampling %q: %w", file.Fullpath(), err)
	}
	if len(pairs) == 0 {
		return fileState{}, fmt.Errorf("detectFileState: %q is empty", file.Fullpath())
	}

	keysV2, err := detectKeyEncoding(pairs)
	if err != nil {
		return fileState{}, fmt.Errorf("detectFileState: %q: key-encoding vote: %w", file.Fullpath(), err)
	}
	squeezed, err := detectSqueezeState(pairs)
	if err != nil {
		return fileState{}, fmt.Errorf("detectFileState: %q: squeeze-state vote: %w", file.Fullpath(), err)
	}
	return fileState{keysV2: keysV2, squeezed: squeezed}, nil
}

// sampleCommitmentFile returns up to `samples` (k, v) pairs read at evenly
// distributed positions in the commitment file. Prefers BT ordinal lookup
// (O(log N) per sample, no sequential scan); falls back to stride-skip on
// a sequential seg.Reader if the file has no BT index.
//
// Returned pairs hold copies of the underlying bytes — the seg/bt buffers
// are reused on every read.
func sampleCommitmentFile(dt *DomainRoTx, fi *FilesItem, samples int) ([]sampledPair, error) {
	if samples <= 0 {
		return nil, fmt.Errorf("sampleCommitmentFile: samples must be > 0 (got %d)", samples)
	}
	if fi.bindex != nil && !fi.bindex.Empty() {
		return sampleViaBT(dt, fi, samples)
	}
	return sampleViaStride(dt, fi, samples)
}

func sampleViaBT(dt *DomainRoTx, fi *FilesItem, samples int) ([]sampledPair, error) {
	keyCount := fi.bindex.KeyCount()
	if keyCount == 0 {
		return nil, nil
	}
	n := uint64(samples)
	if n > keyCount {
		n = keyCount
	}
	reader := dt.dataReader(fi.decompressor)
	out := make([]sampledPair, 0, n)
	for i := uint64(0); i < n; i++ {
		// distribute samples across the full key space; stride = keyCount/n
		// puts the i-th sample at index i*keyCount/n, covering [0, keyCount).
		ordinal := i * keyCount / n
		cur := fi.bindex.OrdinalLookup(reader, ordinal)
		if cur == nil {
			return nil, fmt.Errorf("sampleViaBT: ordinal %d/%d lookup returned nil in %s", ordinal, keyCount, fi.decompressor.FileName())
		}
		// Copy: cur.Key / cur.Value may share buffers with the underlying getter.
		k := append([]byte(nil), cur.Key()...)
		v := append([]byte(nil), cur.Value()...)
		out = append(out, sampledPair{k: k, v: v})
	}
	return out, nil
}

func sampleViaStride(dt *DomainRoTx, fi *FilesItem, samples int) ([]sampledPair, error) {
	reader := dt.dataReader(fi.decompressor)
	// Count pairs by a fast pass. The seg layer doesn't expose pair count
	// directly through the reader, but decompressor.Count() returns the total
	// number of words; commitment .kv files are (k, v) word pairs.
	wordCount := fi.decompressor.Count()
	if wordCount == 0 || wordCount%2 != 0 {
		return nil, fmt.Errorf("sampleViaStride: %s has %d words (want even, non-zero)", fi.decompressor.FileName(), wordCount)
	}
	pairCount := wordCount / 2
	n := samples
	if n > pairCount {
		n = pairCount
	}
	stride := pairCount / n
	if stride == 0 {
		stride = 1
	}

	reader.Reset(0)
	out := make([]sampledPair, 0, n)
	var taken int
	var pairIdx int
	for reader.HasNext() && taken < n {
		// Read or skip the (k, v) pair at pairIdx.
		if pairIdx == taken*stride {
			k, _ := reader.Next(nil)
			if !reader.HasNext() {
				return nil, fmt.Errorf("sampleViaStride: %s truncated at pair %d", fi.decompressor.FileName(), pairIdx)
			}
			v, _ := reader.Next(nil)
			out = append(out, sampledPair{
				k: append([]byte(nil), k...),
				v: append([]byte(nil), v...),
			})
			taken++
		} else {
			// Skip key.
			if _, _ = reader.Skip(); !reader.HasNext() {
				return nil, fmt.Errorf("sampleViaStride: %s truncated skipping pair %d", fi.decompressor.FileName(), pairIdx)
			}
			// Skip value.
			_, _ = reader.Skip()
		}
		pairIdx++
	}
	return out, nil
}
