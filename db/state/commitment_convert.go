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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/log/v3"
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

// keyXform returns the per-pair key transform for the requested
// (detected, target) (V1/V2) combination. The four cases are:
//
//	V1 → V1  pass-through
//	V1 → V2  nibbles.EncodeKeyV2(commitment.UncompactNibbles(k))
//	V2 → V1  commitment.HexNibblesToCompactBytes(nibbles.DecodeKeyV2(k))
//	V2 → V2  pass-through
//
// V2 encoding panics on malformed input (path length > 128 or nibble > 0x0F).
// The V1→V2 path is preceded by UncompactNibbles whose output is bounded by
// the source bytes; V2→V1 is preceded by DecodeKeyV2 which itself rejects
// non-canonical input. If a path-length > 128 still slips through, EncodeKeyV2
// panics — this is intentional: a corrupt source file should fail loudly
// rather than silently emit garbage.
func keyXform(detectedV2, targetV2 bool) func([]byte) ([]byte, error) {
	switch {
	case detectedV2 == targetV2:
		return func(k []byte) ([]byte, error) { return k, nil }
	case !detectedV2 && targetV2:
		return func(k []byte) ([]byte, error) {
			return nibbles.EncodeKeyV2(commitment.UncompactNibbles(k)), nil
		}
	default: // detectedV2 && !targetV2
		return func(k []byte) ([]byte, error) {
			decoded, err := nibbles.DecodeKeyV2(k)
			if err != nil {
				return nil, fmt.Errorf("keyXform V2→V1: DecodeKeyV2(%x): %w", k, err)
			}
			return commitment.HexNibblesToCompactBytes(decoded), nil
		}
	}
}

// buildValueTransformer returns the valueTransformer for the requested
// (detected, target) squeeze-axis combination, or nil when both match
// (pass-through; the dump path treats nil as a no-op).
//
//	unsqueezed → unsqueezed  nil
//	unsqueezed → squeezed    commitment.commitmentValTransformDomain(rng, …)
//	squeezed   → unsqueezed  closure around ExpandShortenedKeysInBranch
//	squeezed   → squeezed    nil
//
// The unsqueezed→squeezed path delegates to the existing read-side machinery
// in domain_committed.go so the converter writes the same on-disk format the
// live system produces. The squeezed→unsqueezed direction calls
// ExpandShortenedKeysInBranch — the same code path the live read side uses
// to materialise plain keys.
//
// KeyCommitmentState rows have no embedded plain keys; both directions are
// effectively pass-through on their value, so the caller does not need a
// state-value carve-out.
func buildValueTransformer(
	detectedSqueezed, targetSqueezed bool,
	commitmentRo *DomainRoTx,
	accounts, storage *DomainRoTx,
	af, sf *FilesItem,
	rng MergeRange,
) (valueTransformer, error) {
	if detectedSqueezed == targetSqueezed {
		return nil, nil
	}
	if !detectedSqueezed && targetSqueezed {
		vt, err := commitmentRo.commitmentValTransformDomain(rng, accounts, storage, af, sf)
		if err != nil {
			return nil, fmt.Errorf("buildValueTransformer unsqueezed→squeezed: %w", err)
		}
		return vt, nil
	}
	// squeezed → unsqueezed
	return func(val []byte, startTxNum, endTxNum uint64) ([]byte, error) {
		if len(val) == 0 {
			return val, nil
		}
		expanded, err := ExpandShortenedKeysInBranch(val, accounts, storage, af, sf, startTxNum, endTxNum)
		if err != nil {
			return nil, fmt.Errorf("buildValueTransformer squeezed→unsqueezed: %w", err)
		}
		return expanded, nil
	}, nil
}

// convertCommitmentFile produces a re-encoded copy of `file` in `dstDir`.
//
// It detects the source file's current state, builds the per-pair key and value
// transformers for the target ConvertOpts, streams every (k, v) pair from the
// source via the aggregator's compression-aware reader, applies keyXform per
// pair (state keys pass through), and pushes (newKey, value) into a fresh
// TemporalMemBatch's commitment-domain wal. The value-side transform is
// supplied to dumpStepRangeToPath as `vt valueTransformer` so it runs during
// collation alongside the ETL sort.
//
// dstDir must exist; the resulting `.kv` plus per-domain index files (.bt /
// .kvi / .kvei depending on the commitment-domain config) are written there.
// The aggregator's view of `snapshots/domain/` is not touched: dumpStepRangeToPath
// is called with `integrate=false`, so originals remain readable for the entire
// duration of Phase 1 and a crashed run is recoverable by just deleting dstDir.
//
// If the detected state already matches `opts`, returns `errSkip` (a sentinel
// caught by the orchestrator) without producing any output. Range-mismatch and
// codec errors are wrapped with the source file's path.
//
// `progressPrefix` is built once per file by the orchestrator
// (e.g. `"(3/12 files, 25.0% overall)"`) and appended verbatim to per-file log
// lines so no overall-% arithmetic happens inside the conversion loop.
func convertCommitmentFile(
	ctx context.Context,
	at *AggregatorRoTx,
	file VisibleFile,
	dstDir string,
	opts ConvertOpts,
	progressPrefix string,
	logger log.Logger,
) (sizeDelta datasize.ByteSize, deltaPct float32, err error) {
	st, err := detectFileState(at, file, 48)
	if err != nil {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: %w", file.Fullpath(), err)
	}
	if st.keysV2 == opts.TargetNibblesV2 && st.squeezed == opts.TargetSqueeze {
		return 0, 0, errSkip
	}

	stepSize := at.StepSize()
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()
	if endTxNum == 0 || endTxNum < startTxNum {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: invalid range %d..%d", file.Fullpath(), startTxNum, endTxNum)
	}
	stepFrom := kv.Step(startTxNum / stepSize)
	stepTo := kv.Step(endTxNum / stepSize)

	commitmentRo := at.d[kv.CommitmentDomain]
	accountsRo := at.d[kv.AccountsDomain]
	storageRo := at.d[kv.StorageDomain]

	af, err := accountsRo.rawLookupFileByRange(startTxNum, endTxNum)
	if err != nil {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: %w (accounts step %d-%d): %w",
			file.Fullpath(), errRangeMatch, stepFrom, stepTo, err)
	}
	sf, err := storageRo.rawLookupFileByRange(startTxNum, endTxNum)
	if err != nil {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: %w (storage step %d-%d): %w",
			file.Fullpath(), errRangeMatch, stepFrom, stepTo, err)
	}

	kxform := keyXform(st.keysV2, opts.TargetNibblesV2)
	rng := MergeRange{name: "convert", needMerge: true, from: startTxNum, to: endTxNum}
	vt, err := buildValueTransformer(st.squeezed, opts.TargetSqueeze, commitmentRo, accountsRo, storageRo, af, sf, rng)
	if err != nil {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: %w", file.Fullpath(), err)
	}

	vf, ok := file.(visibleFile)
	if !ok {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: VisibleFile is not state.visibleFile (got %T)", file.Fullpath(), file)
	}
	src := vf.src
	if src == nil || src.decompressor == nil {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: source has no decompressor", file.Fullpath())
	}

	batch := &TemporalMemBatch{}
	batch.domainWriters[kv.CommitmentDomain] = commitmentRo.NewWriter()
	wal := batch.domainWriters[kv.CommitmentDomain]
	defer wal.Close()

	reader := commitmentRo.dataReader(src.decompressor)
	reader.Reset(0)

	totalForFile := at.KeyCountInFiles(kv.CommitmentDomain, startTxNum, endTxNum)
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	// txNumForPut only affects the 8-byte step prefix on the wal entry; collateETL
	// strips it before applying vt. endTxNum-1 guarantees the encoded step lands
	// inside [stepFrom, stepTo).
	txNumForPut := endTxNum - 1
	baseName := filepath.Base(file.Fullpath())

	var ki uint64
	var k, v []byte
	for reader.HasNext() {
		k, _ = reader.Next(k[:0])
		if !reader.HasNext() {
			return 0, 0, fmt.Errorf("convertCommitmentFile %q: truncated at ki=%d (value missing)", file.Fullpath(), ki)
		}
		v, _ = reader.Next(v[:0])
		ki++

		var outKey []byte
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			outKey = append([]byte(nil), k...)
		} else {
			tk, kerr := kxform(k)
			if kerr != nil {
				return 0, 0, fmt.Errorf("convertCommitmentFile %q: keyXform at ki=%d key=%x: %w",
					file.Fullpath(), ki, k, kerr)
			}
			// keyXform may return the input slice itself (pass-through cases) or
			// a buffer owned by the codec; copy so the next reader.Next does not
			// stomp it before the etl collector serialises the row.
			outKey = append([]byte(nil), tk...)
		}
		// Copy v too: the wal stores it into an etl buffer; reader.Next will
		// reuse the underlying slice on the next iteration.
		if perr := wal.PutWithPrev(outKey, append([]byte(nil), v...), txNumForPut, nil); perr != nil {
			return 0, 0, fmt.Errorf("convertCommitmentFile %q: wal put at ki=%d: %w", file.Fullpath(), ki, perr)
		}

		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-logEvery.C:
			filePct := float32(0)
			if totalForFile > 0 {
				filePct = 100.0 * float32(ki) / float32(totalForFile)
			}
			logger.Info(fmt.Sprintf(
				"[commitment_convert] progress %d/%d file=%s (%.1f%% in file) %s",
				ki, totalForFile, baseName, filePct, progressPrefix))
		default:
		}
	}

	if err := commitmentRo.d.dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, vt, dstDir, false); err != nil {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: dumpStepRangeToPath: %w", file.Fullpath(), err)
	}

	newKVPath := commitmentRo.d.kvNewFilePathIn(dstDir, stepFrom, stepTo)
	delta, pct, err := commitmentFileSizeDelta(file.Fullpath(), newKVPath)
	if err != nil {
		return 0, 0, fmt.Errorf("convertCommitmentFile %q: size delta: %w", file.Fullpath(), err)
	}

	logger.Info(fmt.Sprintf(
		"[commitment_convert] phase 1 file done %s sizeDelta=%.1f%% ki=%d %s",
		baseName, pct, ki, progressPrefix))

	return delta, pct, nil
}

// commitmentFileSizeDelta returns (origSize-newSize) and that delta as a
// percentage of origSize. Positive values mean the new file is smaller.
// Mirrors the pattern in SqueezeCommitmentFiles (squeeze.go:156-166).
func commitmentFileSizeDelta(origPath, newPath string) (datasize.ByteSize, float32, error) {
	oi, err := os.Stat(origPath)
	if err != nil {
		return 0, 0, err
	}
	ni, err := os.Stat(newPath)
	if err != nil {
		return 0, 0, err
	}
	delta := datasize.ByteSize(oi.Size()) - datasize.ByteSize(ni.Size())
	var pct float32
	if oi.Size() > 0 {
		pct = 100.0 * float32(oi.Size()-ni.Size()) / float32(oi.Size())
	}
	return delta, pct, nil
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
