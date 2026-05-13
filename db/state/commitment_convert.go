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
	"regexp"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// commitmentStepRangeRe parses the `<from>-<to>` step range from a commitment
// filename of the shape `<anything>-commitment.<from>-<to>.<ext>`.
var commitmentStepRangeRe = regexp.MustCompile(`-commitment\.(\d+)-(\d+)\.`)

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

// errSkip signals "file is already in the target encoding"; the orchestrator
// catches this and skips the file without counting it as an error.
var (
	errSkip              = errors.New("file already in target state")
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
// Sub-threshold (steps < DomainMinStepsToCompress) commitment files are
// written uncompressed by the merge path; this constructs the reader with
// the correct codec for the file's step span so detection reads bytes
// correctly. Mirrors convertCommitmentFile's codec selection.
//
// Returned pairs hold copies of the underlying bytes — the seg/bt buffers
// are reused on every read.
func sampleCommitmentFile(dt *DomainRoTx, fi *FilesItem, samples int) ([]sampledPair, error) {
	if samples <= 0 {
		return nil, fmt.Errorf("sampleCommitmentFile: samples must be > 0 (got %d)", samples)
	}
	compression := dt.d.Compression
	if fi.StepCount(dt.d.stepSize) < DomainMinStepsToCompress {
		compression = seg.CompressNone
	}
	reader := seg.NewReader(fi.decompressor.MakeGetter(), compression)
	if fi.bindex != nil && !fi.bindex.Empty() {
		return sampleViaBT(reader, fi, samples)
	}
	return sampleViaStride(reader, fi, samples)
}

func sampleViaBT(reader *seg.Reader, fi *FilesItem, samples int) ([]sampledPair, error) {
	keyCount := fi.bindex.KeyCount()
	if keyCount == 0 {
		return nil, nil
	}
	n := uint64(samples)
	if n > keyCount {
		n = keyCount
	}
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
// Account+storage file lookup happens here (not at the call site) so pure
// key-encoding conversions (squeeze axis unchanged) don't need account/storage
// files at all — useful when the datadir was produced by a partial rebuild
// where commitment ranges don't align with account/storage ranges.
//
// KeyCommitmentState rows have no embedded plain keys; both directions are
// effectively pass-through on their value, so the caller does not need a
// state-value carve-out.
func buildValueTransformer(
	detectedSqueezed, targetSqueezed bool,
	commitmentRo *DomainRoTx,
	accounts, storage *DomainRoTx,
	rng MergeRange,
	startTxNum, endTxNum uint64,
	stepFrom, stepTo kv.Step,
	srcPath string,
) (valueTransformer, error) {
	if detectedSqueezed == targetSqueezed {
		return nil, nil
	}
	af, err := accounts.rawLookupFileByRange(startTxNum, endTxNum)
	if err != nil {
		return nil, fmt.Errorf("%q: %w (accounts step %d-%d): %w",
			srcPath, errRangeMatch, stepFrom, stepTo, err)
	}
	sf, err := storage.rawLookupFileByRange(startTxNum, endTxNum)
	if err != nil {
		return nil, fmt.Errorf("%q: %w (storage step %d-%d): %w",
			srcPath, errRangeMatch, stepFrom, stepTo, err)
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

// buildPhase1Prefix renders the trailing `(N/M files, X.X% overall by keys)`
// suffix appended to every phase-1 log line. The percentage is computed from
// `processedKeys / grandTotalKeys` rather than file count so the displayed
// progress matches the actual work done (commitment files vary in size by
// orders of magnitude across step ranges).
func buildPhase1Prefix(fileIdx, fileTotal int, processedKeys, grandTotalKeys uint64) string {
	pct := float32(0)
	if grandTotalKeys > 0 {
		pct = 100.0 * float32(processedKeys) / float32(grandTotalKeys)
	}
	return fmt.Sprintf("(%d/%d files, %.1f%% overall by keys)", fileIdx, fileTotal, pct)
}

// formatRate returns the key/s rate for `ki` keys read in `elapsed`, formatted
// via PrettyCounter. Tiny files finish in under a second and produce absurdly
// large rates; suppress those by returning "--" instead.
func formatRate(ki uint64, elapsed time.Duration) string {
	if elapsed < time.Second {
		return "--"
	}
	return common.PrettyCounter(uint64(float64(ki) / elapsed.Seconds()))
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
// `fileIdx`/`fileTotal`/`grandTotalKeys`/`processedKeys` are used to render the
// per-tick `(N/M files, X.X% overall by keys)` progress suffix. The function
// rebuilds the suffix per log site (not once per file) so the percentage stays
// current mid-file. `ki` is the number of (k, v) pairs read from the source,
// returned so the caller can advance its running `processedKeys` total.
func convertCommitmentFile(
	ctx context.Context,
	at *AggregatorRoTx,
	file VisibleFile,
	dstDir string,
	opts ConvertOpts,
	fileIdx, fileTotal int,
	grandTotalKeys, processedKeys uint64,
	logger log.Logger,
) (sizeDelta int64, deltaPct float32, ki uint64, err error) {
	st, err := detectFileState(at, file, 48)
	if err != nil {
		// A file with only the state row (no non-state samples) has no branches
		// to re-encode on either axis. Treat as already-in-target so the
		// orchestrator skips it instead of aborting Phase 1 on every other file.
		if errors.Is(err, errNoNonStateSamples) {
			return 0, 0, 0, errSkip
		}
		return 0, 0, 0, fmt.Errorf("convertCommitmentFile %q: %w", file.Fullpath(), err)
	}

	stepSize := at.StepSize()
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()
	if endTxNum == 0 || endTxNum < startTxNum {
		return 0, 0, 0, fmt.Errorf("convertCommitmentFile %q: invalid range %d..%d", file.Fullpath(), startTxNum, endTxNum)
	}
	stepFrom := kv.Step(startTxNum / stepSize)
	stepTo := kv.Step(endTxNum / stepSize)

	// commitmentValTransformDomain short-circuits to pass-through when the
	// file's step span doesn't reach the plain-key-referencing threshold
	// (odd step span — see ValuesPlainKeyReferencingThresholdReached). Such a
	// file cannot be squeezed; pretend it's already at target on the squeeze
	// axis so the converter doesn't rewrite it with the same value bytes but
	// a "squeezed" label, which would break idempotency.
	effectiveTargetSqueeze := opts.TargetSqueeze
	if !st.squeezed && opts.TargetSqueeze &&
		!ValuesPlainKeyReferencingThresholdReached(stepSize, startTxNum, endTxNum) {
		effectiveTargetSqueeze = false
		logger.Info(fmt.Sprintf(
			"[commitment_convert] %s: step span %d-%d below squeeze threshold; squeeze axis treated as already-target",
			filepath.Base(file.Fullpath()), stepFrom, stepTo))
	}
	if st.keysV2 == opts.TargetNibblesV2 && st.squeezed == effectiveTargetSqueeze {
		return 0, 0, 0, errSkip
	}

	commitmentRo := at.d[kv.CommitmentDomain]
	accountsRo := at.d[kv.AccountsDomain]
	storageRo := at.d[kv.StorageDomain]

	kxform := keyXform(st.keysV2, opts.TargetNibblesV2)
	rng := MergeRange{name: "convert", needMerge: true, from: startTxNum, to: endTxNum}
	// Account+storage files are only needed when the squeeze axis changes (the
	// value transformer dereferences/expands plain-key offsets). For pure key-
	// encoding conversions, skip the lookups so the converter still works on
	// datadirs where commitment ranges don't align with account/storage ranges.
	vt, err := buildValueTransformer(st.squeezed, effectiveTargetSqueeze, commitmentRo, accountsRo, storageRo, rng, startTxNum, endTxNum, stepFrom, stepTo, file.Fullpath())
	if err != nil {
		return 0, 0, 0, fmt.Errorf("convertCommitmentFile %q: %w", file.Fullpath(), err)
	}

	vf, ok := file.(visibleFile)
	if !ok {
		return 0, 0, 0, fmt.Errorf("convertCommitmentFile %q: VisibleFile is not state.visibleFile (got %T)", file.Fullpath(), file)
	}
	src := vf.src
	if src == nil || src.decompressor == nil {
		return 0, 0, 0, fmt.Errorf("convertCommitmentFile %q: source has no decompressor", file.Fullpath())
	}

	batch := &TemporalMemBatch{}
	batch.domainWriters[kv.CommitmentDomain] = commitmentRo.NewWriter()
	wal := batch.domainWriters[kv.CommitmentDomain]
	defer wal.Close()

	// Sub-threshold (steps < DomainMinStepsToCompress) commitment files are
	// written uncompressed by the merge path (see merge.go:431). Reading them
	// with the domain's configured Compression would feed compressed-key
	// decoding against raw bytes — mirror SqueezeCommitmentFiles' logic and
	// pick the correct codec from the source file's step span.
	srcCompression := commitmentRo.d.Compression
	if src.StepCount(stepSize) < DomainMinStepsToCompress {
		srcCompression = seg.CompressNone
	}
	reader := seg.NewReader(src.decompressor.MakeGetter(), srcCompression)
	reader.Reset(0)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	// txNumForPut only affects the 8-byte step prefix on the wal entry; collateETL
	// strips it before applying vt. endTxNum-1 guarantees the encoded step lands
	// inside [stepFrom, stepTo).
	txNumForPut := endTxNum - 1
	baseName := filepath.Base(file.Fullpath())
	fileStart := time.Now()

	// Value transform is applied here (not in collateETL) so we can (a) carve out
	// KeyCommitmentState — its value is opaque metadata, not BranchData — and (b)
	// pass the actual source file range to vt, matching SqueezeCommitmentFiles.
	// collateETL's per-pair fromTxNum/endTxNum are derived from stepFrom/stepTo
	// with a (-1) offset that does not match the source file for stepFrom>0 files.
	var k, v []byte
	for reader.HasNext() {
		k, _ = reader.Next(k[:0])
		if !reader.HasNext() {
			return 0, 0, ki, fmt.Errorf("convertCommitmentFile %q: truncated at ki=%d (value missing)", file.Fullpath(), ki)
		}
		v, _ = reader.Next(v[:0])
		ki++

		isState := bytes.Equal(k, commitmentdb.KeyCommitmentState)
		var outKey []byte
		if isState {
			outKey = append([]byte(nil), k...)
		} else {
			tk, kerr := kxform(k)
			if kerr != nil {
				return 0, 0, ki, fmt.Errorf("convertCommitmentFile %q: keyXform at ki=%d key=%x: %w",
					file.Fullpath(), ki, k, kerr)
			}
			outKey = append([]byte(nil), tk...)
		}

		var outVal []byte
		if !isState && vt != nil {
			tv, verr := vt(v, startTxNum, endTxNum)
			if verr != nil {
				return 0, 0, ki, fmt.Errorf("convertCommitmentFile %q: vt at ki=%d key=%x: %w",
					file.Fullpath(), ki, k, verr)
			}
			outVal = append([]byte(nil), tv...)
		} else {
			outVal = append([]byte(nil), v...)
		}

		if perr := wal.PutWithPrev(outKey, outVal, txNumForPut, nil); perr != nil {
			return 0, 0, ki, fmt.Errorf("convertCommitmentFile %q: wal put at ki=%d: %w", file.Fullpath(), ki, perr)
		}

		select {
		case <-ctx.Done():
			return 0, 0, ki, ctx.Err()
		case <-logEvery.C:
			logger.Info(fmt.Sprintf(
				"[commitment_convert] phase 1 file=%s processing %s key/s at %s/%s %s",
				baseName,
				formatRate(ki, time.Since(fileStart)),
				common.PrettyCounter(processedKeys+ki),
				common.PrettyCounter(grandTotalKeys),
				buildPhase1Prefix(fileIdx, fileTotal, processedKeys+ki, grandTotalKeys)))
		default:
		}
	}

	// vt was applied per-pair above; pass nil so collateETL doesn't double-transform.
	if err := commitmentRo.d.dumpStepRangeToPath(ctx, stepFrom, stepTo, batch, nil, dstDir, false); err != nil {
		return 0, 0, ki, fmt.Errorf("convertCommitmentFile %q: dumpStepRangeToPath: %w", file.Fullpath(), err)
	}

	newKVPath := commitmentRo.d.kvNewFilePathIn(dstDir, stepFrom, stepTo)
	delta, pct, err := commitmentFileSizeDelta(file.Fullpath(), newKVPath)
	if err != nil {
		return 0, 0, ki, fmt.Errorf("convertCommitmentFile %q: size delta: %w", file.Fullpath(), err)
	}

	elapsed := time.Since(fileStart)
	logger.Info(fmt.Sprintf(
		"[commitment_convert] phase 1 file done %s ki=%s sizeDelta=%.1f%% in %s (%s key/s) at %s/%s %s",
		baseName,
		common.PrettyCounter(ki),
		pct,
		elapsed.Round(time.Second),
		formatRate(ki, elapsed),
		common.PrettyCounter(processedKeys+ki),
		common.PrettyCounter(grandTotalKeys),
		buildPhase1Prefix(fileIdx, fileTotal, processedKeys+ki, grandTotalKeys)))

	return delta, pct, ki, nil
}

// commitmentFileSizeDelta returns (origSize-newSize) and that delta as a
// percentage of origSize. Positive values mean the new file is smaller.
// Returns int64 because unsqueeze grows the file, producing a negative delta;
// uint64 (datasize.ByteSize) would underflow to a huge positive value.
func commitmentFileSizeDelta(origPath, newPath string) (int64, float32, error) {
	oi, err := os.Stat(origPath)
	if err != nil {
		return 0, 0, err
	}
	ni, err := os.Stat(newPath)
	if err != nil {
		return 0, 0, err
	}
	delta := oi.Size() - ni.Size()
	var pct float32
	if oi.Size() > 0 {
		pct = 100.0 * float32(delta) / float32(oi.Size())
	}
	return delta, pct, nil
}

// signedByteSizeHR formats a signed byte count with a leading "-" when negative,
// reusing datasize's human-readable rendering on the absolute value.
func signedByteSizeHR(n int64) string {
	if n < 0 {
		return "-" + datasize.ByteSize(-n).HR()
	}
	return datasize.ByteSize(n).HR()
}

// ConvertCommitmentFiles re-encodes every commitment .kv file in the datadir to
// the target (squeeze, nibblesV2) state described by opts. It runs five
// sequential phases:
//
//  1. Convert all: detect each file's current state, transform key+value as
//     needed, and emit the result into <datadir>/snapshots/rebuild/domain/.
//     Originals in snapshots/domain/ are untouched. Files already in target
//     state are skipped (no output).
//  2. Pre-swap check: verify every converted file produced its full set of
//     accessor siblings (.kvi / .bt / .kvei per the commitment domain config).
//  3. Backup originals: mkdir snapshots/backup/domains/ and mv every related
//     original (.kv plus its accessor and .torrent siblings) into it.
//  4. Promote: mv snapshots/rebuild/domain/* into snapshots/domain/, then
//     rmdir the rebuild tree.
//  5. Reload aggregator + emit revert instruction: agg.ReloadFiles() drops
//     mmap handles on the now-gone originals and rescans snapshots/domain/.
//
// Phase 1 must complete for every file before Phase 2 begins — no per-file
// interleaving with backup/promote. If anything fails in Phase 1 the caller
// recovers by deleting snapshots/rebuild/; nothing in snapshots/domain/ has
// been touched yet.
//
// IMPORTANT: agg.ReloadFiles() in Phase 5 invalidates the AggregatorRoTx
// passed in via at. The caller must not use at after a successful return; it
// should rollback its transaction and open a fresh one against the reloaded
// aggregator if it needs to keep working.
//
// Pre-flight refuses to run if snapshots/backup/domains/ is non-empty (a
// prior conversion's backup is still in place) and wipes a leftover
// snapshots/rebuild/domain/ from a prior crashed run.
func ConvertCommitmentFiles(ctx context.Context, at *AggregatorRoTx, opts ConvertOpts, logger log.Logger) error {
	// at.Files mixes domain .kv with history .v / .ef when commitment history is
	// enabled. The converter only handles .kv files; filter the rest out.
	allFiles := at.Files(kv.CommitmentDomain)
	files := make(VisibleFiles, 0, len(allFiles))
	for _, f := range allFiles {
		if strings.HasSuffix(f.Fullpath(), ".kv") {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		logger.Info("[commitment_convert] no commitment files to convert")
		return nil
	}

	dirs := at.Dirs()
	rebuildDir := filepath.Join(dirs.Snap, "rebuild", "domain")
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")

	if err := preflightBackupDir(backupDir); err != nil {
		return err
	}
	if err := preflightRebuildDir(rebuildDir, logger); err != nil {
		return err
	}

	if err := os.MkdirAll(rebuildDir, 0o755); err != nil {
		return fmt.Errorf("[commitment_convert] mkdir rebuild dir %s: %w", rebuildDir, err)
	}

	// Pre-pass: sum total key count across every input file so phase 1 can
	// render a keys-overall percentage. KeyCountInFiles is cheap (no I/O —
	// just sums cached decompressor.Count() values), so this loop costs O(files).
	var grandTotalKeys uint64
	for _, f := range files {
		grandTotalKeys += at.KeyCountInFiles(kv.CommitmentDomain, f.StartRootNum(), f.EndRootNum())
	}

	// Phase 1: convert all.
	phaseStart := time.Now()
	processedFiles, skippedFiles, totalSizeDelta, processedKeys, err := convertPhase1(ctx, at, files, rebuildDir, opts, grandTotalKeys, logger)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf(
		"[commitment_convert] phase 1 complete: converted %d, skipped %d, total %d, keys=%s in %s, sizeDelta=%s",
		processedFiles, skippedFiles, len(files),
		common.PrettyCounter(processedKeys),
		time.Since(phaseStart).Round(time.Second), signedByteSizeHR(totalSizeDelta)))

	if processedFiles == 0 {
		if rmErr := dir.RemoveAll(rebuildDir); rmErr != nil {
			logger.Warn("[commitment_convert] failed to remove empty rebuild dir", "path", rebuildDir, "err", rmErr)
		}
		cleanupParentIfEmpty(filepath.Dir(rebuildDir), logger)
		logger.Info("[commitment_convert] no files needed conversion; no backup or promote performed")
		return nil
	}

	// Phase 2: pre-swap check — identify converted files and verify every
	// expected accessor sibling landed in rebuildDir.
	convertedFiles, err := convertPhase2(at, files, rebuildDir)
	if err != nil {
		return err
	}
	if len(convertedFiles) != processedFiles {
		return fmt.Errorf("[commitment_convert] phase 2 mismatch: %d converted, %d found in rebuild dir",
			processedFiles, len(convertedFiles))
	}
	logger.Info(fmt.Sprintf("[commitment_convert] phase 2 pre-swap check: %d files ok", len(convertedFiles)))

	// Phase 3: backup originals.
	movedToBackup, err := convertPhase3(dirs.SnapDomain, backupDir, convertedFiles, at.StepSize())
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("[commitment_convert] phase 3 backup: %d files moved to %s",
		movedToBackup, backupDir))

	// Phase 4: promote rebuilt files into snapshots/domain/.
	promoted, err := convertPhase4(rebuildDir, dirs.SnapDomain)
	if err != nil {
		return err
	}
	if rmErr := dir.RemoveAll(rebuildDir); rmErr != nil {
		logger.Warn("[commitment_convert] failed to remove empty rebuild dir", "path", rebuildDir, "err", rmErr)
	}
	cleanupParentIfEmpty(filepath.Dir(rebuildDir), logger)
	logger.Info(fmt.Sprintf("[commitment_convert] phase 4 promote: %d files moved to %s",
		promoted, dirs.SnapDomain))

	// Phase 5: reload aggregator.
	if reloadErr := at.a.ReloadFiles(); reloadErr != nil {
		return fmt.Errorf("[commitment_convert] phase 5 ReloadFiles: %w", reloadErr)
	}

	logger.Info(fmt.Sprintf(
		"[commitment_convert] DONE. converted %d files. Originals preserved at:\n    %s\nTo restore originals: re-run with --restore",
		processedFiles, backupDir))
	return nil
}

// RestoreCommitmentFiles undoes ConvertCommitmentFiles by moving every file
// from snapshots/backup/domains/ back into snapshots/domain/. Before each
// rename it sweeps the destination of any orphaned siblings that share the
// backup file's step range — necessary because the converter may have produced
// accessor types that don't exist in the backup (e.g. converted .kvei when the
// original had only .kvi, or a newer version-prefix on the .kv).
//
// Refuses if snapshots/backup/domains/ is missing or empty (errors out so the
// operator notices). On success the now-empty backup dir is removed, the
// snapshots/backup/ parent is cleaned if also empty, and the caller is told to
// restart erigon to pick up the restored files.
func RestoreCommitmentFiles(ctx context.Context, at *AggregatorRoTx, logger log.Logger) error {
	dirs := at.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	snapDomain := dirs.SnapDomain

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("[commitment_convert] no backup to restore from at %s", backupDir)
		}
		return fmt.Errorf("[commitment_convert] restore: read backup dir %s: %w", backupDir, err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("[commitment_convert] no backup to restore from at %s (empty)", backupDir)
	}

	type stepRange struct{ from, to string }
	ranges := make(map[stepRange]struct{})
	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := commitmentStepRangeRe.FindStringSubmatch(e.Name())
		if m == nil {
			return fmt.Errorf("[commitment_convert] restore: backup entry %q does not match commitment step-range pattern", e.Name())
		}
		ranges[stepRange{m[1], m[2]}] = struct{}{}
		files = append(files, e.Name())
	}

	// Orphan sweep: remove every *-commitment.<from>-<to>.* in snapshots/domain/
	// for each step-range covered by the backup. Naive rename-overwrite would
	// leave cross-version accessor siblings behind (e.g. converted .kvei when
	// the original had only .kvi), breaking erigon startup against the
	// restored files.
	swept := 0
	for r := range ranges {
		pattern := filepath.Join(snapDomain, fmt.Sprintf("*-commitment.%s-%s.*", r.from, r.to))
		matches, globErr := filepath.Glob(pattern)
		if globErr != nil {
			return fmt.Errorf("[commitment_convert] restore: glob %s: %w", pattern, globErr)
		}
		for _, m := range matches {
			if rmErr := dir.RemoveFile(m); rmErr != nil {
				return fmt.Errorf("[commitment_convert] restore: rm orphan %s: %w", m, rmErr)
			}
			swept++
		}
	}

	moved := 0
	for _, name := range files {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		src := filepath.Join(backupDir, name)
		dst := filepath.Join(snapDomain, name)
		if renameErr := os.Rename(src, dst); renameErr != nil {
			return fmt.Errorf("[commitment_convert] restore mv %s -> %s: %w", src, dst, renameErr)
		}
		moved++
	}

	if rmErr := dir.RemoveAll(backupDir); rmErr != nil {
		logger.Warn("[commitment_convert] restore: failed to remove empty backup dir", "path", backupDir, "err", rmErr)
	}
	cleanupParentIfEmpty(filepath.Dir(backupDir), logger)

	logger.Info(fmt.Sprintf("[commitment_convert] restore complete: restored %d files from %s to %s (swept %d orphans); restart erigon",
		moved, backupDir, snapDomain, swept))
	return nil
}

// preflightBackupDir refuses to start if the backup dir already exists with
// content. A non-empty backup means a prior conversion's originals are still
// there and a second pass would silently overwrite them.
func preflightBackupDir(backupDir string) error {
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("[commitment_convert] pre-flight: check backup dir %s: %w", backupDir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf(
			"[commitment_convert] pre-flight: backup dir %s already exists with %d entries; "+
				"refuse to overwrite a prior conversion's backup (rm -rf %s and retry)",
			backupDir, len(entries), backupDir)
	}
	return nil
}

// preflightRebuildDir wipes a leftover rebuild dir from a prior crashed run.
// A crashed Phase 1 leaves partial outputs in rebuildDir; deleting them is
// safe because snapshots/domain/ was never touched.
func preflightRebuildDir(rebuildDir string, logger log.Logger) error {
	entries, err := os.ReadDir(rebuildDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("[commitment_convert] pre-flight: check rebuild dir %s: %w", rebuildDir, err)
	}
	if len(entries) > 0 {
		logger.Info("[commitment_convert] pre-flight: wiping leftover rebuild dir",
			"path", rebuildDir, "entries", len(entries))
	}
	if rmErr := dir.RemoveAll(rebuildDir); rmErr != nil {
		return fmt.Errorf("[commitment_convert] pre-flight: wipe rebuild dir %s: %w", rebuildDir, rmErr)
	}
	return nil
}

// convertPhase1 runs convertCommitmentFile against every visible commitment
// file in sequence, writing outputs into rebuildDir. errSkip is caught
// (counted as skipped, not as error); any other error aborts.
//
// `grandTotalKeys` is the pre-computed sum of key counts across every input
// file (computed by the caller via a single pass over at.KeyCountInFiles).
// It is used to render the keys-overall percentage in the trailing progress
// suffix. Locally-tracked `processedKeys` advances on both successful
// completion (by `ki`) and on `errSkip` (by the file's key count) so the
// suffix converges to 100% when every file is processed-or-skipped.
func convertPhase1(
	ctx context.Context,
	at *AggregatorRoTx,
	files VisibleFiles,
	rebuildDir string,
	opts ConvertOpts,
	grandTotalKeys uint64,
	logger log.Logger,
) (processedFiles, skippedFiles int, totalSizeDelta int64, processedKeys uint64, err error) {
	N := len(files)
	for i, f := range files {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return processedFiles, skippedFiles, totalSizeDelta, processedKeys, ctxErr
		}
		delta, _, ki, convErr := convertCommitmentFile(ctx, at, f, rebuildDir, opts, i+1, N, grandTotalKeys, processedKeys, logger)
		if errors.Is(convErr, errSkip) {
			skippedFiles++
			processedKeys += at.KeyCountInFiles(kv.CommitmentDomain, f.StartRootNum(), f.EndRootNum())
			logger.Info(fmt.Sprintf("[commitment_convert] phase 1 skip %s (already in target state) %s",
				filepath.Base(f.Fullpath()),
				buildPhase1Prefix(i+1, N, processedKeys, grandTotalKeys)))
			continue
		}
		if convErr != nil {
			return processedFiles, skippedFiles, totalSizeDelta, processedKeys, fmt.Errorf(
				"[commitment_convert] phase 1 file %s: %w (cleanup: rm -rf %s)",
				f.Fullpath(), convErr, rebuildDir)
		}
		processedKeys += ki
		processedFiles++
		totalSizeDelta += delta
	}
	return processedFiles, skippedFiles, totalSizeDelta, processedKeys, nil
}

// convertPhase2 walks the visible commitment files and returns the subset that
// produced output in rebuildDir, verifying that every expected accessor
// sibling (per the commitment domain's Accessors config) is also present.
func convertPhase2(at *AggregatorRoTx, files VisibleFiles, rebuildDir string) ([]VisibleFile, error) {
	stepSize := at.StepSize()
	commitmentRo := at.d[kv.CommitmentDomain]
	d := commitmentRo.d
	converted := make([]VisibleFile, 0, len(files))
	for _, f := range files {
		stepFrom := kv.Step(f.StartRootNum() / stepSize)
		stepTo := kv.Step(f.EndRootNum() / stepSize)
		newKVPath := d.kvNewFilePathIn(rebuildDir, stepFrom, stepTo)
		if _, statErr := os.Stat(newKVPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("[commitment_convert] phase 2 stat %s: %w", newKVPath, statErr)
		}
		// Per-config accessor siblings must also exist.
		if d.Accessors.Has(statecfg.AccessorHashMap) {
			p := d.kviAccessorNewFilePathIn(rebuildDir, stepFrom, stepTo)
			if _, statErr := os.Stat(p); statErr != nil {
				return nil, fmt.Errorf("[commitment_convert] phase 2 missing .kvi at %s: %w", p, statErr)
			}
		}
		if d.Accessors.Has(statecfg.AccessorBTree) {
			p := d.kvBtAccessorNewFilePathIn(rebuildDir, stepFrom, stepTo)
			if _, statErr := os.Stat(p); statErr != nil {
				return nil, fmt.Errorf("[commitment_convert] phase 2 missing .bt at %s: %w", p, statErr)
			}
		}
		if d.Accessors.Has(statecfg.AccessorExistence) {
			p := d.kvExistenceIdxNewFilePathIn(rebuildDir, stepFrom, stepTo)
			if _, statErr := os.Stat(p); statErr != nil {
				return nil, fmt.Errorf("[commitment_convert] phase 2 missing .kvei at %s: %w", p, statErr)
			}
		}
		converted = append(converted, f)
	}
	return converted, nil
}

// convertPhase3 moves the original .kv, accessor siblings, and torrent siblings
// for every converted file from snapshots/domain/ into backupDir. The glob
// pattern matches every file whose basename is "*-commitment.<from>-<to>.*",
// covering .kv / .bt / .kvi / .kvei plus all four .torrent variants.
func convertPhase3(snapDomain, backupDir string, convertedFiles []VisibleFile, stepSize uint64) (int, error) {
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return 0, fmt.Errorf("[commitment_convert] phase 3 mkdir backup %s: %w", backupDir, err)
	}
	moved := 0
	for _, f := range convertedFiles {
		stepFrom := kv.Step(f.StartRootNum() / stepSize)
		stepTo := kv.Step(f.EndRootNum() / stepSize)
		pattern := filepath.Join(snapDomain, fmt.Sprintf("*-commitment.%d-%d.*", stepFrom, stepTo))
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return moved, fmt.Errorf("[commitment_convert] phase 3 glob %s: %w", pattern, err)
		}
		for _, src := range matches {
			dst := filepath.Join(backupDir, filepath.Base(src))
			if renameErr := os.Rename(src, dst); renameErr != nil {
				return moved, fmt.Errorf("[commitment_convert] phase 3 mv %s -> %s: %w",
					src, dst, renameErr)
			}
			moved++
		}
	}
	return moved, nil
}

// convertPhase4 moves every regular file in rebuildDir into snapDomain. The
// caller deletes rebuildDir afterwards.
func convertPhase4(rebuildDir, snapDomain string) (int, error) {
	entries, err := os.ReadDir(rebuildDir)
	if err != nil {
		return 0, fmt.Errorf("[commitment_convert] phase 4 read rebuild dir %s: %w", rebuildDir, err)
	}
	moved := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		src := filepath.Join(rebuildDir, e.Name())
		dst := filepath.Join(snapDomain, e.Name())
		if renameErr := os.Rename(src, dst); renameErr != nil {
			return moved, fmt.Errorf("[commitment_convert] phase 4 mv %s -> %s: %w",
				src, dst, renameErr)
		}
		moved++
	}
	return moved, nil
}

// cleanupParentIfEmpty removes the parent dir if it's empty (the child has
// just been cleared). Failure is logged but non-fatal — a stray empty
// directory is harmless.
func cleanupParentIfEmpty(parent string, logger log.Logger) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	if len(entries) != 0 {
		return
	}
	if rmErr := dir.RemoveAll(parent); rmErr != nil {
		logger.Warn("[commitment_convert] failed to remove empty parent dir", "path", parent, "err", rmErr)
	}
}

func sampleViaStride(reader *seg.Reader, fi *FilesItem, samples int) ([]sampledPair, error) {
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
