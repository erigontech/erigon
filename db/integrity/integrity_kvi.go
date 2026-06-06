// Copyright 2025 The Erigon Authors
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

package integrity

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// ErrIntegrity is useful to differentiate integrity errors from program errors.
var ErrIntegrity = errors.New("integrity error")

// Specific commitment-validation error classes. All wrap ErrIntegrity so
// `errors.Is(err, ErrIntegrity)` keeps working for the integrity tool's
// existing classifier. Callers (publisher / consumer validators) MUST
// branch on these explicitly — different classes warrant different
// handling (pause vs skip vs hard-fail vs quarantine).
//
// The validator strategy assumes three phases gated by what's loaded:
//
//	Phase A: read the file's KeyCommitmentState record. Always works
//	         (the file is on disk). Failures here → ErrCommitmentRecordInvalid.
//
//	Phase B: cross-check ProofRoot against header.Root for AtBlock.
//	         Always works under all prune modes — headers are kept
//	         back to zero (see db/snapshotsync/preverified_filter.go).
//	         Failures here → ErrCommitmentHeaderMismatch (real corruption)
//	         or ErrAnchorHeaderMissing (header not yet loaded).
//
//	Phase C: cross-check recorded TxNum against the block's txnum
//	         range. Requires the body for AtBlock — under minimal
//	         mode bodies < prune horizon are intentionally absent.
//	         Failures here → ErrCommitmentTxNumRange (real mismatch)
//	         or ErrAnchorBodyMissing (body unavailable).
//
// Callers map ErrAnchorBodyMissing to either skip-validate (file's
// anchor is in the pruned range — Phase C can never run) or pause
// (anchor is post-horizon, body is still downloading).
var (
	// ErrCommitmentRecordInvalid: the file's KeyCommitmentState record
	// couldn't be decoded, has empty rootHash, has txNum=0, or has
	// txNum outside the file's [startTxNum, endTxNum) range. Real
	// file corruption — quarantine.
	ErrCommitmentRecordInvalid = fmt.Errorf("%w: commitment record invalid", ErrIntegrity)

	// ErrCommitmentRangeMismatch: GetLatestFromFiles returned the
	// KeyCommitmentState record from a file whose [start,end) range
	// does NOT match the queried step range — the record came from a
	// wider, merged file. This is the merge-transition signature: the
	// small step file being validated has just been superseded by a
	// merge (its 'state' now lives in the merged file). Not corruption
	// — transient. Lifecycle callers pause; the stale step entry is
	// dropped by the next disk-reconcile sweep.
	ErrCommitmentRangeMismatch = fmt.Errorf("%w: commitment file range mismatch", ErrIntegrity)

	// ErrAnchorHeaderMissing: header for the recorded AtBlock isn't
	// in the local BlockReader yet. Transient (the EL hasn't opened
	// the segment containing it) — caller should pause and retry.
	// Shouldn't happen at steady-state under any prune mode since
	// headers are kept for the full chain.
	ErrAnchorHeaderMissing = fmt.Errorf("%w: anchor header missing", ErrIntegrity)

	// ErrAnchorBodyMissing: body for the recorded AtBlock isn't
	// available. Under minimal mode (or any --prune.mode that prunes
	// bodies below a horizon) this is INTENTIONAL for blocks below
	// the horizon — callers should detect via prune.Mode + AtBlock
	// and skip Phase C with a logged warning. Above the horizon this
	// is transient — caller should pause.
	ErrAnchorBodyMissing = fmt.Errorf("%w: anchor body missing", ErrIntegrity)

	// ErrCommitmentHeaderMismatch: the file's ProofRoot doesn't
	// match the chain header's stateRoot for AtBlock. Real
	// corruption (or the file is from a fork) — quarantine.
	ErrCommitmentHeaderMismatch = fmt.Errorf("%w: ProofRoot != header.Root", ErrIntegrity)

	// ErrCommitmentTxNumRange: the recorded TxNum lies outside the
	// recorded AtBlock's [minTxNum, maxTxNum]. Means the (blockNum,
	// txNum) pair the file claims is internally inconsistent against
	// the canonical txnum index. Quarantine.
	ErrCommitmentTxNumRange = fmt.Errorf("%w: txNum outside AtBlock's txnum range", ErrIntegrity)

	// ErrCommitmentReplayMismatch: replaying the trie forward from a
	// partial-block (mid-block) commitment file's recorded txNum to
	// the end of AtBlock did NOT reproduce the canonical
	// header.StateRoot. The file's `KeyCommitmentState` (its recorded
	// root) may match canonical, but its interior BranchData entries
	// are inconsistent — the trie's unfold path reads the corrupt
	// children during downstream replay and diverges. Real corruption;
	// quarantine.
	//
	// For partial-block files VerifyCommitmentMatchesHeader cannot run
	// directly (the consensus anchor for the mid-block state itself
	// doesn't exist), but AtBlock's *end* has one. A correctly-built
	// file must reach it via forward replay.
	ErrCommitmentReplayMismatch = fmt.Errorf("%w: replay to AtBlock end != header.Root", ErrIntegrity)
)

// CheckKvis checks all kvi index files for a domain sequentially (one file at a time),
// parallelizing the lookup work inside each file.
func CheckKvis(ctx context.Context, tx kv.TemporalTx, domain kv.Domain, checkType Check, sc SamplerCfg, cache *IntegrityCache, failFast bool, logger log.Logger) error {
	start := time.Now()
	aggTx := state.AggTx(tx)
	files := aggTx.Files(domain)
	kvCompression := statecfg.Schema.GetDomainCfg(domain).Compression
	var eg *errgroup.Group
	if failFast {
		// if 1 goroutine fails, fail others
		eg, ctx = errgroup.WithContext(ctx)
	} else {
		eg = &errgroup.Group{}
	}
	if dbg.EnvBool("CHECK_KVIS_SEQUENTIAL", false) {
		eg.SetLimit(1)
	}

	type workItem struct {
		kvPath  string
		kviPath string
		fps     []fileFingerprint // nil when cache is disabled
	}
	var works []workItem
	for _, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		kvPath := file.Fullpath()
		kviPath := kvPath + "i"
		var err error
		kviPath, err = version.ReplaceVersionWithMask(kviPath)
		if err != nil {
			return err
		}
		var ok bool
		kviPath, _, ok, err = version.FindFilesWithVersionsByPattern(kviPath)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("kvi not found for %s", kvPath)
		}
		var fps []fileFingerprint
		if cache != nil {
			fps, err = fingerprintsOf(kvPath, kviPath)
			if err != nil {
				return err
			}
			if cache.has(string(checkType), fps) {
				logger.Info("skipping (cache hit)", "kv", filepath.Base(kvPath))
				continue
			}
		}
		works = append(works, workItem{kvPath, kviPath, fps})
	}

	var successMu sync.Mutex
	var successFps [][]fileFingerprint
	var keyCount atomic.Uint64
	for _, w := range works {
		eg.Go(func() error {
			keys, err := CheckKvi(ctx, w.kviPath, w.kvPath, kvCompression, sc, failFast, logger)
			if err == nil {
				keyCount.Add(keys)
				if w.fps != nil {
					successMu.Lock()
					successFps = append(successFps, w.fps)
					successMu.Unlock()
				}
				return nil
			}
			logger.Warn(err.Error())
			return err
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	for _, fps := range successFps {
		cache.add(string(checkType), fps)
	}
	logger.Info("[integrity] "+string(checkType), "dur", time.Since(start), "files", len(files), "keys", keyCount.Load())
	return nil
}

type kviWorkItem struct {
	key    []byte
	offset uint64
}

func CheckKvi(ctx context.Context, kviPath string, kvPath string, kvCompression seg.FileCompression, sc SamplerCfg, failFast bool, logger log.Logger) (uint64, error) {
	kviFileName := filepath.Base(kviPath)
	kvFileName := filepath.Base(kvPath)
	logger.Info("[integrity] CommitmentKvi", "kvi", kviFileName, "kv", kvFileName)
	start := time.Now()
	kvi, err := recsplit.OpenIndex(kviPath)
	if err != nil {
		return 0, err
	}
	defer kvi.Close()
	kvDecompressor, err := seg.NewDecompressor(kvPath)
	if err != nil {
		return 0, err
	}
	defer kvDecompressor.Close()
	kvReader := seg.NewReader(kvDecompressor.MakeGetter(), kvCompression)

	var firstErr error
	if kvKeyCount := uint64(kvReader.Count()) / 2; kvKeyCount != kvi.KeyCount() {
		err = fmt.Errorf("kv key count %d != kvi key count %d in %s", kvKeyCount, kvi.KeyCount(), kviFileName)
		if failFast {
			return 0, err
		}
		logger.Warn(err.Error())
		firstErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
	}

	trace := logger.Enabled(ctx, log.LvlTrace)
	checkOne := func(kviReader *recsplit.IndexReader, work kviWorkItem) error {
		if trace {
			logger.Trace("[integrity] CommitmentKvi", "key", hex.EncodeToString(work.key), "offset", work.offset, "kvi", kviFileName)
		}
		kviOffset, found := kviReader.Lookup(work.key)
		if !found {
			return fmt.Errorf("%w: key %x not found in %s", ErrIntegrity, work.key, kviFileName)
		}
		if kviOffset != work.offset {
			return fmt.Errorf("%w: key %x offset mismatch %d != %d in %s", ErrIntegrity, work.key, work.offset, kviOffset, kviFileName)
		}
		return nil
	}

	var keyCount uint64
	eg, ctx := errgroup.WithContext(ctx)
	numWorkers := estimate.AlmostAllCPUs()
	workCh := make(chan kviWorkItem, numWorkers*4)

	for range numWorkers {
		eg.Go(func() error {
			kviReader := kvi.GetReaderFromPool()
			defer kviReader.Close()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case work, ok := <-workCh:
					if !ok {
						return nil
					}
					if err := checkOne(kviReader, work); err != nil {
						if !failFast {
							logger.Warn(err.Error())
						}
						return err
					}
				}
			}
		})
	}

	// Producer: scan kv file, emit sampled (key, offset) pairs to workers.
	// Unsampled entries use Skip() for both key and value to avoid decompressing key bytes.
	eg.Go(func() error {
		defer close(workCh)
		sampler := sc.NewSampler()
		logTicker := time.NewTicker(30 * time.Second)
		defer logTicker.Stop()
		var keyBuf []byte
		var keyOffset uint64 // byte offset of the current key in the bitstream
		for kvReader.HasNext() {
			// Skip path is hot (full-skip mode loops over every key); amortize the ctx check.
			if keyCount%100000 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if sampler.CanSkip() {
				kvReader.Skip()                // skip key (no decompression of bytes)
				keyOffset, _ = kvReader.Skip() // skip value, advance to next key
				keyCount++
				continue
			}
			keyBuf, _ = kvReader.Next(keyBuf[:0]) // decompress key
			nextOffset, _ := kvReader.Skip()      // skip value
			select {
			case <-ctx.Done():
				return ctx.Err()
			case workCh <- kviWorkItem{key: bytes.Clone(keyBuf), offset: keyOffset}:
			}
			keyOffset = nextOffset
			keyCount++

			select {
			case <-logTicker.C:
				at := fmt.Sprintf("%d/%d", keyCount, kvi.KeyCount())
				percent := fmt.Sprintf("%.1f%%", float64(keyCount)/float64(kvi.KeyCount())*100)
				rate := float64(keyCount) / time.Since(start).Seconds()
				eta := time.Duration(float64(kvi.KeyCount()-keyCount)/rate) * time.Second
				logger.Info("[integrity] CommitmentKvi", "at", at, "p", percent, "k/s", rate, "eta", eta, "kvi", kviFileName)
			default:
			}
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return keyCount, err
	}
	duration := time.Since(start)
	rate := float64(keyCount) / duration.Seconds()
	logger.Info("[integrity] CommitmentKvi", "dur", duration, "keys", keyCount, "k/s", rate, "kvi", kviFileName, "kv", kvFileName)
	return keyCount, firstErr
}
