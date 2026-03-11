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
				return nil
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
