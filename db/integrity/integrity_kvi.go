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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dbg"
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

func CheckKvis(ctx context.Context, tx kv.TemporalTx, domain kv.Domain, failFast bool, logger log.Logger) error {
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
	var integrityErr error
	var keyCount atomic.Uint64
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
		eg.Go(func() error {
			keys, err := CheckKvi(ctx, kviPath, kvPath, kvCompression, failFast, logger)
			if err == nil {
				keyCount.Add(keys)
				return nil
			}
			if !failFast {
				logger.Warn(err.Error())
			}
			return err
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	logger.Info("checked kvi files in", "dur", time.Since(start), "files", len(files), "keys", keyCount.Load())
	return integrityErr
}

func CheckKvi(ctx context.Context, kviPath string, kvPath string, kvCompression seg.FileCompression, failFast bool, logger log.Logger) (uint64, error) {
	kviFileName := filepath.Base(kviPath)
	kvFileName := filepath.Base(kvPath)
	logger.Info("checking kvi", "kvi", kviFileName, "kv", kvFileName)
	start := time.Now()
	kvi, err := recsplit.OpenIndex(kviPath)
	if err != nil {
		return 0, err
	}
	defer kvi.Close()
	kvi.MadvSequential()
	kviReader := kvi.GetReaderFromPool()
	kvDecompressor, err := seg.NewDecompressor(kvPath)
	if err != nil {
		return 0, err
	}
	defer kvDecompressor.Close()
	kvDecompressor.MadvSequential()
	kvReader := seg.NewReader(kvDecompressor.MakeGetter(), kvCompression)
	var integrityErr error
	if kvKeyCount := uint64(kvReader.Count()) / 2; kvKeyCount != kvi.KeyCount() {
		err = fmt.Errorf("kv key count %d != kvi key count %d in %s", kvKeyCount, kvi.KeyCount(), kviFileName)
		if failFast {
			return 0, err
		}
		logger.Warn(err.Error())
		integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
	}
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()
	var keyBuf []byte
	var keyOffset, keyCount uint64
	var atValue bool
	for kvReader.HasNext() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-logTicker.C:
			at := fmt.Sprintf("%d/%d", keyCount, kvi.KeyCount())
			percent := fmt.Sprintf("%.1f%%", float64(keyCount)/float64(kvi.KeyCount())*100)
			rate := float64(keyCount) / time.Since(start).Seconds()
			eta := time.Duration(float64(kvi.KeyCount()-keyCount)/rate) * time.Second
			logger.Info("checking kvi progress", "at", at, "p", percent, "k/s", rate, "eta", eta, "kvi", kviFileName)
		default: // proceed
		}
		if atValue {
			keyOffset, _ = kvReader.Skip()
			atValue = false
			continue
		}
		keyBuf, _ = kvReader.Next(keyBuf[:0])
		if logger.Enabled(ctx, log.LvlTrace) {
			logger.Trace("checking kvi for", "key", hex.EncodeToString(keyBuf), "offset", keyOffset, "kvi", kviFileName)
		}
		keyCount++
		atValue = true
		kviOffset, ok := kviReader.Lookup(keyBuf)
		if !ok {
			err = fmt.Errorf("key %x not found in %s", keyBuf, kviFileName)
			if failFast {
				return 0, err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			continue
		}
		if kviOffset != keyOffset {
			err = fmt.Errorf("key %x offset mismatch %d != %d in %s", keyBuf, keyOffset, kviOffset, kviFileName)
			if failFast {
				return 0, err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
		}
	}
	duration := time.Since(start)
	rate := float64(keyCount) / duration.Seconds()
	logger.Info("checked kvi in", "dur", duration, "keys", keyCount, "k/s", rate, "kvi", kviFileName, "kv", kvFileName)
	return keyCount, integrityErr
}
