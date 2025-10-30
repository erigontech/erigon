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
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

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

func CheckKvis(tx kv.TemporalTx, domain kv.Domain, failFast bool, logger log.Logger) error {
	start := time.Now()
	aggTx := state.AggTx(tx)
	files := aggTx.Files(domain)
	var kvCompression seg.FileCompression
	switch domain {
	case kv.CommitmentDomain:
		kvCompression = statecfg.Schema.CommitmentDomain.Compression
	case kv.StorageDomain:
		kvCompression = statecfg.Schema.StorageDomain.Compression
	case kv.AccountsDomain:
		kvCompression = statecfg.Schema.AccountsDomain.Compression
	case kv.CodeDomain:
		kvCompression = statecfg.Schema.CodeDomain.Compression
	default:
		panic(fmt.Sprintf("add compression for domain to CheckKvis: %s", domain))
	}
	var integrityErr error
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
		err = CheckKvi(kviPath, kvPath, kvCompression, failFast, logger)
		if err == nil {
			continue
		}
		if failFast {
			return err
		}
		log.Warn(err.Error())
		integrityErr = fmt.Errorf("%s: %w", ErrIntegrity, err)
	}
	logger.Info("checked kvi files in", "dur", time.Since(start), "files", len(files))
	return integrityErr
}

func CheckKvi(kviPath string, kvPath string, kvCompression seg.FileCompression, failFast bool, logger log.Logger) error {
	kviFileName := filepath.Base(kviPath)
	kvFileName := filepath.Base(kvPath)
	logger.Info("checking kvi", "kvi", kviFileName, "kv", kvFileName)
	start := time.Now()
	kvi, err := recsplit.OpenIndex(kviPath)
	if err != nil {
		return err
	}
	defer kvi.Close()
	kviReader := kvi.GetReaderFromPool()
	kvDecompressor, err := seg.NewDecompressor(kvPath)
	if err != nil {
		return err
	}
	defer kvDecompressor.Close()
	kvReader := seg.NewReader(kvDecompressor.MakeGetter(), kvCompression)
	var integrityErr error
	if kvKeyCount := uint64(kvReader.Count()) / 2; kvKeyCount != kvi.KeyCount() {
		err = fmt.Errorf("kv key count %d != kvi key count %d in %s", kvKeyCount, kvi.KeyCount(), kviFileName)
		if failFast {
			return err
		}
		logger.Warn(err.Error())
		integrityErr = fmt.Errorf("%s: %w", ErrIntegrity, err)
	}
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()
	var keyBuf []byte
	var keyOffset, keyCount uint64
	var atValue bool
	for kvReader.HasNext() {
		if keyCount == 10_000 {
			time.Sleep(15 * time.Second)
		}
		select {
		case <-logTicker.C:
			rate := float64(keyCount) / time.Since(start).Seconds()
			logger.Info("checking kvi progress", "key", keyCount, "keys", kvi.KeyCount(), "k/s", rate, "kvi", kviFileName)
		default: // proceed
		}
		if atValue {
			keyOffset, _ = kvReader.Skip()
			atValue = false
			continue
		}
		keyBuf, _ = kvReader.Next(keyBuf[:0])
		logger.Trace("checking kvi for", "key", hex.EncodeToString(keyBuf), "offset", keyOffset, "kvi", kviFileName)
		keyCount++
		atValue = true
		kviOffset, ok := kviReader.Lookup(keyBuf)
		if !ok {
			err = fmt.Errorf("key %x not found in %s", keyBuf, kviFileName)
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%s: %w", ErrIntegrity, err)
			continue
		}
		if kviOffset != keyOffset {
			err = fmt.Errorf("key %x offset mismatch %d != %d in %s", keyBuf, keyOffset, kviOffset, kviFileName)
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%s: %w", ErrIntegrity, err)
		}
	}
	duration := time.Since(start)
	rate := float64(keyCount) / duration.Seconds()
	logger.Info("checked kvi in", "dur", duration, "keys", keyCount, "k/s", rate, "kvi", kviFileName, "kv", kvFileName)
	return integrityErr
}
