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
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func CheckKvis(tx kv.TemporalTx, domain kv.Domain, failFast bool, logger log.Logger) error {
	aggTx := state.AggTx(tx)
	files := aggTx.Files(domain)
	var kvCompression seg.FileCompression
	switch domain {
	case kv.CommitmentDomain:
		kvCompression = statecfg.Schema.CommitmentDomain.Compression
	default:
		panic(fmt.Sprintf("add compression for domain to CheckKvis: %s", domain))
	}
	var kvis []state.VisibleFile
	for _, file := range files {
		if strings.HasSuffix(file.Fullpath(), ".kvi") {
			kvis = append(kvis, file)
		}
	}
	slices.SortFunc(kvis, func(a, b state.VisibleFile) int {
		return cmp.Compare(a.EndRootNum(), b.EndRootNum())
	})
	var integrityErr error
	for _, kvi := range kvis {
		err := CheckKvi(kvi.Fullpath(), kvCompression, failFast, logger)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrIntegrity) {
			integrityErr = AccumulateIntegrityError(integrityErr, err)
			continue
		}
		return err
	}
	return integrityErr
}

func CheckKvi(kviFilePath string, kvCompression seg.FileCompression, failFast bool, logger log.Logger) error {
	logger.Trace("checking kvi", "file", kviFilePath)
	kvi, err := recsplit.OpenIndex(kviFilePath)
	if err != nil {
		return err
	}
	defer kvi.Close()
	kviReader := kvi.GetReaderFromPool()
	kvFilePath, ok := strings.CutSuffix(kviFilePath, "i")
	if !ok {
		return fmt.Errorf("invalid kvi file name: %s", kviFilePath)
	}
	kvDecompressor, err := seg.NewDecompressor(kvFilePath)
	if err != nil {
		return err
	}
	defer kvDecompressor.Close()
	kvReader := seg.NewReader(kvDecompressor.MakeGetter(), kvCompression)
	var keyBuf []byte
	var keyOffset uint64
	var integrityErr error
	if uint64(kvReader.Count()) != kvi.KeyCount() {
		err = fmt.Errorf("kv key count %d != kvi key count %d in %s", kvReader.Count(), kvi.KeyCount(), kviFilePath)
		if failFast {
			return err
		} else {
			logger.Warn(err.Error())
			integrityErr = AccumulateIntegrityError(integrityErr, err)
		}
	}
	for kvReader.HasNext() {
		keyBuf, keyOffset = kvReader.Next(keyBuf[:0])
		logger.Trace("checking kvi for", "key", keyBuf, "offset", keyOffset)
		kviOffset, ok := kviReader.Lookup(keyBuf)
		if !ok {
			err = fmt.Errorf("key %x not found in %s", keyBuf, kviFilePath)
			if failFast {
				return err
			} else {
				logger.Warn(err.Error())
				integrityErr = AccumulateIntegrityError(integrityErr, err)
				continue
			}
		}
		if kviOffset != keyOffset {
			err = fmt.Errorf("key %x offset mismatch in %s: %d != %d", keyBuf, kviFilePath, keyOffset, kviOffset)
			if failFast {
				return err
			} else {
				logger.Warn(err.Error())
				integrityErr = AccumulateIntegrityError(integrityErr, err)
			}
		}
	}
	return integrityErr
}
