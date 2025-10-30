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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func CheckCommitmentRoot(ctx context.Context, db kv.TemporalRoDB, failFast bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	aggTx := state.AggTx(tx)
	files := aggTx.Files(kv.CommitmentDomain)
	// atm our older files are missing the root due to purification, so this flag can be used to only check the last file
	onlyCheckLastFile := dbg.EnvBool("CHECK_COMMITMENT_ROOT_ONLY_LAST_FILE", false)
	if onlyCheckLastFile && len(files) > 0 {
		files = files[len(files)-1:]
	}
	var integrityErr error
	for _, file := range files {
		fileName := filepath.Base(file.Fullpath())
		startTxNum := file.StartRootNum()
		endTxNum := file.EndRootNum()
		maxTxNum := endTxNum - 1
		logger.Info("checking commitment root in", "file", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)
		v, ok, start, end, err := aggTx.DebugGetLatestFromFiles(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, maxTxNum)
		if err != nil {
			return err
		}
		if !ok {
			err = fmt.Errorf("commitment root not found in %s with startTxNum=%d,endTxNum=%d", fileName, startTxNum, endTxNum)
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			continue
		}
		if start != startTxNum || end != endTxNum {
			err = fmt.Errorf("commitment root found but not in %s startTxNum=%d,endTxNum=%d != (%d,%d)", fileName, startTxNum, endTxNum, start, end)
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			continue
		}
		rootHash, err := commitment.HexTrieExtractStateRoot(v)
		if err != nil {
			err = fmt.Errorf("commitment root in %s with startTxNum=%d,endTxNum=%d could not be extracted: %w", fileName, startTxNum, endTxNum, err)
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			continue
		}
		if common.BytesToHash(rootHash) == (common.Hash{}) {
			err = fmt.Errorf("commitment root in %s with startTxNum=%d,endTxNum=%d is empty", fileName, startTxNum, endTxNum)
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
		}
	}
	return integrityErr
}

func CheckCommitmentKvi(ctx context.Context, db kv.TemporalRoDB, failFast bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return CheckKvis(ctx, tx, kv.CommitmentDomain, failFast, logger)
}

func CheckCommitmentKvDeref(ctx context.Context, db kv.TemporalRoDB, failFast bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	aggTx := state.AggTx(tx)
	files := aggTx.Files(kv.CommitmentDomain)
	var eg *errgroup.Group
	if failFast {
		// if 1 goroutine fails, fail others
		eg, ctx = errgroup.WithContext(ctx)
	} else {
		eg = &errgroup.Group{}
	}
	if dbg.EnvBool("CHECK_COMMITMENT_KVS_DEREF_SEQUENTIAL", false) {
		eg.SetLimit(1)
	}
	var integrityErr error
	var keyCount, derefCount atomic.Uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		eg.Go(func() error {
			keys, derefs, err := checkCommitmentKvDeref(ctx, file, aggTx.StepSize(), failFast, logger)
			if err == nil {
				keyCount.Add(keys)
				derefCount.Add(derefs)
				return nil
			}
			if !failFast {
				logger.Warn(err.Error())
			}
			return err
		})
	}
	err = eg.Wait()
	if err != nil {
		return err
	}
	logger.Info("checked commitment kvs dereference in", "files", len(files), "keys", keyCount.Load(), "derefs", derefCount.Load())
	return integrityErr
}

func checkCommitmentKvDeref(ctx context.Context, file state.VisibleFile, stepSize uint64, failFast bool, logger log.Logger) (uint64, uint64, error) {
	start := time.Now()
	fileName := filepath.Base(file.Fullpath())
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()
	if !state.ValuesPlainKeyReferencingThresholdReached(stepSize, startTxNum, endTxNum) {
		logger.Info("checking commitment defer skipped, file not within threshold", "file", fileName)
		return 0, 0, nil
	}
	logger.Info("checking commitment deref in", "file", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)
	commDecomp, err := seg.NewDecompressor(file.Fullpath())
	if err != nil {
		return 0, 0, err
	}
	defer commDecomp.Close()
	commDecomp.MadvSequential()
	commCompression := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
	commReader := seg.NewReader(commDecomp.MakeGetter(), commCompression)
	accReader, accDecompClose, err := deriveReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.AccountsDomain)
	if err != nil {
		return 0, 0, err
	}
	defer accDecompClose()
	storageReader, storageDecompClose, err := deriveReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.StorageDomain)
	if err != nil {
		return 0, 0, err
	}
	defer storageDecompClose()
	totalKeys := uint64(commDecomp.Count()) / 2
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()
	var branchKeyBuf, branchValueBuf, newBranchValueBuf, plainKeyBuf []byte
	var keyCount, derefCount uint64
	var integrityErr error
	for commReader.HasNext() {
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-logTicker.C:
			at := fmt.Sprintf("%d/%d", keyCount, totalKeys)
			percent := fmt.Sprintf("%.1f%%", float64(keyCount)/float64(totalKeys)*100)
			rate := float64(keyCount) / time.Since(start).Seconds()
			eta := time.Duration(float64(totalKeys-keyCount)/rate) * time.Second
			logger.Info("checking commitment deref progress", "at", at, "p", percent, "k/s", rate, "eta", eta, "derefs", derefCount, "kv", fileName)
		default: // proceed
		}
		branchKey, _ := commReader.Next(branchKeyBuf[:0])
		if !commReader.HasNext() {
			err = errors.New("invalid key/value pair during decompression")
			if failFast {
				return 0, 0, err
			}
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			logger.Warn(err.Error())
		}
		branchValue, _ := commReader.Next(branchValueBuf[:0])
		branchData := commitment.BranchData(branchValue)
		_, err = branchData.ReplacePlainKeys(newBranchValueBuf[:0], func(key []byte, isStorage bool) (newKey []byte, err error) {
			logger.Trace(
				"checking commitment deref for branch",
				"branchKey", hex.EncodeToString(branchKey),
				"key", hex.EncodeToString(key),
				"isStorage", isStorage,
				"file", fileName,
			)
			if isStorage {
				if len(key) == length.Addr+length.Hash {
					return nil, nil // not a referenced key, nothing to check
				}
				offset, err := checkOffsetDeref(key, uint64(storageReader.Size()))
				if err != nil {
					err = fmt.Errorf("storage reference key %x issue for branch %x in %s: %w", key, branchKey, fileName, err)
					if failFast {
						return nil, err
					}
					logger.Warn(err.Error())
					return nil, nil
				}
				storageReader.Reset(offset)
				plainKey, _ := storageReader.Next(plainKeyBuf[:0])
				if len(plainKey) != length.Addr+length.Hash {
					err = fmt.Errorf("storage reference key %x has invalid plainKey for branch %x in %s", key, branchKey, fileName)
					if failFast {
						return nil, err
					}
					logger.Warn(err.Error())
					integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
					return nil, nil
				}
				derefCount++
				return plainKey, nil
			}
			if len(key) == length.Addr {
				return nil, nil // not a referenced key, nothing to check
			}
			offset, err := checkOffsetDeref(key, uint64(accReader.Size()))
			if err != nil {
				err = fmt.Errorf("account reference key %x issue for branch %x in %s: %w", key, branchKey, fileName, err)
				if failFast {
					return nil, err
				}
				logger.Warn(err.Error())
				return nil, nil
			}
			accReader.Reset(offset)
			plainKey, _ := accReader.Next(plainKeyBuf[:0])
			if len(plainKey) != length.Addr {
				err = fmt.Errorf("account reference key %x has invalid plainKey for branch %x in %s", key, branchKey, fileName)
				if failFast {
					return nil, err
				}
				logger.Warn(err.Error())
				integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
				return nil, nil
			}
			derefCount++
			return nil, nil
		})
		if err != nil {
			return 0, 0, err
		}
		keyCount++
	}
	return keyCount, derefCount, integrityErr
}

func deriveReaderForOtherDomain(baseFile string, oldDomain, newDomain kv.Domain) (*seg.Reader, func(), error) {
	fileVersionMask, err := version.ReplaceVersionWithMask(baseFile)
	if err != nil {
		return nil, nil, err
	}
	fileVersionMask = strings.Replace(fileVersionMask, oldDomain.String(), newDomain.String(), 1)
	newFile, _, ok, err := version.FindFilesWithVersionsByPattern(fileVersionMask)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("could not derive reader for other domain due to file not found: %s,%s->%s", baseFile, oldDomain, newDomain)
	}
	decomp, err := seg.NewDecompressor(newFile)
	if err != nil {
		return nil, nil, err
	}
	compression := statecfg.Schema.GetDomainCfg(newDomain).Compression
	return seg.NewReader(decomp.MakeGetter(), compression), decomp.Close, nil
}

func checkOffsetDeref(key []byte, end uint64) (uint64, error) {
	offset, n := binary.Uvarint(key)
	if n <= 0 {
		return 0, errors.New("invalid offset")
	}
	if offset >= end {
		return 0, errors.New("offset out of bounds")
	}
	return offset, nil
}
