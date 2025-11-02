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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
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
		logger.Info("checking commitment root in", "kv", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)
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
	start := time.Now()
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
	var keyCount, accDerefs, accNonDerefs, storageDerefs, storageNonDerefs atomic.Uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		eg.Go(func() error {
			counts, err := checkCommitmentKvDeref(ctx, file, aggTx.StepSize(), failFast, logger)
			if err == nil {
				keyCount.Add(counts.branchKeyCount)
				accDerefs.Add(counts.accDerefCount)
				accNonDerefs.Add(counts.accNonDerefCount)
				storageDerefs.Add(counts.storageDerefCount)
				storageNonDerefs.Add(counts.storageNonDerefCount)
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
	logger.Info(
		"checked commitment kvs dereference in",
		"dur", time.Since(start),
		"files", len(files),
		"keys", keyCount.Load(),
		"accDerefs", accDerefs.Load(),
		"accNonDerefs", accNonDerefs.Load(),
		"storageDerefs", storageDerefs.Load(),
		"storageNonDerefs", storageNonDerefs.Load(),
	)
	return integrityErr
}

type derefCounts struct {
	branchKeyCount       uint64
	accDerefCount        uint64
	accNonDerefCount     uint64
	storageDerefCount    uint64
	storageNonDerefCount uint64
}

func checkCommitmentKvDeref(ctx context.Context, file state.VisibleFile, stepSize uint64, failFast bool, logger log.Logger) (derefCounts, error) {
	start := time.Now()
	fileName := filepath.Base(file.Fullpath())
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()
	if !state.MayContainValuesPlainKeyReferencing(stepSize, startTxNum, endTxNum) {
		logger.Info(
			"checking commitment defer skipped, file not above min",
			"file", fileName,
			"startTxNum", startTxNum,
			"endTxNum", endTxNum,
			"steps", (endTxNum-startTxNum)/stepSize,
		)
		return derefCounts{}, nil
	}
	logger.Info("checking commitment deref in", "kv", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)
	commDecomp, err := seg.NewDecompressor(file.Fullpath())
	if err != nil {
		return derefCounts{}, err
	}
	defer commDecomp.Close()
	commDecomp.MadvSequential()
	commCompression := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
	commReader := seg.NewReader(commDecomp.MakeGetter(), commCompression)
	accReader, accDecompClose, err := deriveReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.AccountsDomain)
	if err != nil {
		return derefCounts{}, err
	}
	defer accDecompClose()
	storageReader, storageDecompClose, err := deriveReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.StorageDomain)
	if err != nil {
		return derefCounts{}, err
	}
	defer storageDecompClose()
	totalKeys := uint64(commDecomp.Count()) / 2
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()
	branchKeyBuf := make([]byte, 0, datasize.MB.Bytes())
	branchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	newBranchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	plainKeyBuf := make([]byte, 0, datasize.MB.Bytes())
	var counts derefCounts
	var integrityErr error
	for commReader.HasNext() {
		select {
		case <-ctx.Done():
			return derefCounts{}, ctx.Err()
		case <-logTicker.C:
			at := fmt.Sprintf("%d/%d", counts.branchKeyCount, totalKeys)
			percent := fmt.Sprintf("%.1f%%", float64(counts.branchKeyCount)/float64(totalKeys)*100)
			rate := float64(counts.branchKeyCount) / time.Since(start).Seconds()
			eta := time.Duration(float64(totalKeys-counts.branchKeyCount)/rate) * time.Second
			logger.Info(
				"checking commitment deref progress",
				"at", at,
				"p", percent,
				"k/s", rate,
				"eta", eta,
				"accDerefs", counts.accDerefCount,
				"accNonDerefs", counts.accNonDerefCount,
				"storageDerefs", counts.storageDerefCount,
				"storageNonDerefs", counts.storageNonDerefCount,
				"kv", fileName,
			)
		default: // proceed
		}
		branchKey, _ := commReader.Next(branchKeyBuf[:0])
		if !commReader.HasNext() {
			err = errors.New("invalid key/value pair during decompression")
			if failFast {
				return derefCounts{}, err
			}
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			logger.Warn(err.Error())
		}
		branchValue, _ := commReader.Next(branchValueBuf[:0])
		if bytes.Equal(branchKey, commitmentdb.KeyCommitmentState) {
			logger.Info("skipping state key", "valueLen", len(branchValue), "file", fileName)
			continue
		}
		branchData := commitment.BranchData(branchValue)
		_, err = branchData.ReplacePlainKeys(newBranchValueBuf[:0], func(key []byte, isStorage bool) (newKey []byte, err error) {
			if logger.Enabled(ctx, log.LvlTrace) {
				logger.Trace(
					"checking commitment deref for branch",
					"branchKey", hex.EncodeToString(branchKey),
					"key", hex.EncodeToString(key),
					"isStorage", isStorage,
					"kv", fileName,
				)
			}
			if isStorage {
				if len(key) == length.Addr+length.Hash {
					if logger.Enabled(ctx, log.LvlTrace) {
						logger.Trace(
							"skipping, not a storage reference",
							"branchKey", hex.EncodeToString(branchKey),
							"addr", common.BytesToAddress(key[:length.Addr]),
							"hash", common.BytesToHash(key[length.Addr:]),
							"kv", fileName,
						)
					}
					counts.storageNonDerefCount++
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
				counts.storageDerefCount++
				if logger.Enabled(ctx, log.LvlTrace) {
					logger.Trace(
						"dereferenced storage key",
						"branchKey", hex.EncodeToString(branchKey),
						"key", hex.EncodeToString(key),
						"offset", offset,
						"addr", common.BytesToAddress(plainKey[:length.Addr]),
						"hash", common.BytesToHash(plainKey[length.Addr:]),
						"kv", fileName,
					)
				}
				return plainKey, nil
			}
			if len(key) == length.Addr {
				if logger.Enabled(ctx, log.LvlTrace) {
					logger.Trace(
						"skipping, not an account reference",
						"branchKey", hex.EncodeToString(branchKey),
						"addr", common.BytesToAddress(key[:length.Addr]),
						"kv", fileName,
					)
				}
				counts.accNonDerefCount++
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
			counts.accDerefCount++
			if logger.Enabled(ctx, log.LvlTrace) {
				logger.Trace(
					"dereferenced account key",
					"branchKey", hex.EncodeToString(branchKey),
					"key", hex.EncodeToString(key),
					"offset", offset,
					"addr", common.BytesToAddress(plainKey),
					"kv", fileName,
				)
			}
			return plainKey, nil
		})
		if err != nil {
			return derefCounts{}, err
		}
		counts.branchKeyCount++
	}
	logger.Info(
		"checked commitment kv dereference in",
		"dur", time.Since(start),
		"keys", counts.branchKeyCount,
		"accDerefs", counts.accDerefCount,
		"accNonDerefs", counts.accNonDerefCount,
		"storageDerefs", counts.storageDerefCount,
		"storageNonDerefs", counts.storageNonDerefCount,
		"kv", fileName,
	)
	return counts, integrityErr
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
