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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func CheckCommitmentRoot(ctx context.Context, db kv.TemporalRoDB, br services.FullBlockReader, failFast bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	aggTx := state.AggTx(tx)
	defer aggTx.Close()
	files := aggTx.Files(kv.CommitmentDomain)
	// atm our older files are missing the root due to purification, so this flag can be used to only check the last file
	onlyCheckLastFile := dbg.EnvBool("CHECK_COMMITMENT_ROOT_ONLY_LAST_FILE", true)
	// may want to check all files for root key presence, but only recompute for the last file (due to purification)
	onlyRecomputeLastFile := dbg.EnvBool("CHECK_COMMITMENT_ROOT_ONLY_LAST_FILE_RECOMPUTE", true)
	if onlyCheckLastFile && len(files) > 0 {
		files = files[len(files)-1:]
	}
	var integrityErr error
	for i, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		recompute := !onlyRecomputeLastFile || i == len(files)-1
		err = checkCommitmentRootInFile(ctx, db, br, file, recompute, logger)
		if err != nil {
			if !errors.Is(err, ErrIntegrity) {
				return err
			}
			if failFast {
				return err
			}
			log.Warn(err.Error())
			integrityErr = err
			continue
		}
	}
	return integrityErr
}

func checkCommitmentRootInFile(ctx context.Context, db kv.TemporalRoDB, br services.FullBlockReader, f state.VisibleFile, recompute bool, logger log.Logger) error {
	tx, err := db.BeginTemporalRo(ctx) // we need separate RoTx per file if we re-compute commitment for more than 1 file
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fileName := filepath.Base(f.Fullpath())
	startTxNum := f.StartRootNum()
	endTxNum := f.EndRootNum()
	logger.Info("checking commitment root in", "kv", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)
	info, err := checkCommitmentRootViaFileData(ctx, tx, br, f, logger)
	if err != nil {
		return fmt.Errorf("%w: in %s with startTxNum=%d, endTxNum=%d", err, fileName, startTxNum, endTxNum)
	}
	sd, err := checkCommitmentRootViaSd(ctx, tx, f, info, logger)
	if err != nil {
		return fmt.Errorf("%w: in %s with startTxNum=%d, endTxNum=%d", err, fileName, startTxNum, endTxNum)
	}
	defer sd.Close()
	if !recompute {
		logger.Info("skipping commitment root recompute in file", "kv", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)
		return nil
	}
	err = checkCommitmentRootViaRecompute(ctx, tx, sd, info, f, logger)
	if err != nil {
		return fmt.Errorf("%w: in %s with startTxNum=%d, endTxNum=%d", err, fileName, startTxNum, endTxNum)
	}
	return nil
}

type commitmentRootInfo struct {
	rootHash      common.Hash
	blockNum      uint64
	txNum         uint64
	blockMinTxNum uint64
	blockMaxTxNum uint64
}

func (info commitmentRootInfo) PartialBlock() bool {
	return info.txNum < info.blockMaxTxNum
}

func checkCommitmentRootViaFileData(ctx context.Context, tx kv.TemporalTx, br services.FullBlockReader, f state.VisibleFile, logger log.Logger) (commitmentRootInfo, error) {
	var info commitmentRootInfo
	startTxNum := f.StartRootNum()
	endTxNum := f.EndRootNum()
	maxTxNum := endTxNum - 1
	v, ok, start, end, err := tx.Debug().GetLatestFromFiles(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, maxTxNum)
	if err != nil {
		return info, err
	}
	if !ok {
		return info, fmt.Errorf("%w: commitment root not found", ErrIntegrity)
	}
	if start != startTxNum {
		return info, fmt.Errorf("%w: commitment root not found with same startTxNum: %d != %d", ErrIntegrity, start, startTxNum)
	}
	if end != endTxNum {
		return info, fmt.Errorf("%w: commitment root not found with same endTxNum: %d != %d", ErrIntegrity, end, endTxNum)
	}
	rootHashBytes, blockNum, txNum, err := commitment.HexTrieExtractStateRoot(v)
	if err != nil {
		return info, fmt.Errorf("%w: commitment root could not be extracted: %w", ErrIntegrity, err)
	}
	if txNum >= endTxNum {
		return info, fmt.Errorf("%w: commitment root txNum is gte endTxNum: %d >= %d", ErrIntegrity, txNum, endTxNum)
	}
	if txNum < startTxNum {
		return info, fmt.Errorf("%w: commitment root txNum is lt startTxNum: %d < %d", ErrIntegrity, txNum, startTxNum)
	}
	txNumReader := br.TxnumReader()
	blockMinTxNum, err := txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return info, err
	}
	blockMaxTxNum, err := txNumReader.Max(ctx, tx, blockNum)
	if err != nil {
		return info, err
	}
	if txNum > blockMaxTxNum {
		return info, fmt.Errorf("%w: commitment root txNum gt blockMaxTxNum for block %d: %d > %d", ErrIntegrity, blockNum, txNum, blockMaxTxNum)
	}
	if txNum < blockMinTxNum {
		return info, fmt.Errorf("%w: commitment root txNum is lt blockMinTxNum for block %d: %d < %d", ErrIntegrity, blockNum, txNum, blockMinTxNum)
	}
	if txNum == 0 {
		return info, fmt.Errorf("%w: commitment root txNum should not be zero", ErrIntegrity)
	}
	rootHash := common.Hash(rootHashBytes)
	if rootHash == (common.Hash{}) {
		return info, fmt.Errorf("%w: commitment root is empty", ErrIntegrity)
	}
	info.rootHash, info.blockNum, info.txNum, info.blockMinTxNum, info.blockMaxTxNum = rootHash, blockNum, txNum, blockMinTxNum, blockMaxTxNum
	if info.PartialBlock() {
		logger.Info("skipping commitment root check with canonical header root as it is for partial block", "file", filepath.Base(f.Fullpath()), "blockNum", blockNum, "txNum", txNum, "blockMinTxNum", blockMinTxNum, "blockMaxTxNum", blockMaxTxNum)
		return info, nil
	}
	h, err := br.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return info, err
	}
	if h == nil {
		return info, fmt.Errorf("%w: missing canonical header for block %d", ErrIntegrity, blockNum)
	}
	if h.Root != rootHash {
		return info, fmt.Errorf("%w: commitment root does not match header root for block %d: %s != %s", ErrIntegrity, blockNum, h.Root, rootHash)
	}
	return info, nil
}

func checkCommitmentRootViaSd(ctx context.Context, tx kv.TemporalTx, f state.VisibleFile, info commitmentRootInfo, logger log.Logger) (*execctx.SharedDomains, error) {
	maxTxNum := f.EndRootNum() - 1
	sd, err := execctx.NewSharedDomains(ctx, tx, logger)
	if err != nil {
		return nil, err
	}
	sd.GetCommitmentCtx().SetTrace(logger.Enabled(ctx, log.LvlTrace))
	sd.GetCommitmentCtx().SetLimitedHistoryStateReader(tx, maxTxNum) // to use tx.Debug().GetLatestFromFiles with maxTxNum
	latestTxNum, _, err := sd.SeekCommitment(ctx, tx)                // seek commitment again to use the new state reader instead
	if err != nil {
		return nil, err
	}
	if latestTxNum > maxTxNum {
		return nil, fmt.Errorf("%w: commitment root sd txNum should is gt maxTxNum: %d > %d", ErrIntegrity, latestTxNum, maxTxNum)
	}
	if latestTxNum > info.blockMaxTxNum {
		return nil, fmt.Errorf("%w: commitment root sd txNum should is gt blockMaxTxNum: %d > %d", ErrIntegrity, latestTxNum, info.blockMaxTxNum)
	}
	if latestTxNum < info.blockMinTxNum {
		return nil, fmt.Errorf("%w: commitment root sd txNum should is lt blockMinTxNum: %d < %d", ErrIntegrity, latestTxNum, info.blockMinTxNum)
	}
	if latestTxNum == 0 {
		return nil, fmt.Errorf("%w: commitment root sd txNum should not be zero", ErrIntegrity)
	}
	if info.PartialBlock() {
		logger.Info("skipping commitment root sd check with canonical header root as it is for partial block", "file", filepath.Base(f.Fullpath()), "blockNum", info.blockNum, "txNum", info.txNum, "blockMinTxNum", info.blockMinTxNum, "blockMaxTxNum", info.blockMaxTxNum)
		return sd, nil
	}
	rootHashBytes, err := sd.GetCommitmentCtx().Trie().RootHash()
	if err != nil {
		return nil, err
	}
	rootHash := common.Hash(rootHashBytes)
	if rootHash != info.rootHash {
		return nil, fmt.Errorf("%w: commitment root does not match verified root for block %d: %s != %s", ErrIntegrity, info.blockNum, rootHash, info.rootHash)
	}
	return sd, nil
}

func checkCommitmentRootViaRecompute(ctx context.Context, tx kv.TemporalTx, sd *execctx.SharedDomains, info commitmentRootInfo, f state.VisibleFile, logger log.Logger) error {
	touchLoggingVisitor := func(k []byte) {
		logger.Debug("account touch for root block", "key", common.Address(k), "blockNum", info.blockNum, "file", filepath.Base(f.Fullpath()))
	}
	touches, err := touchHistoricalKeys(sd, tx, kv.AccountsDomain, info.blockMinTxNum, info.txNum+1, touchLoggingVisitor)
	if err != nil {
		return err
	}
	logger.Info("recomputing commitment root after", "touches", touches, "file", filepath.Base(f.Fullpath()))
	recomputedBytes, err := sd.ComputeCommitment(ctx, tx, false /* saveStateAfter */, info.blockNum, info.txNum, "integrity", nil /* commitProgress */)
	if err != nil {
		return err
	}
	recomputed := common.Hash(recomputedBytes)
	if recomputed != info.rootHash {
		return fmt.Errorf("%w: recomputed root does not match verified root: %s != %s", ErrIntegrity, recomputed, info.rootHash)
	}
	logger.Info("recomputed commitment root matches", "root", recomputed, "touches", touches, "file", filepath.Base(f.Fullpath()))
	return nil
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
	var branchKeys, referencedAccounts, plainAccounts, referencedStorages, plainStorages atomic.Uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		eg.Go(func() error {
			counts, err := checkCommitmentKvDeref(ctx, file, aggTx.StepSize(), failFast, logger)
			if err == nil {
				branchKeys.Add(counts.branchKeys)
				referencedAccounts.Add(counts.referencedAccounts)
				plainAccounts.Add(counts.plainAccounts)
				referencedStorages.Add(counts.referencedStorages)
				plainStorages.Add(counts.plainStorages)
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
		"branchKeys", branchKeys.Load(),
		"referencedAccounts", referencedAccounts.Load(),
		"plainAccounts", plainAccounts.Load(),
		"referencedStorages", referencedStorages.Load(),
		"plainStorages", plainStorages.Load(),
	)
	return nil
}

type derefCounts struct {
	branchKeys         uint64
	referencedAccounts uint64
	plainAccounts      uint64
	referencedStorages uint64
	plainStorages      uint64
}

func checkCommitmentKvDeref(ctx context.Context, file state.VisibleFile, stepSize uint64, failFast bool, logger log.Logger) (derefCounts, error) {
	start := time.Now()
	fileName := filepath.Base(file.Fullpath())
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()
	if !state.MayContainValuesPlainKeyReferencing(stepSize, startTxNum, endTxNum) {
		logger.Info(
			"[integrity] commitment deref skipped, file not above min steps",
			"file", fileName,
			"startTxNum", startTxNum,
			"endTxNum", endTxNum,
			"steps", (endTxNum-startTxNum)/stepSize,
		)
		return derefCounts{}, nil
	}
	logger.Info("[integrity] commitment deref in", "kv", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)
	commDecomp, err := seg.NewDecompressor(file.Fullpath())
	if err != nil {
		return derefCounts{}, err
	}
	defer commDecomp.Close()
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
	branchKeyBuf := make([]byte, 0, 128)
	branchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	newBranchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	plainKeyBuf := make([]byte, 0, length.Addr+length.Hash)
	var counts derefCounts
	var integrityErr error
	for i := 0; commReader.HasNext(); i++ {
		if i%1024 == 0 {
			select {
			case <-ctx.Done():
				return derefCounts{}, ctx.Err()
			case <-logTicker.C:
				at := fmt.Sprintf("%d/%d", counts.branchKeys, totalKeys)
				percent := fmt.Sprintf("%.1f%%", float64(counts.branchKeys)/float64(totalKeys)*100)
				rate := float64(counts.branchKeys) / time.Since(start).Seconds()
				eta := time.Duration(float64(totalKeys-counts.branchKeys)/rate) * time.Second
				logger.Info(
					"[integrity] commitment deref",
					"at", at,
					"p", percent,
					"k/s", rate,
					"eta", eta,
					"referencedAccounts", counts.referencedAccounts,
					"plainAccounts", counts.plainAccounts,
					"referencedStorages", counts.referencedStorages,
					"plainStorages", counts.plainStorages,
					"kv", fileName,
				)
			default: // proceed
			}
		}

		branchKey, _ := commReader.Next(branchKeyBuf[:0])
		if !commReader.HasNext() {
			err = errors.New("invalid key/value pair during decompression")
			if failFast {
				return derefCounts{}, err
			}
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			logger.Warn(err.Error())
			continue
		}
		branchValue, _ := commReader.Next(branchValueBuf[:0])
		if bytes.Equal(branchKey, commitmentdb.KeyCommitmentState) {
			logger.Info("skipping state key", "valueLen", len(branchValue), "file", fileName)
			continue
		}
		counts.branchKeys++
		branchData := commitment.BranchData(branchValue)
		newBranchData, err := branchData.ReplacePlainKeys(newBranchValueBuf[:0], func(key []byte, isStorage bool) ([]byte, error) {
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
					counts.plainStorages++
					return key, nil // not a referenced key, nothing to check
				}
				counts.referencedStorages++
				offset := state.DecodeReferenceKey(key)
				if offset >= uint64(storageReader.Size()) {
					err = fmt.Errorf("storage reference key %x out of bounds for branch %x in %s: %d vs %d", key, branchKey, fileName, offset, storageReader.Size())
					if failFast {
						return nil, err
					}
					logger.Warn(err.Error())
					return key, nil
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
					return key, nil
				}
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
				counts.plainAccounts++
				return key, nil // not a referenced key, nothing to check
			}
			counts.referencedAccounts++
			offset := state.DecodeReferenceKey(key)
			if offset >= uint64(accReader.Size()) {
				err = fmt.Errorf("account reference key %x out of bounds for branch %x in %s: %d vs %d", key, branchKey, fileName, offset, accReader.Size())
				if failFast {
					return nil, err
				}
				logger.Warn(err.Error())
				return key, nil
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
				return key, nil
			}
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
		err = newBranchData.Validate(branchKey)
		if err != nil {
			err = fmt.Errorf("branch data validation failure for branch key %x in %s: %w", branchKey, fileName, err)
			if failFast {
				return derefCounts{}, err
			}
			logger.Warn(err.Error())
			integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
		}
	}
	logger.Info(
		"checked commitment kv dereference in",
		"dur", time.Since(start),
		"branchKeys", counts.branchKeys,
		"referencedAccounts", counts.referencedAccounts,
		"plainAccounts", counts.plainAccounts,
		"referencedStorages", counts.referencedStorages,
		"plainStorages", counts.plainStorages,
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

func CheckCommitmentHistVal(ctx context.Context, db kv.TemporalRoDB, br services.FullBlockReader, failFast bool, logger log.Logger) error {
	start := time.Now()
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	files := tx.Debug().DomainFiles(kv.CommitmentDomain)
	var eg *errgroup.Group
	if failFast {
		// if 1 goroutine fails, fail others
		eg, ctx = errgroup.WithContext(ctx)
	} else {
		eg = &errgroup.Group{}
	}
	if dbg.EnvBool("CHECK_COMMITMENT_HIST_VAL_SEQUENTIAL", false) {
		eg.SetLimit(1)
	} else {
		eg.SetLimit(dbg.EnvInt("CHECK_COMMITMENT_HIST_VAL_WORKERS", 8))
	}
	var totalVals atomic.Uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".v") {
			continue
		}
		eg.Go(func() error {
			tx, err := db.BeginTemporalRo(ctx) // each worker has its own RoTx
			if err != nil {
				return err
			}
			defer tx.Rollback()
			valCount, err := checkCommitmentHistVal(ctx, tx, br, file, failFast, logger)
			if err == nil {
				totalVals.Add(valCount)
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
	dur := time.Since(start)
	total := totalVals.Load()
	rate := float64(total) / dur.Seconds()
	logger.Info("checked commitment history vals", "dur", time.Since(start), "files", len(files), "vals", total, "vals/s", rate)
	return nil
}

func checkCommitmentHistVal(ctx context.Context, tx kv.TemporalTx, br services.FullBlockReader, file state.VisibleFile, failFast bool, logger log.Logger) (uint64, error) {
	start := time.Now()
	fileName := filepath.Base(file.Fullpath())
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()
	txCount := endTxNum - startTxNum
	// cover 5% by doing random bucket sampling from each file
	coverageQuotient := dbg.EnvUint("CHECK_COMMITMENT_HIST_VAL_COVERAGE_QUOTIENT", 20)
	if coverageQuotient > txCount {
		panic(fmt.Errorf("coverage quotient %d is greater than total tx count %d", coverageQuotient, txCount))
	}
	bucket := rand.Intn(int(coverageQuotient))
	bucketSize := txCount / coverageQuotient
	bucketStart := startTxNum + uint64(bucket)*bucketSize
	bucketEnd := min(bucketStart+bucketSize, endTxNum)
	logger.Info(
		"checking commitment hist vals in",
		"v", fileName,
		"startTxNum", startTxNum,
		"endTxNum", endTxNum,
		"coverageQuotient", coverageQuotient,
		"bucket", bucket,
		"bucketSize", bucketSize,
		"bucketStart", bucketStart,
		"bucketEnd", bucketEnd,
	)
	txNumReader := br.TxnumReader()
	it, err := tx.HistoryRange(kv.CommitmentDomain, int(bucketStart), int(bucketEnd), order.Asc, -1)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()
	var total uint64
	var integrityErr error
	for it.HasNext() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-logTicker.C:
			rate := float64(total) / time.Since(start).Seconds()
			logger.Info("checking commitment hist vals progress", "at", total, "vals/s", rate, "v", fileName)
		default:
			// no-op
		}
		k, v, err := it.Next()
		if err != nil {
			return 0, err
		}
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			rootHashBytes, blockNum, txNum, err := commitment.HexTrieExtractStateRoot(v)
			if err != nil {
				return 0, fmt.Errorf("issue extracting state root value in %s for [%d,%d) tx nums: %w", fileName, bucketStart, bucketEnd, err)
			}
			maxTxNum, err := txNumReader.Max(ctx, tx, blockNum)
			if err != nil {
				return 0, err
			}
			if txNum < maxTxNum {
				logger.Info("skipping commitment state as it is for partial block", "blockNum", blockNum, "txNum", txNum, "maxTxNum", maxTxNum, "v", fileName)
				continue
			}
			if txNum != maxTxNum {
				err = fmt.Errorf("commitment state txNum mismatch for block %d in %s: %d != %d", blockNum, fileName, txNum, maxTxNum)
				if failFast {
					return 0, err
				}
				logger.Warn(err.Error())
				integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			}
			rootHash := common.Hash(rootHashBytes)
			header, err := br.HeaderByNumber(ctx, tx, blockNum)
			if err != nil {
				return 0, err
			}
			if rootHash != header.Root {
				err = fmt.Errorf("commitment state root mismatch for block %d, tx %d in %s: %s != %s", blockNum, txNum, fileName, rootHash, header.Root)
				if failFast {
					return 0, err
				}
				logger.Warn(err.Error())
				integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			}
		} else {
			branchData := commitment.BranchData(v)
			err = branchData.Validate(k)
			if err != nil {
				err = fmt.Errorf("branch data validation failure for branch key %x in %s: %w", k, fileName, err)
				if failFast {
					return 0, err
				}
				logger.Warn(err.Error())
				integrityErr = fmt.Errorf("%w: %w", ErrIntegrity, err)
			}
		}
		total++
	}
	dur := time.Since(start)
	rate := float64(total) / dur.Seconds()
	logger.Info("checked commitment history vals in", "dur", dur, "vals", total, "vals/s", rate, "v", fileName)
	return total, integrityErr
}

func CheckCommitmentHistAtBlk(ctx context.Context, db kv.TemporalRoDB, br services.FullBlockReader, blockNum uint64, logger log.Logger) error {
	logger.Info("checking commitment hist at block", "blockNum", blockNum)
	start := time.Now()
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	header, err := br.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	txNumsReader := br.TxnumReader()
	minTxNum, err := txNumsReader.Min(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	maxTxNum, err := txNumsReader.Max(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	toTxNum := maxTxNum + 1
	sd, err := execctx.NewSharedDomains(ctx, tx, logger)
	if err != nil {
		return err
	}
	sd.GetCommitmentCtx().SetHistoryStateReader(tx, toTxNum)
	sd.GetCommitmentCtx().SetTrace(logger.Enabled(ctx, log.LvlTrace))
	sd.GetCommitmentContext().SetDeferBranchUpdates(false)
	latestTxNum, latestBlockNum, err := sd.SeekCommitment(ctx, tx) // seek commitment again with new history state reader
	if err != nil {
		return err
	}
	if latestBlockNum != blockNum {
		return fmt.Errorf("commitment state blockNum doesn't match blockNum: %d != %d", latestBlockNum, blockNum)
	}
	if latestTxNum != maxTxNum {
		return fmt.Errorf("commitment state txNum doesn't match maxTxNum: %d != %d", latestTxNum, maxTxNum)
	}
	logger.Info("commitment recalc info", "blockNum", blockNum, "minTxNum", minTxNum, "maxTxNum", maxTxNum, "toTxNum", toTxNum)
	touchLoggingVisitor := func(k []byte) {
		args := []any{"key", common.Address(k[:length.Addr])}
		if len(k) > length.Addr {
			args = append(args, "slot", common.Hash(k[length.Addr:]))
		}
		logger.Debug("commitment touched key", args...)
	}
	touchStart := time.Now()
	accTouches, err := touchHistoricalKeys(sd, tx, kv.AccountsDomain, minTxNum, toTxNum, touchLoggingVisitor)
	if err != nil {
		return err
	}
	storageTouches, err := touchHistoricalKeys(sd, tx, kv.StorageDomain, minTxNum, toTxNum, touchLoggingVisitor)
	if err != nil {
		return err
	}
	codeTouches, err := touchHistoricalKeys(sd, tx, kv.CodeDomain, minTxNum, toTxNum, touchLoggingVisitor)
	if err != nil {
		return err
	}
	touchDur := time.Since(touchStart)
	logger.Info("commitment touched keys", "accTouches", accTouches, "storageTouches", storageTouches, "codeTouches", codeTouches, "touchDur", touchDur)
	recalcStart := time.Now()
	root, err := sd.ComputeCommitment(ctx, tx, false /* saveStateAfter */, blockNum, maxTxNum, "integrity", nil /* commitProgress */)
	if err != nil {
		return err
	}
	rootHash := common.Hash(root)
	if header.Root != rootHash {
		return fmt.Errorf("commitment root mismatch: %s != %s (blockNum=%d,txNum=%d)", header.Root, rootHash, blockNum, maxTxNum)
	}
	logger.Info(
		"commitment root matches",
		"blockNum", blockNum,
		"txNum", maxTxNum,
		"root", rootHash,
		"totalDur", time.Since(start),
		"touchDur", touchDur,
		"recalcDur", time.Since(recalcStart),
	)
	return nil
}

func CheckCommitmentHistAtBlkRange(ctx context.Context, db kv.TemporalRoDB, br services.FullBlockReader, from, to uint64, logger log.Logger) error {
	if from >= to {
		return fmt.Errorf("invalid blk range: %d >= %d", from, to)
	}
	start := time.Now()
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(estimate.AlmostAllCPUs())
	for blockNum := from; blockNum < to; blockNum++ {
		blockNum := blockNum
		g.Go(func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := CheckCommitmentHistAtBlk(ctx, db, br, blockNum, logger); err != nil {
				return fmt.Errorf("checkCommitmentHistAtBlk: %d, %w", blockNum, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	dur := time.Since(start)
	blks := to - from
	rate := float64(blks) / dur.Seconds()
	logger.Info("checked commitment hist at blk range", "dur", dur, "blks", blks, "blks/s", rate, "from", from, "to", to)
	return nil
}

func CheckStateVerify(ctx context.Context, db kv.TemporalRoDB, failFast bool, fromStep uint64, logger log.Logger) error {
	start := time.Now()
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	aggTx := state.AggTx(tx)
	files := aggTx.Files(kv.CommitmentDomain)
	stepSize := aggTx.StepSize()
	var integrityErr error
	var totalFiles int
	for _, file := range files {
		if !strings.HasSuffix(file.Fullpath(), ".kv") {
			continue
		}
		startTxNum := file.StartRootNum()
		fileStep := startTxNum / stepSize
		if fileStep < fromStep {
			continue
		}
		totalFiles++

		var checkErr error
		if startTxNum == 0 {
			// Base file: forward check (commitment refs count <= domain entries count)
			checkErr = checkStateCorrespondenceBase(ctx, file, stepSize, failFast, logger)
		} else {
			// Non-base file: reverse check (every domain key is in commitment refs)
			// Include the next commitment file's refs to handle step boundary effects:
			// accounts written near the end of a step may have their commitment branch
			// data in the next step's file.
			// Collect all previous files for no-op write detection.
			var nextFile state.VisibleFile
			var prevFiles []state.VisibleFile
			for j := 0; j < len(files); j++ {
				if files[j].StartRootNum() == file.EndRootNum() && strings.HasSuffix(files[j].Fullpath(), ".kv") {
					nextFile = files[j]
				}
				if files[j].EndRootNum() <= file.StartRootNum() && strings.HasSuffix(files[j].Fullpath(), ".kv") {
					prevFiles = append(prevFiles, files[j])
				}
			}
			checkErr = checkStateCorrespondenceReverse(ctx, file, nextFile, prevFiles, stepSize, failFast, logger)
		}
		if checkErr != nil {
			if !errors.Is(checkErr, ErrIntegrity) {
				return checkErr
			}
			if failFast {
				return checkErr
			}
			logger.Warn(checkErr.Error())
			integrityErr = checkErr
			continue
		}
	}
	logger.Info("[verify-state] done", "dur", time.Since(start), "files", totalFiles)
	return integrityErr
}

// checkStateCorrespondenceBase verifies base files (startTxNum==0) where commitment
// branches reference ALL keys in the trie, and the accounts/storage files contain
// all those keys. Forward check: commitment ref count <= domain entry count.
func checkStateCorrespondenceBase(ctx context.Context, file state.VisibleFile, stepSize uint64, failFast bool, logger log.Logger) error {
	start := time.Now()
	fileName := filepath.Base(file.Fullpath())
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()

	logger.Info("[verify-state] checking base file", "kv", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)

	// Open commitment decompressor + reader
	commDecomp, err := seg.NewDecompressor(file.Fullpath())
	if err != nil {
		return err
	}
	defer commDecomp.Close()
	commCompression := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
	commReader := seg.NewReader(commDecomp.MakeGetter(), commCompression)

	// Open accounts decompressor + reader
	accDecomp, accReader, accClose, err := deriveDecompAndReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.AccountsDomain)
	if err != nil {
		return err
	}
	defer accClose()

	// Open storage decompressor + reader
	stoDecomp, storageReader, storageClose, err := deriveDecompAndReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.StorageDomain)
	if err != nil {
		return err
	}
	defer storageClose()

	// Count domain entries (key/value pairs, so divide by 2)
	expectedAccounts := uint64(accDecomp.Count()) / 2
	expectedStorages := uint64(stoDecomp.Count()) / 2

	isReferencing := state.MayContainValuesPlainKeyReferencing(stepSize, startTxNum, endTxNum)

	// Track unique keys found in commitment branches
	accountOffsets := make(map[uint64]struct{}, 1*1024*1024) // for referenced files
	storageOffsets := make(map[uint64]struct{}, 1*1024*1024) // for referenced files
	accountPlain := make(map[string]struct{}, 1*1024*1024)   // for plain key files
	storagePlain := make(map[string]struct{}, 1*1024*1024)   // for plain key files

	totalKeys := uint64(commDecomp.Count()) / 2
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()
	branchKeyBuf := make([]byte, 0, 128)
	branchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	var branchKeys uint64
	var integrityErr error

	for i := 0; commReader.HasNext(); i++ {
		if i%1024 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logTicker.C:
				at := fmt.Sprintf("%d/%d", branchKeys, totalKeys)
				percent := fmt.Sprintf("%.1f%%", float64(branchKeys)/float64(totalKeys)*100)
				logger.Info("[verify-state] progress", "at", at, "p", percent, "kv", fileName)
			default:
			}
		}

		branchKey, _ := commReader.Next(branchKeyBuf[:0])
		if !commReader.HasNext() {
			err = errors.New("invalid key/value pair during decompression")
			if failFast {
				return fmt.Errorf("%w: %s in %s", ErrIntegrity, err, fileName)
			}
			integrityErr = fmt.Errorf("%w: %s in %s", ErrIntegrity, err, fileName)
			logger.Warn(err.Error())
			continue
		}
		branchValue, _ := commReader.Next(branchValueBuf[:0])

		if bytes.Equal(branchKey, commitmentdb.KeyCommitmentState) {
			continue
		}
		branchKeys++

		branchData := commitment.BranchData(branchValue)

		// Check completeness
		if !branchData.IsComplete() {
			touchMap := uint16(0)
			afterMap := uint16(0)
			if len(branchData) >= 4 {
				touchMap = binary.BigEndian.Uint16(branchData[0:])
				afterMap = binary.BigEndian.Uint16(branchData[2:])
			}
			err := fmt.Errorf("%w: incomplete branch at key=%x (touchMap=0x%04x afterMap=0x%04x) in %s", ErrIntegrity, branchKey, touchMap, afterMap, fileName)
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = err
			continue
		}

		// Walk the branch to extract all referenced keys.
		// The callback returns nil (keep original key in output) because the result of ReplacePlainKeys
		// is discarded (_): all side-effects (populating storageOffsets/accountOffsets, length validation)
		// happen before the return, so the decoded plain key value itself is not needed here.
		_, err := branchData.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			if isStorage {
				if len(key) == length.Addr+length.Hash {
					// Plain key
					storagePlain[string(key)] = struct{}{}
					return key, nil
				}
				if isReferencing {
					// Referenced key — decode offset
					offset := state.DecodeReferenceKey(key)
					if offset >= uint64(storageReader.Size()) {
						err := fmt.Errorf("%w: storage reference key %x out of bounds for branch %x in %s: %d vs %d", ErrIntegrity, key, branchKey, fileName, offset, storageReader.Size())
						if failFast {
							return nil, err
						}
						logger.Warn(err.Error())
						return key, nil
					}
					if _, alreadySeen := storageOffsets[offset]; !alreadySeen {
						storageOffsets[offset] = struct{}{}
						storageReader.Reset(offset)
						if _, keyLen := storageReader.Skip(); keyLen != length.Addr+length.Hash { // we don't need key itself
							err := fmt.Errorf("%w: storage reference key %x has invalid plainKey len=%d for branch %x in %s", ErrIntegrity, key, keyLen, branchKey, fileName)
							if failFast {
								return nil, err
							}
							logger.Warn(err.Error())
						}
					}
					return nil, nil // safe: result of ReplacePlainKeys is discarded (_)
				}
				// Unknown key format
				err := fmt.Errorf("%w: unexpected storage key len=%d for branch %x in %s", ErrIntegrity, len(key), branchKey, fileName)
				if failFast {
					return nil, err
				}
				logger.Warn(err.Error())
				return key, nil
			}

			// Account key
			if len(key) == length.Addr {
				// Plain key
				accountPlain[string(key)] = struct{}{}
				return key, nil
			}
			if isReferencing {
				// Referenced key — decode offset
				offset := state.DecodeReferenceKey(key)
				if offset >= uint64(accReader.Size()) {
					err := fmt.Errorf("%w: account reference key %x out of bounds for branch %x in %s: %d vs %d", ErrIntegrity, key, branchKey, fileName, offset, accReader.Size())
					if failFast {
						return nil, err
					}
					logger.Warn(err.Error())
					return key, nil
				}
				if _, alreadySeen := accountOffsets[offset]; !alreadySeen {
					accountOffsets[offset] = struct{}{}
					accReader.Reset(offset)
					if _, keyLen := accReader.Skip(); keyLen != length.Addr { // we don't need key itself
						err := fmt.Errorf("%w: account reference key %x has invalid plainKey len=%d for branch %x in %s", ErrIntegrity, key, keyLen, branchKey, fileName)
						if failFast {
							return nil, err
						}
						logger.Warn(err.Error())
					}
				}
				return nil, nil // safe: result of ReplacePlainKeys is discarded (_)
			}
			// Unknown key format
			err := fmt.Errorf("%w: unexpected account key len=%d for branch %x in %s", ErrIntegrity, len(key), branchKey, fileName)
			if failFast {
				return nil, err
			}
			logger.Warn(err.Error())
			return key, nil
		})
		if err != nil {
			if failFast {
				return err
			}
			logger.Warn(err.Error())
			integrityErr = err
		}
	}

	// Compare counts
	var foundAccounts, foundStorages uint64
	if isReferencing {
		foundAccounts = uint64(len(accountOffsets))
		foundStorages = uint64(len(storageOffsets))
	} else {
		foundAccounts = uint64(len(accountPlain))
		foundStorages = uint64(len(storagePlain))
	}

	// Forward check: commitment must not reference MORE keys than exist in domain files.
	// Commitment may reference FEWER keys because branch data for accounts written near
	// step boundaries may land in the next step's commitment file.
	if foundAccounts > expectedAccounts {
		err := fmt.Errorf("%w: FAIL %s accounts_in_commitment=%d > accounts_in_file=%d (extra=%d)", ErrIntegrity, fileName, foundAccounts, expectedAccounts, foundAccounts-expectedAccounts)
		if failFast {
			return err
		}
		logger.Warn(err.Error())
		integrityErr = err
	}
	if foundStorages > expectedStorages {
		err := fmt.Errorf("%w: FAIL %s storage_in_commitment=%d > storage_in_file=%d (extra=%d)", ErrIntegrity, fileName, foundStorages, expectedStorages, foundStorages-expectedStorages)
		if failFast {
			return err
		}
		logger.Warn(err.Error())
		integrityErr = err
	}

	dur := time.Since(start)
	if integrityErr == nil {
		logger.Info("[verify-state] key correspondence PASS (base)", "kv", fileName,
			"accounts", fmt.Sprintf("%d/%d", foundAccounts, expectedAccounts),
			"storage", fmt.Sprintf("%d/%d", foundStorages, expectedStorages),
			"dur", dur)

		// Phase 2: Hash verification — only runs if key correspondence passes.
		numWorkers := dbg.EnvInt("CHECK_VERIFY_STATE_WORKERS", runtime.NumCPU())
		hashErr := checkHashVerification(ctx, file, stepSize, failFast, numWorkers, logger)
		if hashErr != nil {
			integrityErr = hashErr
		}
	}
	return integrityErr
}

// checkStateCorrespondenceReverse verifies non-base files (startTxNum > 0) by checking
// that every key in accounts.kv and storage.kv is referenced by some commitment branch.
//
// nextFile (optional) is the next commitment .kv file. Refs from it are also extracted
// to handle boundary effects: accounts written near the end of a step may have their
// commitment branch data in the next step's file.
//
// Approach: walk commitment branches → write all extracted plain keys to temp files →
// sort+dedup → merge-join with domain .kv files (which are also sorted by key).
func checkStateCorrespondenceReverse(ctx context.Context, file state.VisibleFile, nextFile state.VisibleFile, prevFiles []state.VisibleFile, stepSize uint64, failFast bool, logger log.Logger) error {
	start := time.Now()
	fileName := filepath.Base(file.Fullpath())
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()

	logger.Info("[verify-state] checking non-base file", "kv", fileName, "startTxNum", startTxNum, "endTxNum", endTxNum)

	// Open commitment decompressor + reader
	commDecomp, err := seg.NewDecompressor(file.Fullpath())
	if err != nil {
		return err
	}
	defer commDecomp.Close()
	commCompression := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
	commReader := seg.NewReader(commDecomp.MakeGetter(), commCompression)

	// Open accounts decompressor + reader (for dereferencing and reverse check)
	accDecomp, accReader, accClose, err := deriveDecompAndReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.AccountsDomain)
	if err != nil {
		return err
	}
	defer accClose()

	// Open storage decompressor + reader
	stoDecomp, storageReader, storageClose, err := deriveDecompAndReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.StorageDomain)
	if err != nil {
		return err
	}
	defer storageClose()

	expectedAccounts := uint64(accDecomp.Count()) / 2
	expectedStorages := uint64(stoDecomp.Count()) / 2

	// Create temp files for commitment-extracted keys (hex-encoded, one per line)
	accKeysFile, err := os.CreateTemp("", "verify-acc-*.hex")
	if err != nil {
		return err
	}
	accKeysPath := accKeysFile.Name()
	defer dir.RemoveFile(accKeysPath)

	stoKeysFile, err := os.CreateTemp("", "verify-sto-*.hex")
	if err != nil {
		return err
	}
	stoKeysPath := stoKeysFile.Name()
	defer dir.RemoveFile(stoKeysPath)

	accBuf := bufio.NewWriterSize(accKeysFile, 1<<20)
	stoBuf := bufio.NewWriterSize(stoKeysFile, 1<<20)

	totalKeys := uint64(commDecomp.Count()) / 2
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()
	branchKeyBuf := make([]byte, 0, 128)
	branchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	plainKeyBuf := make([]byte, 0, length.Addr+length.Hash)
	hexBuf := make([]byte, (length.Addr+length.Hash)*2) // big enough for any hex-encoded key
	var branchKeys uint64
	var integrityErr error
	var extractedAccKeys, extractedStoKeys, skippedAccKeys, skippedStoKeys uint64

	// Phase 1: Walk commitment branches, write extracted plain keys to temp files
	for i := 0; commReader.HasNext(); i++ {
		if i%1024 == 0 {
			select {
			case <-ctx.Done():
				accKeysFile.Close()
				stoKeysFile.Close()
				return ctx.Err()
			case <-logTicker.C:
				logger.Info("[verify-state] extracting refs", "at", fmt.Sprintf("%d/%d", branchKeys, totalKeys),
					"p", fmt.Sprintf("%.1f%%", float64(branchKeys)/float64(totalKeys)*100), "kv", fileName)
			default:
			}
		}

		branchKey, _ := commReader.Next(branchKeyBuf[:0])
		if !commReader.HasNext() {
			break
		}
		branchValue, _ := commReader.Next(branchValueBuf[:0])

		if bytes.Equal(branchKey, commitmentdb.KeyCommitmentState) {
			continue
		}
		branchKeys++

		branchData := commitment.BranchData(branchValue)

		if !branchData.IsComplete() {
			touchMap := uint16(0)
			afterMap := uint16(0)
			if len(branchData) >= 4 {
				touchMap = binary.BigEndian.Uint16(branchData[0:])
				afterMap = binary.BigEndian.Uint16(branchData[2:])
			}
			err := fmt.Errorf("%w: incomplete branch at key=%x (touchMap=0x%04x afterMap=0x%04x) in %s", ErrIntegrity, branchKey, touchMap, afterMap, fileName)
			if failFast {
				accKeysFile.Close()
				stoKeysFile.Close()
				return err
			}
			logger.Warn(err.Error())
			integrityErr = err
			continue
		}

		_, err := branchData.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			if isStorage {
				plainKey := key
				if len(key) == length.Addr+length.Hash {
					// Plain key (52 bytes)
				} else {
					// Try to decode as file offset reference
					offset := state.DecodeReferenceKey(key)
					if offset < uint64(storageReader.Size()) {
						storageReader.Reset(offset)
						plainKey, _ = storageReader.Next(plainKeyBuf[:0])
					} else {
						skippedStoKeys++
						return key, nil
					}
				}
				if len(plainKey) == length.Addr+length.Hash {
					extractedStoKeys++
					n := hex.Encode(hexBuf, plainKey)
					stoBuf.Write(hexBuf[:n])
					stoBuf.WriteByte('\n')
				}
				return plainKey, nil
			}
			// Account key
			plainKey := key
			if len(key) == length.Addr {
				// Plain key (20 bytes)
			} else {
				// Try to decode as file offset reference
				offset := state.DecodeReferenceKey(key)
				if offset < uint64(accReader.Size()) {
					accReader.Reset(offset)
					plainKey, _ = accReader.Next(plainKeyBuf[:0])
				} else {
					skippedAccKeys++
					return key, nil
				}
			}
			if len(plainKey) == length.Addr {
				extractedAccKeys++
				n := hex.Encode(hexBuf, plainKey)
				accBuf.Write(hexBuf[:n])
				accBuf.WriteByte('\n')
			}
			return plainKey, nil
		})
		if err != nil {
			if failFast {
				accKeysFile.Close()
				stoKeysFile.Close()
				return err
			}
			integrityErr = err
		}
	}

	// Also extract refs from the next commitment file (handles step boundary effects)
	if nextFile != nil {
		if err := extractCommitmentRefsToTempFiles(ctx, nextFile, stepSize, accBuf, stoBuf, hexBuf, logger); err != nil {
			logger.Warn("[verify-state] failed to extract refs from next file", "err", err)
			// Non-fatal: proceed with what we have
		}
	}

	accBuf.Flush()
	stoBuf.Flush()
	accKeysFile.Close()
	stoKeysFile.Close()

	logger.Info("[verify-state] extracted refs, sorting", "kv", fileName,
		"extractedAcc", extractedAccKeys, "extractedSto", extractedStoKeys,
		"skippedAcc", skippedAccKeys, "skippedSto", skippedStoKeys,
		"branches", branchKeys, "dur", time.Since(start))

	// Phase 2: Sort + dedup temp files using system sort (handles external sorting for large files)
	if err := sortUniqueTempFile(accKeysPath); err != nil {
		return fmt.Errorf("sorting account keys: %w", err)
	}
	if err := sortUniqueTempFile(stoKeysPath); err != nil {
		return fmt.Errorf("sorting storage keys: %w", err)
	}

	logger.Info("[verify-state] sorted refs, verifying domains", "kv", fileName, "dur", time.Since(start))

	// Phase 3: Reverse check — merge-join domain .kv with sorted commitment refs.
	// Collect previous file paths for no-op write detection.
	var prevCommitmentPaths []string
	for _, pf := range prevFiles {
		prevCommitmentPaths = append(prevCommitmentPaths, pf.Fullpath())
	}
	// Sort newest-first so we check the most recent previous file first.
	sort.Slice(prevCommitmentPaths, func(i, j int) bool {
		return prevCommitmentPaths[i] > prevCommitmentPaths[j]
	})
	accMissing, err := reverseCheckDomainKeys(accDecomp, kv.AccountsDomain, accKeysPath, prevCommitmentPaths, fileName, failFast, logger)
	if err != nil && !errors.Is(err, ErrIntegrity) {
		return err
	}
	if err != nil {
		integrityErr = err
	}
	stoMissing, err := reverseCheckDomainKeys(stoDecomp, kv.StorageDomain, stoKeysPath, prevCommitmentPaths, fileName, failFast, logger)
	if err != nil && !errors.Is(err, ErrIntegrity) {
		return err
	}
	if err != nil {
		integrityErr = err
	}

	dur := time.Since(start)
	if integrityErr == nil {
		logger.Info("[verify-state] key correspondence PASS", "kv", fileName,
			"accounts", fmt.Sprintf("%d/%d", expectedAccounts-accMissing, expectedAccounts),
			"storage", fmt.Sprintf("%d/%d", expectedStorages-stoMissing, expectedStorages),
			"dur", dur)

		// Phase 2: Hash verification — only runs if key correspondence passes.
		numWorkers := dbg.EnvInt("CHECK_VERIFY_STATE_WORKERS", runtime.NumCPU())
		hashErr := checkHashVerification(ctx, file, stepSize, failFast, numWorkers, logger)
		if hashErr != nil {
			integrityErr = hashErr
		}
	}
	return integrityErr
}

// sortUniqueTempFile sorts and deduplicates a text file in-place using the system's sort command.
func sortUniqueTempFile(path string) error {
	cmd := exec.Command("sort", "-u", "-o", path, path)
	cmd.Env = append(os.Environ(), "LC_ALL=C") // binary sort order for hex strings
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sort -u %s: %w: %s", path, err, string(out))
	}
	return nil
}

// missingEntry holds a domain key+value that wasn't found in commitment refs.
type missingEntry struct {
	key []byte
	val []byte
}

// reverseCheckDomainKeys opens a domain .kv decompressor and checks that every key
// appears in the sorted hex keys file (produced from commitment refs).
// Both the domain .kv and the sorted file are in ascending order, enabling a merge-join.
//
// prevCommitmentPaths are commitment file paths for previous steps (newest-first).
// When a key is missing from refs, its value is compared with previous domain files
// to detect no-op writes (same value recorded redundantly).
func reverseCheckDomainKeys(decomp *seg.Decompressor, domain kv.Domain, sortedKeysPath string, prevCommitmentPaths []string, commitFileName string, failFast bool, logger log.Logger) (missing uint64, retErr error) {
	compression := statecfg.Schema.GetDomainCfg(domain).Compression
	reader := seg.NewReader(decomp.MakeGetter(), compression)
	reader.Reset(0) // start from beginning

	f, err := os.Open(sortedKeysPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	// Advance to first ref key
	var refKey string
	hasRef := scanner.Scan()
	if hasRef {
		refKey = scanner.Text()
	}

	keyBuf := make([]byte, 0, length.Addr+length.Hash)
	valBuf := make([]byte, 0, 128)
	var checked, skippedEmpty uint64
	var missingEntries []missingEntry
	for reader.HasNext() {
		key, _ := reader.Next(keyBuf[:0])
		if !reader.HasNext() {
			break // malformed: key without value
		}
		val, _ := reader.Next(valBuf[:0])

		// Entries with empty values represent deletions — the key was removed
		// from the state trie. Commitment branches only reference live entries,
		// so we skip these.
		if len(val) == 0 {
			skippedEmpty++
			continue
		}

		domainKeyHex := hex.EncodeToString(key)
		checked++

		// Advance sorted refs until >= domainKeyHex
		for hasRef && refKey < domainKeyHex {
			hasRef = scanner.Scan()
			if hasRef {
				refKey = scanner.Text()
			}
		}

		if !hasRef || refKey != domainKeyHex {
			// Collect for no-op verification against previous files.
			missingEntries = append(missingEntries, missingEntry{
				key: common.Copy(key),
				val: common.Copy(val),
			})
		}
	}

	if skippedEmpty > 0 {
		logger.Info("[verify-state] skipped empty-value entries (deletions)",
			"domain", domain, "skipped", skippedEmpty, "kv", commitFileName)
	}

	if len(missingEntries) == 0 {
		return 0, nil
	}

	// Check missing entries against previous domain files to detect no-op writes.
	genuineMissing := verifyMissingAgainstPrevFiles(missingEntries, domain, prevCommitmentPaths, commitFileName, failFast, logger)
	missing = uint64(genuineMissing)

	if missing > 0 {
		retErr = fmt.Errorf("%w: %s %d/%d keys not referenced by commitment in %s",
			ErrIntegrity, domain, missing, checked, commitFileName)
		logger.Warn(retErr.Error())
	}
	return missing, retErr
}

// verifyMissingAgainstPrevFiles checks each missing entry against previous domain files.
// For each missing key, it scans previous files (newest-first) to find the same key.
// If the value matches, it's a no-op write (not an error). If the value differs or
// the key isn't found in any previous file, it's a genuine missing entry.
//
// Uses merge-join within each file: missing entries are sorted by key, and the file
// is scanned sequentially, advancing both cursors in lockstep.
func verifyMissingAgainstPrevFiles(entries []missingEntry, domain kv.Domain, prevCommitmentPaths []string, commitFileName string, failFast bool, logger log.Logger) int {
	if len(prevCommitmentPaths) == 0 {
		// No previous files to check — all are genuine.
		for i, e := range entries {
			if i < 10 {
				logger.Warn("[verify-state] domain key not in commitment refs (no previous files)",
					"domain", domain, "key", hex.EncodeToString(e.key), "kv", commitFileName)
			}
		}
		return len(entries)
	}

	// Sort missing entries by key for merge-join.
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})

	// Track which entries are confirmed as no-ops.
	confirmed := make([]bool, len(entries))
	remaining := len(entries)

	compression := statecfg.Schema.GetDomainCfg(domain).Compression

	for _, prevCommitPath := range prevCommitmentPaths {
		if remaining == 0 {
			break
		}

		// Derive the domain file path from the commitment file path.
		prevDomainReader, prevClose, err := deriveReaderForOtherDomain(prevCommitPath, kv.CommitmentDomain, domain)
		if err != nil {
			logger.Warn("[verify-state] could not open previous domain file",
				"domain", domain, "err", err)
			continue
		}

		// Merge-join: walk the previous file and check all unconfirmed entries.
		_ = compression // reader already has compression configured
		keyBuf := make([]byte, 0, length.Addr+length.Hash)
		valBuf := make([]byte, 0, 128)
		ei := 0 // index into sorted entries

		for prevDomainReader.HasNext() && ei < len(entries) {
			// Skip already confirmed entries.
			for ei < len(entries) && confirmed[ei] {
				ei++
			}
			if ei >= len(entries) {
				break
			}

			prevKey, _ := prevDomainReader.Next(keyBuf[:0])
			if !prevDomainReader.HasNext() {
				break
			}
			prevVal, _ := prevDomainReader.Next(valBuf[:0])

			// Advance entries index past keys that are < prevKey.
			for ei < len(entries) && bytes.Compare(entries[ei].key, prevKey) < 0 {
				ei++
			}
			if ei >= len(entries) {
				break
			}

			if bytes.Equal(entries[ei].key, prevKey) {
				if bytes.Equal(entries[ei].val, prevVal) {
					// No-op write confirmed.
					confirmed[ei] = true
					remaining--
					logger.Info("[verify-state] no-op write confirmed (same value in previous file)",
						"domain", domain, "key", hex.EncodeToString(entries[ei].key),
						"kv", commitFileName, "prevKv", filepath.Base(prevCommitPath))
				}
				ei++
			}
		}

		prevClose()
	}

	// Report genuinely missing entries.
	genuine := 0
	for i, e := range entries {
		if !confirmed[i] {
			genuine++
			if genuine <= 10 {
				logger.Warn("[verify-state] domain key not in commitment refs",
					"domain", domain, "key", hex.EncodeToString(e.key), "kv", commitFileName)
			}
		}
	}
	return genuine
}

// extractCommitmentRefsToTempFiles walks a commitment .kv file and appends all
// extracted plain keys (hex-encoded) to the provided writers. This is used to
// include refs from the "next" commitment file for boundary coverage.
// Opens its own domain readers for dereferencing (the next file's offsets point
// into its own domain files, not the current file's).
func extractCommitmentRefsToTempFiles(ctx context.Context, file state.VisibleFile, stepSize uint64, accBuf *bufio.Writer, stoBuf *bufio.Writer, hexBuf []byte, logger log.Logger) error {
	nextFileName := filepath.Base(file.Fullpath())
	logger.Info("[verify-state] also extracting refs from next file", "kv", nextFileName)

	commDecomp, err := seg.NewDecompressor(file.Fullpath())
	if err != nil {
		return err
	}
	defer commDecomp.Close()
	commCompression := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
	commReader := seg.NewReader(commDecomp.MakeGetter(), commCompression)

	// Always open domain readers for dereferencing (commitment files may contain
	// reference keys regardless of MayContainValuesPlainKeyReferencing result)
	_, nextAccReader, nextAccClose, err := deriveDecompAndReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.AccountsDomain)
	if err != nil {
		return err
	}
	defer nextAccClose()
	_, nextStoReader, nextStoClose, err := deriveDecompAndReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.StorageDomain)
	if err != nil {
		return err
	}
	defer nextStoClose()

	branchKeyBuf := make([]byte, 0, 128)
	branchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	plainKeyBuf := make([]byte, 0, length.Addr+length.Hash)

	for commReader.HasNext() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		branchKey, _ := commReader.Next(branchKeyBuf[:0])
		if !commReader.HasNext() {
			break
		}
		branchValue, _ := commReader.Next(branchValueBuf[:0])

		if bytes.Equal(branchKey, commitmentdb.KeyCommitmentState) {
			continue
		}

		branchData := commitment.BranchData(branchValue)
		if !branchData.IsComplete() {
			continue
		}

		branchData.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			if isStorage {
				plainKey := key
				if len(key) != length.Addr+length.Hash {
					offset := state.DecodeReferenceKey(key)
					if offset >= uint64(nextStoReader.Size()) {
						return key, nil
					}
					nextStoReader.Reset(offset)
					plainKey, _ = nextStoReader.Next(plainKeyBuf[:0])
				}
				if len(plainKey) == length.Addr+length.Hash {
					n := hex.Encode(hexBuf, plainKey)
					stoBuf.Write(hexBuf[:n])
					stoBuf.WriteByte('\n')
				}
				return plainKey, nil
			}
			plainKey := key
			if len(key) != length.Addr {
				offset := state.DecodeReferenceKey(key)
				if offset >= uint64(nextAccReader.Size()) {
					return key, nil
				}
				nextAccReader.Reset(offset)
				plainKey, _ = nextAccReader.Next(plainKeyBuf[:0])
			}
			if len(plainKey) == length.Addr {
				n := hex.Encode(hexBuf, plainKey)
				accBuf.Write(hexBuf[:n])
				accBuf.WriteByte('\n')
			}
			return plainKey, nil
		})
	}
	return nil
}

// hashWorkItem holds a single commitment branch entry for hash verification.
type hashWorkItem struct {
	branchKey   []byte
	branchValue []byte
}

// checkHashVerification verifies that stateHash stored in each commitment branch cell
// matches the hash recomputed from the actual domain values. Uses a producer-consumer
// pattern: 1 producer reads the commitment file sequentially, N workers each open their
// own domain readers and verify hashes in parallel.
func checkHashVerification(ctx context.Context, file state.VisibleFile, stepSize uint64, failFast bool, numWorkers int, logger log.Logger) error {
	start := time.Now()
	fileName := filepath.Base(file.Fullpath())
	startTxNum := file.StartRootNum()
	endTxNum := file.EndRootNum()

	isReferencing := state.MayContainValuesPlainKeyReferencing(stepSize, startTxNum, endTxNum)

	logger.Info("[verify-state] hash verification starting",
		"kv", fileName, "workers", numWorkers, "referencing", isReferencing)

	if numWorkers < 1 {
		numWorkers = 1
	}

	// For non-referencing files, preload domain key→value maps so workers
	// can look up values for plain keys. These files are small enough to fit in memory.
	var preloadedAccValues, preloadedStoValues map[string][]byte
	if !isReferencing {
		var err error
		preloadedAccValues, err = preloadDomainValues(file.Fullpath(), kv.CommitmentDomain, kv.AccountsDomain, length.Addr)
		if err != nil {
			return fmt.Errorf("preload accounts: %w", err)
		}
		preloadedStoValues, err = preloadDomainValues(file.Fullpath(), kv.CommitmentDomain, kv.StorageDomain, length.Addr+length.Hash)
		if err != nil {
			return fmt.Errorf("preload storage: %w", err)
		}
		logger.Info("[verify-state] preloaded domain values",
			"accounts", len(preloadedAccValues), "storage", len(preloadedStoValues), "kv", fileName)
	}

	// Channel for sending work items from producer to workers.
	workCh := make(chan hashWorkItem, numWorkers*4)
	var hashMismatches atomic.Uint64
	var hashChecked atomic.Uint64

	// Pool to reuse per-item value maps and reduce GC pressure.
	var valMapPool sync.Pool
	valMapPool.New = func() any { return make(map[string][]byte, 8) }

	// Set up errgroup with context for cancellation on failure.
	var eg *errgroup.Group
	if failFast {
		eg, ctx = errgroup.WithContext(ctx)
	} else {
		eg = &errgroup.Group{}
	}

	// Launch N worker goroutines.
	for w := 0; w < numWorkers; w++ {
		eg.Go(func() error {
			// Each worker opens its own domain readers for referencing files.
			var accReader, stoReader *seg.Reader
			var accClose, stoClose func()
			if isReferencing {
				var err error
				accReader, accClose, err = deriveReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.AccountsDomain)
				if err != nil {
					return fmt.Errorf("worker: open accounts reader: %w", err)
				}
				defer accClose()
				stoReader, stoClose, err = deriveReaderForOtherDomain(file.Fullpath(), kv.CommitmentDomain, kv.StorageDomain)
				if err != nil {
					return fmt.Errorf("worker: open storage reader: %w", err)
				}
				defer stoClose()
			}

			plainKeyBuf := make([]byte, 0, length.Addr+length.Hash)
			valBuf := make([]byte, 0, 128)

			for item := range workCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				branchData := commitment.BranchData(item.branchValue)

				// Build maps of accountValues and storageValues by resolving
				// keys/values from domain files.
				accountValues := valMapPool.Get().(map[string][]byte)
				storageValues := valMapPool.Get().(map[string][]byte)

				// We need branch data with plain keys for VerifyBranchHashes.
				// Walk the branch to extract + resolve all keys and read values.
				resolvedBranchData, err := branchData.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
					if isStorage {
						plainKey := key
						isRef := len(key) != length.Addr+length.Hash
						if isRef {
							if !isReferencing {
								return key, nil
							}
							offset := state.DecodeReferenceKey(key)
							if offset >= uint64(stoReader.Size()) {
								return key, nil
							}
							stoReader.Reset(offset)
							plainKey, _ = stoReader.Next(plainKeyBuf[:0])
							if len(plainKey) != length.Addr+length.Hash {
								return key, nil
							}
							val, _ := stoReader.Next(valBuf[:0])
							storageValues[string(plainKey)] = common.Copy(val)
						} else if preloadedStoValues != nil {
							strKey := string(plainKey)
							if val, ok := preloadedStoValues[strKey]; ok {
								storageValues[strKey] = val
							}
						}
						return plainKey, nil
					}

					// Account key
					plainKey := key
					isRef := len(key) != length.Addr
					if isRef {
						if !isReferencing {
							return key, nil
						}
						offset := state.DecodeReferenceKey(key)
						if offset >= uint64(accReader.Size()) {
							return key, nil
						}
						accReader.Reset(offset)
						plainKey, _ = accReader.Next(plainKeyBuf[:0])
						if len(plainKey) != length.Addr {
							return key, nil
						}
						val, _ := accReader.Next(valBuf[:0])
						accountValues[string(plainKey)] = common.Copy(val)
					} else if preloadedAccValues != nil {
						strKey := string(plainKey)
						if val, ok := preloadedAccValues[strKey]; ok {
							accountValues[strKey] = val
						}
					}
					return plainKey, nil
				})
				if err != nil {
					if failFast {
						return err
					}
					logger.Warn("[verify-state] hash: ReplacePlainKeys error", "err", err, "kv", fileName)
					continue
				}

				// Only verify if we have at least one value to check.
				if len(accountValues) == 0 && len(storageValues) == 0 {
					continue
				}

				err = commitment.VerifyBranchHashes(item.branchKey, resolvedBranchData, accountValues, storageValues)
				if err != nil {
					hashMismatches.Add(1)
					if failFast {
						return fmt.Errorf("%w: %s in %s", ErrIntegrity, err.Error(), fileName)
					}
					logger.Warn("[verify-state] hash mismatch", "err", err, "kv", fileName)
				}
				hashChecked.Add(1)
			}
			return nil
		})
	}

	// Producer: read commitment file and send work items.
	eg.Go(func() error {
		defer close(workCh)

		commDecomp, err := seg.NewDecompressor(file.Fullpath())
		if err != nil {
			return err
		}
		defer commDecomp.Close()
		commCompression := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
		commReader := seg.NewReader(commDecomp.MakeGetter(), commCompression)

		branchKeyBuf := make([]byte, 0, 128)
		branchValueBuf := make([]byte, 0, datasize.MB.Bytes())
		logTicker := time.NewTicker(30 * time.Second)
		defer logTicker.Stop()
		var produced uint64

		for commReader.HasNext() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logTicker.C:
				logger.Info("[verify-state] hash verification progress",
					"produced", produced,
					"checked", hashChecked.Load(),
					"mismatches", hashMismatches.Load(),
					"kv", fileName)
			default:
			}

			branchKey, _ := commReader.Next(branchKeyBuf[:0])
			if !commReader.HasNext() {
				break
			}
			branchValue, _ := commReader.Next(branchValueBuf[:0])

			if bytes.Equal(branchKey, commitmentdb.KeyCommitmentState) {
				continue
			}

			branchData := commitment.BranchData(branchValue)
			if !branchData.IsComplete() {
				continue
			}

			produced++

			// Copy data since buffers are reused.
			item := hashWorkItem{
				branchKey:   common.Copy(branchKey),
				branchValue: common.Copy(branchValue),
			}

			select {
			case workCh <- item:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	err := eg.Wait()
	dur := time.Since(start)

	checked := hashChecked.Load()
	mismatches := hashMismatches.Load()

	if err != nil {
		logger.Warn("[verify-state] hash verification FAIL",
			"checked", checked, "mismatches", mismatches,
			"dur", dur, "kv", fileName, "err", err)
		return err
	}

	logger.Info("[verify-state] hash verification PASS",
		"checked", checked, "mismatches", mismatches,
		"dur", dur, "kv", fileName)
	return nil
}

// preloadDomainValues reads all key-value pairs from a domain .kv file into a map.
// Used for non-referencing files where keys are plain (not file offsets).
func preloadDomainValues(commitmentFile string, oldDomain, newDomain kv.Domain, expectedKeyLen int) (map[string][]byte, error) {
	reader, closeFn, err := deriveReaderForOtherDomain(commitmentFile, oldDomain, newDomain)
	if err != nil {
		return nil, err
	}
	defer closeFn()

	values := make(map[string][]byte)
	keyBuf := make([]byte, 0, expectedKeyLen)
	valBuf := make([]byte, 0, 128)
	for reader.HasNext() {
		key, _ := reader.Next(keyBuf[:0])
		if !reader.HasNext() {
			break
		}
		val, _ := reader.Next(valBuf[:0])
		if len(key) == expectedKeyLen {
			values[string(key)] = common.Copy(val)
		}
	}
	return values, nil
}

func deriveDecompAndReaderForOtherDomain(baseFile string, oldDomain, newDomain kv.Domain) (*seg.Decompressor, *seg.Reader, func(), error) {
	fileVersionMask, err := version.ReplaceVersionWithMask(baseFile)
	if err != nil {
		return nil, nil, nil, err
	}
	fileVersionMask = strings.Replace(fileVersionMask, oldDomain.String(), newDomain.String(), 1)
	newFile, _, ok, err := version.FindFilesWithVersionsByPattern(fileVersionMask)
	if err != nil {
		return nil, nil, nil, err
	}
	if !ok {
		return nil, nil, nil, fmt.Errorf("could not derive reader for other domain due to file not found: %s,%s->%s", baseFile, oldDomain, newDomain)
	}
	decomp, err := seg.NewDecompressor(newFile)
	if err != nil {
		return nil, nil, nil, err
	}
	compression := statecfg.Schema.GetDomainCfg(newDomain).Compression
	return decomp, seg.NewReader(decomp.MakeGetter(), compression), decomp.Close, nil
}

func touchHistoricalKeys(sd *execctx.SharedDomains, tx kv.TemporalTx, d kv.Domain, fromTxNum uint64, toTxNum uint64, visitor func(k []byte)) (uint64, error) {
	// toTxNum is exclusive per kv.TemporalTx.HistoryRange contract [from,to)
	stream, err := tx.HistoryRange(d, int(fromTxNum), int(toTxNum), order.Asc, -1)
	if err != nil {
		return 0, err
	}
	defer stream.Close()
	var touches uint64
	for stream.HasNext() {
		k, _, err := stream.Next()
		if err != nil {
			return 0, err
		}
		if visitor != nil {
			visitor(k)
		}
		sd.GetCommitmentCtx().TouchKey(d, string(k), nil)
		touches++
	}
	return touches, nil
}
