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
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
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
	txNumReader := br.TxnumReader(ctx)
	blockMinTxNum, err := txNumReader.Min(tx, blockNum)
	if err != nil {
		return info, err
	}
	blockMaxTxNum, err := txNumReader.Max(tx, blockNum)
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
	sd, err := execctx.NewSharedDomains(tx, logger)
	if err != nil {
		return nil, err
	}
	sd.GetCommitmentCtx().SetTrace(logger.Enabled(ctx, log.LvlTrace))
	sd.GetCommitmentCtx().SetLimitedHistoryStateReader(tx, maxTxNum) // to use tx.Debug().GetLatestFromFiles with maxTxNum
	err = sd.SeekCommitment(ctx, tx)                                 // seek commitment again to use the new state reader instead
	if err != nil {
		return nil, err
	}
	if sd.TxNum() > maxTxNum {
		return nil, fmt.Errorf("%w: commitment root sd txNum should is gt maxTxNum: %d > %d", ErrIntegrity, sd.TxNum(), maxTxNum)
	}
	if sd.TxNum() > info.blockMaxTxNum {
		return nil, fmt.Errorf("%w: commitment root sd txNum should is gt blockMaxTxNum: %d > %d", ErrIntegrity, sd.TxNum(), info.blockMaxTxNum)
	}
	if sd.TxNum() < info.blockMinTxNum {
		return nil, fmt.Errorf("%w: commitment root sd txNum should is lt blockMinTxNum: %d < %d", ErrIntegrity, sd.TxNum(), info.blockMinTxNum)
	}
	if sd.TxNum() == 0 {
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
		logger.Debug("account touch for root block", "key", common.Address(k), "blockNum", sd.BlockNum(), "file", filepath.Base(f.Fullpath()))
	}
	touches, err := touchHistoricalKeys(sd, tx, kv.AccountsDomain, info.blockMinTxNum, info.txNum+1, touchLoggingVisitor)
	if err != nil {
		return err
	}
	logger.Info("recomputing commitment root after", "touches", touches, "file", filepath.Base(f.Fullpath()))
	recomputedBytes, err := sd.ComputeCommitment(ctx, tx, false /* saveStateAfter */, sd.BlockNum(), sd.TxNum(), "integrity", nil)
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
	var integrityErr error
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
	return integrityErr
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
			"checking commitment deref skipped, file not above min steps",
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
	branchKeyBuf := make([]byte, 0, 128)
	branchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	newBranchValueBuf := make([]byte, 0, datasize.MB.Bytes())
	plainKeyBuf := make([]byte, 0, length.Addr+length.Hash)
	var counts derefCounts
	var integrityErr error
	for commReader.HasNext() {
		select {
		case <-ctx.Done():
			return derefCounts{}, ctx.Err()
		case <-logTicker.C:
			at := fmt.Sprintf("%d/%d", counts.branchKeys, totalKeys)
			percent := fmt.Sprintf("%.1f%%", float64(counts.branchKeys)/float64(totalKeys)*100)
			rate := float64(counts.branchKeys) / time.Since(start).Seconds()
			eta := time.Duration(float64(totalKeys-counts.branchKeys)/rate) * time.Second
			logger.Info(
				"checking commitment deref progress",
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
	txNumsReader := br.TxnumReader(ctx)
	minTxNum, err := txNumsReader.Min(tx, blockNum)
	if err != nil {
		return err
	}
	maxTxNum, err := txNumsReader.Max(tx, blockNum)
	if err != nil {
		return err
	}
	toTxNum := maxTxNum + 1
	sd, err := execctx.NewSharedDomains(tx, logger)
	if err != nil {
		return err
	}
	sd.GetCommitmentCtx().SetHistoryStateReader(tx, toTxNum)
	sd.GetCommitmentCtx().SetTrace(logger.Enabled(ctx, log.LvlTrace))
	err = sd.SeekCommitment(ctx, tx) // seek commitment again with new history state reader
	if err != nil {
		return err
	}
	if sd.BlockNum() != blockNum {
		return fmt.Errorf("commitment state blockNum doesn't match blockNum: %d != %d", sd.BlockNum(), blockNum)
	}
	if sd.TxNum() != maxTxNum {
		return fmt.Errorf("commitment state txNum doesn't match maxTxNum: %d != %d", sd.TxNum(), maxTxNum)
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
	for blockNum := from; blockNum < to; blockNum++ {
		err := CheckCommitmentHistAtBlk(ctx, db, br, blockNum, logger)
		if err != nil {
			return err
		}
	}
	dur := time.Since(start)
	blks := to - from
	rate := float64(blks) / dur.Seconds()
	logger.Info("checked commitment hist at blk range", "dur", dur, "blks", blks, "blks/s", rate, "from", from, "to", to)
	return nil
}

func touchHistoricalKeys(sd *execctx.SharedDomains, tx kv.TemporalTx, d kv.Domain, fromTxNum uint64, toTxNum uint64, visitor func(k []byte)) (uint64, error) {
	stream, err := tx.HistoryRange(d, int(fromTxNum), int(toTxNum)+1, order.Asc, -1)
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
