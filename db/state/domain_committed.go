// Copyright 2021 The Erigon Authors
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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// ValuesPlainKeyReferencingThresholdReached checks if the range from..to is large enough to use plain key referencing
// Used for commitment branches - to store references to account and storage keys as shortened keys (file offsets)
func ValuesPlainKeyReferencingThresholdReached(stepSize, from, to uint64) bool {
	return (to-from)/stepSize >= commitment.DefaultKeyReferencingMinSteps
}

// CommitmentBranchReferenced reports whether a commitment file at fileVersion over from..to carries
// shortened key references — a property of the file (version+range), independent of the live write flag.
func CommitmentBranchReferenced(fileVersion version.Version, stepSize, from, to uint64) bool {
	return fileVersion.Less(version.V2_2) && ValuesPlainKeyReferencingThresholdReached(stepSize, from, to)
}

// commitmentVisibleFilesReferenced reports whether any visible commitment file is referenced.
func (at *AggregatorRoTx) commitmentVisibleFilesReferenced() bool {
	stepSize := at.StepSize()
	for _, f := range at.d[kv.CommitmentDomain].files {
		if CommitmentBranchReferenced(f.Version(), stepSize, f.startTxNum, f.endTxNum) {
			return true
		}
	}
	return false
}

// commitmentMergeInputsReferenced reports whether any commitment merge input is referenced.
func commitmentMergeInputsReferenced(inputs []*FilesItem, stepSize uint64) bool {
	for _, f := range inputs {
		if f == nil {
			continue
		}
		if CommitmentBranchReferenced(f.version, stepSize, f.startTxNum, f.endTxNum) {
			return true
		}
	}
	return false
}

// commitmentMergeNeedsTransform reports whether a commitment merge needs a value transformer — and
// thus must wait for the account/storage merges. True when an input must be expanded (any input
// referenced) or the output must be re-shortened (refsEnabled and output range >= threshold). Plain
// inputs merged without re-shortening need neither, so they run in parallel with account/storage.
func commitmentMergeNeedsTransform(inputs []*FilesItem, refsEnabled bool, stepSize, from, to uint64) bool {
	reshorten := refsEnabled && ValuesPlainKeyReferencingThresholdReached(stepSize, from, to)
	return reshorten || commitmentMergeInputsReferenced(inputs, stepSize)
}

// commitmentFileVersionByRange returns the version of the commitment file covering from..to
// (zero if missing, treated as referenced) and its metric bucket index.
func (at *AggregatorRoTx) commitmentFileVersionByRange(from, to uint64) (version.Version, int) {
	for i, f := range at.d[kv.CommitmentDomain].files {
		if f.startTxNum == from && f.endTxNum == to {
			if i > 5 {
				return f.Version(), 5
			}
			return f.Version(), i
		}
	}
	return version.Version{}, 0
}

// replaceShortenedKeysInBranch expands shortened key references (file offsets) in branch data back to full keys
// by looking them up in the account and storage domain files. It guards the call to
// ExpandShortenedKeysInBranch with the read-path preconditions (empty branch, KeyCommitmentState
// carve-out, files-not-empty) and the per-file version gate (referenced iff version < v2.2 and range >= threshold).
func (at *AggregatorRoTx) replaceShortenedKeysInBranch(prefix []byte, branch commitment.BranchData, fStartTxNum uint64, fEndTxNum uint64) (commitment.BranchData, error) {
	logger := log.Root()
	aggTx := at

	if len(branch) == 0 || bytes.Equal(prefix, commitmentdb.KeyCommitmentState) ||
		aggTx.TxNumsInFiles(kv.StateDomains...) == 0 {

		return branch, nil // do not transform, return as is
	}

	fileVersion, metricI := aggTx.commitmentFileVersionByRange(fStartTxNum, fEndTxNum)
	if !CommitmentBranchReferenced(fileVersion, at.StepSize(), fStartTxNum, fEndTxNum) {
		return branch, nil // input file was written plain (v2.2) or below the referencing threshold
	}

	sto := aggTx.d[kv.StorageDomain]
	acc := aggTx.d[kv.AccountsDomain]
	storageItem, err := sto.lookupVisibleFileByRange(fStartTxNum, fEndTxNum)
	if err != nil {
		logger.Crit("dereference key during commitment read", "failed", err.Error())
		return nil, err
	}
	accountItem, err := acc.lookupVisibleFileByRange(fStartTxNum, fEndTxNum)
	if err != nil {
		logger.Crit("dereference key during commitment read", "failed", err.Error())
		return nil, err
	}

	if dbg.KVReadLevelledMetrics {
		defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
	}

	return ExpandShortenedKeysInBranch(branch, acc, sto, accountItem, storageItem, fStartTxNum, fEndTxNum)
}

func DecodeReferenceKey(from []byte) uint64 {
	of, n := binary.Uvarint(from)
	if n == 0 {
		panic(fmt.Sprintf("reference key %x decode failed", from))
	}
	if n < 0 {
		panic(fmt.Sprintf("reference key %x overflow", from))
	}
	return of
}

func EncodeReferenceKey(buf []byte, offset uint64) []byte {
	if cap(buf) == 0 {
		buf = make([]byte, 0, 8)
	}
	return binary.AppendUvarint(buf[:0], offset)
}

// Finds shorter replacement for full key in given file item. filesItem -- result of merging of multiple files.
// If item is nil, or shorter key was not found, or anything else goes wrong, nil key and false returned.
func (dt *DomainRoTx) findShortenedKey(fullKey []byte, itemGetter *seg.Reader, item *FilesItem) (offset uint64, found bool) {
	if item == nil {
		return 0, false
	}
	if !strings.Contains(item.decompressor.FileName(), dt.d.FilenameBase) {
		panic(fmt.Sprintf("findShortenedKeyEasier of %s called with merged file %s", dt.d.FilenameBase, item.decompressor.FileName()))
	}
	if /*assert.Enable && */ itemGetter.FileName() != item.decompressor.FileName() {
		panic(fmt.Sprintf("findShortenedKey of %s itemGetter (%s) is different to item.decompressor (%s)",
			dt.d.FilenameBase, itemGetter.FileName(), item.decompressor.FileName()))
	}

	//if idxList&withExistence != 0 {
	//	hi, _ := dt.ht.iit.hashKey(fullKey)
	//	if !item.existence.ContainsHash(hi) {
	//		continue
	//	}
	//}

	if dt.d.Accessors.Has(statecfg.AccessorHashMap) {
		reader := recsplit.NewIndexReader(item.index)
		defer reader.Close()

		offset, ok := reader.Lookup(fullKey)
		if !ok {
			return 0, false
		}

		itemGetter.Reset(offset)
		if !itemGetter.HasNext() {
			dt.d.logger.Warn("commitment branch key replacement seek failed",
				"key", hex.EncodeToString(fullKey), "idx", "hash", "file", item.decompressor.FileName())
			return 0, false
		}

		k, _ := itemGetter.Next(nil)
		if !bytes.Equal(fullKey, k) {
			dt.d.logger.Warn("commitment branch key replacement seek invalid key",
				"key", hex.EncodeToString(fullKey), "idx", "hash", "file", item.decompressor.FileName())

			return 0, false
		}
		return offset, true
	}
	if dt.d.Accessors.Has(statecfg.AccessorBTree) {
		if item.bindex == nil {
			dt.d.logger.Warn("[agg] commitment branch key replacement: file doesn't have index", "name", item.decompressor.FileName())
		}
		_, _, offset, ok, err := item.bindex.Get(fullKey, itemGetter)
		if err != nil {
			dt.d.logger.Warn("[agg] commitment branch key replacement seek failed",
				"key", hex.EncodeToString(fullKey), "idx", "bt", "err", err, "file", item.decompressor.FileName())
		}
		if !ok {
			return 0, false
		}
		return offset, true
	}
	return 0, false
}

// fileVersionByRange returns the version of the visible file covering from..to (zero if missing, treated as referenced).
func (dt *DomainRoTx) fileVersionByRange(from, to uint64) version.Version {
	for _, f := range dt.files {
		if f.startTxNum == from && f.endTxNum == to {
			return f.Version()
		}
	}
	return version.Version{}
}

// lookupVisibleFileByRange searches only among visible files (those in the RoTx snapshot).
// Use this during merge operations where constituent files are guaranteed to be visible.
func (dt *DomainRoTx) lookupVisibleFileByRange(txFrom, txTo uint64) (*FilesItem, error) {
	for _, f := range dt.files {
		if f.startTxNum == txFrom && f.endTxNum == txTo && f.src != nil {
			return f.src, nil
		}
	}
	return nil, fmt.Errorf("file %s-%s.%d-%d.kv not found in visible files",
		dt.d.FileVersion.DataKV.String(), dt.d.FilenameBase, txFrom/dt.d.stepSize, txTo/dt.d.stepSize)
}

// rawLookupFileByRange searches visible files first, then falls back to dirty files.
// Used by the offline commitment converter, which operates on freshly built files that
// may still be dirty (not yet integrated into the visible RoTx snapshot).
func (dt *DomainRoTx) rawLookupFileByRange(txFrom uint64, txTo uint64) (*FilesItem, error) {
	for _, f := range dt.files {
		if f.startTxNum == txFrom && f.endTxNum == txTo && f.src != nil {
			return f.src, nil // found in visible files
		}
	}
	if dirty := dt.lookupDirtyFileByItsRange(txFrom, txTo); dirty != nil {
		return dirty, nil
	}
	return nil, fmt.Errorf("file %s-%s.%d-%d.kv was not found", dt.d.FileVersion.DataKV.String(), dt.d.FilenameBase, txFrom/dt.d.stepSize, txTo/dt.d.stepSize)
}

func (dt *DomainRoTx) lookupDirtyFileByItsRange(txFrom uint64, txTo uint64) *FilesItem {
	var item *FilesItem
	if item == nil {
		dt.d.dirtyFiles.Walk(func(files []*FilesItem) bool {
			for _, f := range files {
				if f.startTxNum == txFrom && f.endTxNum == txTo {
					item = f
					return false
				}
			}
			return true
		})
	}

	if item == nil || item.bindex == nil {
		var fileStepsss strings.Builder
		fileStepsss.WriteString("" + dt.d.Name.String() + ": ")
		for _, item := range dt.d.dirtyFiles.Items() {
			fromStep, toStep := item.StepRange(dt.d.stepSize)
			fileStepsss.WriteString(fmt.Sprintf("%d-%d;", fromStep, toStep))
		}
		dt.d.logger.Warn("[agg] lookupDirtyFileByItsRange: file not found",
			"stepFrom", txFrom/dt.d.stepSize, "stepTo", txTo/dt.d.stepSize,
			"files", fileStepsss.String(), "filesCount", dt.d.dirtyFiles.Len())

		if item != nil && item.bindex == nil {
			dt.d.logger.Warn("[agg] lookupDirtyFileByItsRange: file found but not indexed", "f", item.decompressor.FileName())
		}

		return nil
	}
	return item
}

// searches in given list of files for a key or searches in domain files if list is empty
func (dt *DomainRoTx) lookupByShortenedKey(shortKey []byte, getter *seg.Reader) (fullKey []byte, found bool) {
	if len(shortKey) < 1 {
		return nil, false
	}
	offset := DecodeReferenceKey(shortKey)
	defer func() {
		if r := recover(); r != nil {
			dt.d.logger.Crit("lookupByShortenedKey panics",
				"err", r,
				"offset", offset, "short", hex.EncodeToString(shortKey),
				"visibleFilesCount", len(dt.files),
				"file", getter.FileName())
		}
	}()

	//getter := NewArchiveGetter(item.decompressor.MakeGetter(), dt.d.Compression)
	getter.Reset(offset)
	n := getter.HasNext()
	if !n || uint64(getter.Size()) <= offset {
		dt.d.logger.Warn("lookupByShortenedKey failed", "file", getter.FileName(), "short", hex.EncodeToString(shortKey), "offset", offset, "hasNext", n, "size", getter.Size(), "offsetBigger", uint64(getter.Size()) <= offset)
		return nil, false
	}

	dt.lookupFullKey, _ = getter.Next(dt.lookupFullKey[:0])
	return dt.lookupFullKey, true
}

// commitmentValTransform parses the value of the commitment record to extract references
// to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// the updated references
func (dt *DomainRoTx) commitmentValTransformDomain(rng MergeRange, accounts, storage *DomainRoTx, mergedAccount, mergedStorage *FilesItem, refsEnabled bool) (valueTransformer, error) {
	if !rng.needMerge {
		panic(fmt.Sprintf("assert: commitmentValTransformDomain called with domain.needMerge=false (from=%d to=%d): caller must guard with values.needMerge", rng.from, rng.to))
	}
	var keyBuf [60]byte // 52b key and 8b for inverted step
	var err error
	shortened := make([]byte, 16)

	hadToLookupStorage := mergedStorage == nil
	if mergedStorage == nil {
		if mergedStorage, err = storage.lookupVisibleFileByRange(rng.from, rng.to); err != nil {
			// TODO may allow to merge, but storage keys will be stored as plainkeys
			return nil, err
		}
	}

	hadToLookupAccount := mergedAccount == nil
	if mergedAccount == nil {
		if mergedAccount, err = accounts.lookupVisibleFileByRange(rng.from, rng.to); err != nil {
			return nil, err
		}
	}

	dr := DomainRanges{values: rng}
	accountFileMap := make(map[uint64]map[uint64]*seg.Reader)
	if accountList, _, _ := accounts.staticFilesInRange(dr); accountList != nil {
		for _, f := range accountList {
			if _, ok := accountFileMap[f.startTxNum]; !ok {
				accountFileMap[f.startTxNum] = make(map[uint64]*seg.Reader)
			}
			accountFileMap[f.startTxNum][f.endTxNum] = accounts.dataReader(f.decompressor)
		}
	}
	storageFileMap := make(map[uint64]map[uint64]*seg.Reader)
	if storageList, _, _ := storage.staticFilesInRange(dr); storageList != nil {
		for _, f := range storageList {
			if _, ok := storageFileMap[f.startTxNum]; !ok {
				storageFileMap[f.startTxNum] = make(map[uint64]*seg.Reader)
			}
			storageFileMap[f.startTxNum][f.endTxNum] = storage.dataReader(f.decompressor)
		}
	}

	ms := storage.dataReader(mergedStorage.decompressor)
	ma := accounts.dataReader(mergedAccount.decompressor)
	dt.d.logger.Debug("prepare commitmentValTransformDomain", "merge", rng.String("range", dt.d.stepSize), "Mstorage", hadToLookupStorage, "Maccount", hadToLookupAccount)

	// reshorten governs whether merged output keys are re-referenced (offsets into the merged
	// account/storage files). It is keyed off the live write flag and the OUTPUT range only;
	// input expansion below is keyed off each input file's own version+range instead.
	reshorten := refsEnabled && ValuesPlainKeyReferencingThresholdReached(dt.d.stepSize, rng.from, rng.to)

	// Per-merge caches for findShortenedKey (key → offset): the merged file is read-only here and
	// hot contracts recur across many branches in the same range, so caching avoids repeat B-tree
	// descents (findShortenedKey → BtIndex.Get dominated merge CPU). Bounded; live only for this merge.
	const cacheMaxEntries = 100_000
	storageKeyCache := make(map[string]uint64)
	accountKeyCache := make(map[string]uint64)
	// The referenced verdict recurs across every branch of an input file (same from,to) and each
	// lookup is a linear scan over visible files, so cache the verdict per range.
	referencedByRange := make(map[[2]uint64]bool)

	vt := func(valBuf []byte, keyFromTxNum, keyEndTxNum uint64) (transValBuf []byte, err error) {
		if len(valBuf) == 0 {
			return valBuf, nil
		}
		// Expand the input's short keys to plain whenever the input file was written referenced
		// (its own version+range), independent of the live flag — otherwise a referenced input
		// merged with the flag off would copy stale offsets into the merged file.
		rngKey := [2]uint64{keyFromTxNum, keyEndTxNum}
		inputReferenced, cached := referencedByRange[rngKey]
		if !cached {
			inputReferenced = CommitmentBranchReferenced(dt.fileVersionByRange(keyFromTxNum, keyEndTxNum), dt.d.stepSize, keyFromTxNum, keyEndTxNum)
			referencedByRange[rngKey] = inputReferenced
		}
		if !inputReferenced && !reshorten {
			return valBuf, nil
		}
		if _, ok := storageFileMap[keyFromTxNum]; !ok {
			storageFileMap[keyFromTxNum] = make(map[uint64]*seg.Reader)
		}
		sig, ok := storageFileMap[keyFromTxNum][keyEndTxNum]
		if !ok {
			f, err := storage.lookupVisibleFileByRange(keyFromTxNum, keyEndTxNum)
			if err != nil {
				return nil, fmt.Errorf("storage file not found in visible files: %w", err)
			}
			sig = storage.dataReader(f.decompressor)
			storageFileMap[keyFromTxNum][keyEndTxNum] = sig
		}

		if _, ok := accountFileMap[keyFromTxNum]; !ok {
			accountFileMap[keyFromTxNum] = make(map[uint64]*seg.Reader)
		}
		aig, ok := accountFileMap[keyFromTxNum][keyEndTxNum]
		if !ok {
			f, err := accounts.lookupVisibleFileByRange(keyFromTxNum, keyEndTxNum)
			if err != nil {
				return nil, fmt.Errorf("account file not found in visible files: %w", err)
			}
			aig = accounts.dataReader(f.decompressor)
			accountFileMap[keyFromTxNum][keyEndTxNum] = aig
		}

		replacer := func(key []byte, isStorage bool) ([]byte, error) {
			var found bool
			auxBuf := keyBuf[:0]
			if isStorage {
				plainKey := len(key) == length.Addr+length.Hash
				if plainKey {
					// Non-optimised key originating from a database record
					auxBuf = append(auxBuf[:0], key...)
				} else {
					// Optimised key referencing a state file record (file number and offset within the file)
					auxBuf, found = storage.lookupByShortenedKey(key, sig)
					if !found {
						dt.d.logger.Crit("valTransform: lost storage full key",
							"shortened", hex.EncodeToString(key),
							"merging", rng.String("", dt.d.stepSize),
							"valBuf", fmt.Sprintf("l=%d %x", len(valBuf), valBuf),
						)
						return nil, fmt.Errorf("lookup lost storage full key %x", key)
					}
				}
				if !reshorten {
					if plainKey {
						return nil, nil // leave the already-plain key in place
					}
					return auxBuf, nil // emit the expanded plain key
				}

				var shortenedKeyOffset uint64
				if cached, ok := storageKeyCache[string(auxBuf)]; ok {
					shortenedKeyOffset = cached
				} else {
					var found bool
					shortenedKeyOffset, found = storage.findShortenedKey(auxBuf, ms, mergedStorage)
					if !found {
						if len(auxBuf) == length.Addr+length.Hash {
							return auxBuf, nil // if plain key is lost, we can save original fullkey
						}
						// if shortened key lost, we can't continue
						dt.d.logger.Crit("valTransform: replacement for full storage key was not found",
							"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
							"shortened", hex.EncodeToString(key), "toReplace", hex.EncodeToString(auxBuf))

						return nil, fmt.Errorf("replacement not found for storage %x", auxBuf)
					}
					if len(storageKeyCache) >= cacheMaxEntries {
						clear(storageKeyCache)
					}
					storageKeyCache[string(auxBuf)] = shortenedKeyOffset
				}
				shortened = EncodeReferenceKey(shortened[:0], shortenedKeyOffset)
				return shortened, nil
			}

			plainKey := len(key) == length.Addr
			if plainKey {
				// Non-optimised key originating from a database record
				auxBuf = append(auxBuf[:0], key...)
			} else {
				auxBuf, found = accounts.lookupByShortenedKey(key, aig)
				if !found {
					dt.d.logger.Crit("valTransform: lost account full key",
						"shortened", hex.EncodeToString(key),
						"merging", rng.String("", dt.d.stepSize),
						"valBuf", fmt.Sprintf("l=%d %x", len(valBuf), valBuf),
					)
					return nil, fmt.Errorf("lookup account full key: %x", key)
				}
			}
			if !reshorten {
				if plainKey {
					return nil, nil // leave the already-plain key in place
				}
				return auxBuf, nil // emit the expanded plain key
			}

			var shortenedKeyOffset uint64
			if cached, ok := accountKeyCache[string(auxBuf)]; ok {
				shortenedKeyOffset = cached
			} else {
				var found bool
				shortenedKeyOffset, found = accounts.findShortenedKey(auxBuf, ma, mergedAccount)
				if !found {
					if len(auxBuf) == length.Addr {
						return auxBuf, nil // if plain key is lost, we can save original fullkey
					}
					dt.d.logger.Crit("valTransform: replacement for full account key was not found",
						"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
						"shortened", hex.EncodeToString(key), "toReplace", hex.EncodeToString(auxBuf))
					return nil, fmt.Errorf("replacement not found for account  %x", auxBuf)
				}
				if len(accountKeyCache) >= cacheMaxEntries {
					clear(accountKeyCache)
				}
				accountKeyCache[string(auxBuf)] = shortenedKeyOffset
			}
			shortened = EncodeReferenceKey(shortened[:0], shortenedKeyOffset)
			return shortened, nil
		}

		branchData, err := commitment.BranchData(valBuf).ReplacePlainKeys(nil, replacer)
		if err != nil {
			return nil, err
		}
		return branchData, nil
	}

	return vt, nil
}
