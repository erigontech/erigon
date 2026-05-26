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

const minStepsForReferencing = 2

// ValuesPlainKeyReferencingThresholdReached checks if the range from..to is large enough to use plain key referencing
// Used for commitment branches - to store references to account and storage keys as shortened keys (file offsets)
func ValuesPlainKeyReferencingThresholdReached(stepSize, from, to uint64) bool {
	return (to-from)/stepSize >= minStepsForReferencing
}

// commitmentBranchReferenced reports whether a commitment file written at fileVersion over
// range from..to carries shortened key references that must be expanded on read. It is a
// property of the file itself (version + range), independent of the live write flag: files
// below v2.1 in the referenced regime carry references once the range reaches the threshold;
// v2.1 files are always plain.
func commitmentBranchReferenced(fileVersion version.Version, stepSize, from, to uint64) bool {
	return fileVersion.Less(version.V2_1) && ValuesPlainKeyReferencingThresholdReached(stepSize, from, to)
}

// commitmentVisibleFilesReferenced reports whether any visible commitment file is referenced.
// Used at merge-range planning, before inputs are resolved, so it over-approximates across all
// visible commitment files.
func (at *AggregatorRoTx) commitmentVisibleFilesReferenced() bool {
	stepSize := at.StepSize()
	for _, f := range at.d[kv.CommitmentDomain].files {
		if commitmentBranchReferenced(f.Version(), stepSize, f.startTxNum, f.endTxNum) {
			return true
		}
	}
	return false
}

// commitmentMergeInputsReferenced reports whether any resolved commitment merge input is
// referenced and therefore needs key expansion during the merge.
func commitmentMergeInputsReferenced(inputs []*FilesItem, stepSize uint64) bool {
	for _, f := range inputs {
		if f == nil {
			continue
		}
		if commitmentBranchReferenced(f.version, stepSize, f.startTxNum, f.endTxNum) {
			return true
		}
	}
	return false
}

// commitmentFileVersionByRange returns the parsed version of the visible commitment file
// covering from..to, plus the metric bucket index for that file. A missing file yields the
// zero version (treated as referenced) to preserve the historical deref behavior.
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
// by looking them up in the account and storage domain files.
func (at *AggregatorRoTx) replaceShortenedKeysInBranch(prefix []byte, branch commitment.BranchData, fStartTxNum uint64, fEndTxNum uint64) (commitment.BranchData, error) {
	logger := log.Root()
	aggTx := at

	if len(branch) == 0 || bytes.Equal(prefix, commitmentdb.KeyCommitmentState) ||
		aggTx.TxNumsInFiles(kv.StateDomains...) == 0 {

		return branch, nil // do not transform, return as is
	}

	fileVersion, metricI := aggTx.commitmentFileVersionByRange(fStartTxNum, fEndTxNum)
	if !commitmentBranchReferenced(fileVersion, at.StepSize(), fStartTxNum, fEndTxNum) {
		return branch, nil // input file was written plain (v2.1) or below the referencing threshold
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
	storageGetter := sto.dataReader(storageItem.decompressor)
	accountGetter := acc.dataReader(accountItem.decompressor)

	result, err := branch.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			if len(key) == length.Addr+length.Hash {
				return nil, nil // save storage key as is
			}
			if dbg.KVReadLevelledMetrics {
				defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
			}
			// Optimised key referencing a state file record (file number and offset within the file)
			storagePlainKey, found := sto.lookupByShortenedKey(key, storageGetter)
			if !found {
				s0, s1 := fStartTxNum/at.StepSize(), fEndTxNum/at.StepSize()
				logger.Crit("replace back lost storage full key", "shortened", hex.EncodeToString(key),
					"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, DecodeReferenceKey(key)))
				return nil, fmt.Errorf("replace back lost storage full key: %x", key)
			}
			return storagePlainKey, nil
		}

		if len(key) == length.Addr {
			return nil, nil // save account key as is
		}

		if dbg.KVReadLevelledMetrics {
			defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
		}
		apkBuf, found := acc.lookupByShortenedKey(key, accountGetter)
		if !found {
			s0, s1 := fStartTxNum/at.StepSize(), fEndTxNum/at.StepSize()
			logger.Crit("replace back lost account full key", "shortened", hex.EncodeToString(key),
				"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, DecodeReferenceKey(key)))
			return nil, fmt.Errorf("replace back lost account full key: %x", key)
		}
		return apkBuf, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
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

// fileVersionByRange returns the parsed version of the visible file covering from..to.
// A missing file yields the zero version, which the referencing predicate treats as
// referenced — preserving the historical expand-on-merge behavior.
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
func (dt *DomainRoTx) commitmentValTransformDomain(rng MergeRange, accounts, storage *DomainRoTx, mergedAccount, mergedStorage *FilesItem) (valueTransformer, error) {
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
	reshorten := dt.d.ReferencesInCommitmentBranches && ValuesPlainKeyReferencingThresholdReached(dt.d.stepSize, rng.from, rng.to)

	vt := func(valBuf []byte, keyFromTxNum, keyEndTxNum uint64) (transValBuf []byte, err error) {
		if len(valBuf) == 0 {
			return valBuf, nil
		}
		// Expand the input's short keys to plain whenever the input file was written referenced
		// (its own version+range), independent of the live flag — otherwise a referenced input
		// merged with the flag off would copy stale offsets into the merged file.
		inputReferenced := commitmentBranchReferenced(dt.fileVersionByRange(keyFromTxNum, keyEndTxNum), dt.d.stepSize, keyFromTxNum, keyEndTxNum)
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

				shortenedKeyOffset, found := storage.findShortenedKey(auxBuf, ms, mergedStorage)
				if !found {
					if len(auxBuf) == length.Addr+length.Hash {
						return auxBuf, nil // if plain key is lost, we can save original fullkey
					}
					// if shortened key lost, we can't continue
					dt.d.logger.Crit("valTransform: replacement for full storage key was not found",
						"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
						"shortened", hex.EncodeToString(shortened), "toReplace", hex.EncodeToString(auxBuf))

					return nil, fmt.Errorf("replacement not found for storage %x", auxBuf)
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

			shortenedKeyOffset, found := accounts.findShortenedKey(auxBuf, ma, mergedAccount)
			if !found {
				if len(auxBuf) == length.Addr {
					return auxBuf, nil // if plain key is lost, we can save original fullkey
				}
				dt.d.logger.Crit("valTransform: replacement for full account key was not found",
					"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
					"shortened", hex.EncodeToString(shortened), "toReplace", hex.EncodeToString(auxBuf))
				return nil, fmt.Errorf("replacement not found for account  %x", auxBuf)
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
