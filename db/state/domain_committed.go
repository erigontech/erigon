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

// CommitmentFileMayContainRef reports whether a commitment .kv file at this version
// may contain shortened referenced keys. v2.0 (and earlier) produced refs above the
// step threshold when AggregatorSqueezeCommitmentValues was on; v2.1+ are noref-only.
func CommitmentFileMayContainRef(dataVer version.Version) bool {
	return dataVer.Less(version.V2_1)
}

// replaceShortenedKeysInBranch expands shortened key references (file offsets) in branch data back to full keys
// by looking them up in the account and storage domain files.
func (at *AggregatorRoTx) replaceShortenedKeysInBranch(prefix []byte, branch commitment.BranchData, fStartTxNum uint64, fEndTxNum uint64) (commitment.BranchData, error) {
	logger := log.Root()
	aggTx := at

	// Independent of the writer-side ReplaceKeysInValues flag: a noref-configured node may
	// still read a referenced-key file produced by an older datadir, so the read path can't
	// skip on config alone. Below-threshold ranges never contained refs in any version.
	if len(branch) == 0 || bytes.Equal(prefix, commitmentdb.KeyCommitmentState) ||
		aggTx.TxNumsInFiles(kv.StateDomains...) == 0 || !ValuesPlainKeyReferencingThresholdReached(at.StepSize(), fStartTxNum, fEndTxNum) {

		return branch, nil // do not transform, return as is
	}

	com := aggTx.d[kv.CommitmentDomain]
	comItem, err := com.lookupVisibleFileByRange(fStartTxNum, fEndTxNum)
	if err != nil {
		logger.Crit("dereference key during commitment read", "failed", err.Error())
		return nil, err
	}
	// Fast path: v2.1+ commitment files are guaranteed noref — skip the per-key walk entirely.
	if !CommitmentFileMayContainRef(comItem.dataVer) {
		return branch, nil
	}

	sto := aggTx.d[kv.StorageDomain]
	acc := aggTx.d[kv.AccountsDomain]
	metricI := 0
	for i, f := range com.files {
		if i > 5 {
			metricI = 5
			break
		}
		if f.startTxNum == fStartTxNum && f.endTxNum == fEndTxNum {
			metricI = i
		}
	}

	// Getters resolve on first referenced-key hit, per domain. Branches with only full keys
	// (the common case once ReplaceKeysInValues is off) skip file resolution entirely.
	var storageGetter, accountGetter *seg.Reader

	result, err := branch.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			if len(key) == length.Addr+length.Hash {
				return nil, nil // save storage key as is
			}
			if dbg.KVReadLevelledMetrics {
				defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
			}
			if storageGetter == nil {
				item, err := sto.lookupVisibleFileByRange(fStartTxNum, fEndTxNum)
				if err != nil {
					logger.Crit("dereference key during commitment read", "failed", err.Error())
					return nil, err
				}
				storageGetter = sto.dataReader(item.decompressor)
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
		if accountGetter == nil {
			item, err := acc.lookupVisibleFileByRange(fStartTxNum, fEndTxNum)
			if err != nil {
				logger.Crit("dereference key during commitment read", "failed", err.Error())
				return nil, err
			}
			accountGetter = acc.dataReader(item.decompressor)
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

// commitmentValTransformDomain returns a per-value transform for rewriting commitment values.
//
// produceRefs=false (merge path): deref any source-side refs back to full keys; pass full-source
// values through unchanged. Output is always noref. Used when merging into a v2.1 destination.
// mergedAccount/mergedStorage are unused in this mode.
//
// produceRefs=true (squeeze path): deref any source-side refs, then re-encode each key as a ref
// against the merged account/storage file (or pass through as full if the merged file has no
// matching record). Output is the legacy referenced encoding. Used by SqueezeCommitmentFiles.
//
// Per-source-range fast path: if source commitment file is v2.1+ (noref) and produceRefs=false,
// the value passes through without walking keys.
func (dt *DomainRoTx) commitmentValTransformDomain(rng MergeRange, accounts, storage *DomainRoTx, mergedAccount, mergedStorage *FilesItem, produceRefs bool) (valueTransformer, error) {
	if !rng.needMerge {
		panic(fmt.Sprintf("assert: commitmentValTransformDomain called with domain.needMerge=false (from=%d to=%d): caller must guard with values.needMerge", rng.from, rng.to))
	}

	var ms, ma *seg.Reader
	if produceRefs {
		var err error
		if mergedStorage == nil {
			if mergedStorage, err = storage.lookupVisibleFileByRange(rng.from, rng.to); err != nil {
				return nil, err
			}
		}
		if mergedAccount == nil {
			if mergedAccount, err = accounts.lookupVisibleFileByRange(rng.from, rng.to); err != nil {
				return nil, err
			}
		}
		ms = storage.dataReader(mergedStorage.decompressor)
		ma = accounts.dataReader(mergedAccount.decompressor)
	}

	// Cache per source range (keyFromTxNum,keyEndTxNum): the source commitment file's data
	// version (so noref sources skip the per-key walk in the merge path), plus account/storage
	// getters resolved on first referenced-key hit.
	type srcCache struct {
		srcVerKnown   bool
		mayContainRef bool
		accountGetter *seg.Reader
		storageGetter *seg.Reader
	}
	perRange := make(map[uint64]map[uint64]*srcCache)
	getCache := func(from, to uint64) *srcCache {
		inner, ok := perRange[from]
		if !ok {
			inner = make(map[uint64]*srcCache)
			perRange[from] = inner
		}
		c, ok := inner[to]
		if !ok {
			c = &srcCache{}
			inner[to] = c
		}
		return c
	}

	vt := func(valBuf []byte, keyFromTxNum, keyEndTxNum uint64) (transValBuf []byte, err error) {
		if len(valBuf) == 0 {
			return valBuf, nil
		}
		// Below-threshold source ranges never produced refs in any version, and refs are not
		// produced below threshold either — nothing to do regardless of produceRefs.
		if !ValuesPlainKeyReferencingThresholdReached(dt.d.stepSize, keyFromTxNum, keyEndTxNum) {
			return valBuf, nil
		}
		c := getCache(keyFromTxNum, keyEndTxNum)
		if !c.srcVerKnown {
			srcCom, err := dt.lookupVisibleFileByRange(keyFromTxNum, keyEndTxNum)
			if err != nil {
				return nil, fmt.Errorf("commitment src file not found: %w", err)
			}
			c.mayContainRef = CommitmentFileMayContainRef(srcCom.dataVer)
			c.srcVerKnown = true
		}
		// Pass-through only when source has no refs AND we're not emitting refs.
		if !c.mayContainRef && !produceRefs {
			return valBuf, nil
		}

		// derefStorage / derefAccount return the full key, dereffing source refs if needed.
		derefStorage := func(key []byte) ([]byte, error) {
			if len(key) == length.Addr+length.Hash {
				return key, nil
			}
			if c.storageGetter == nil {
				f, err := storage.lookupVisibleFileByRange(keyFromTxNum, keyEndTxNum)
				if err != nil {
					return nil, fmt.Errorf("storage src file not found: %w", err)
				}
				c.storageGetter = storage.dataReader(f.decompressor)
			}
			fullKey, found := storage.lookupByShortenedKey(key, c.storageGetter)
			if !found {
				dt.d.logger.Crit("valTransform: lost storage full key",
					"shortened", hex.EncodeToString(key),
					"src", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
					"merging", rng.String("", dt.d.stepSize),
					"valBuf", fmt.Sprintf("l=%d %x", len(valBuf), valBuf),
				)
				return nil, fmt.Errorf("lookup lost storage full key %x", key)
			}
			return fullKey, nil
		}
		derefAccount := func(key []byte) ([]byte, error) {
			if len(key) == length.Addr {
				return key, nil
			}
			if c.accountGetter == nil {
				f, err := accounts.lookupVisibleFileByRange(keyFromTxNum, keyEndTxNum)
				if err != nil {
					return nil, fmt.Errorf("account src file not found: %w", err)
				}
				c.accountGetter = accounts.dataReader(f.decompressor)
			}
			fullKey, found := accounts.lookupByShortenedKey(key, c.accountGetter)
			if !found {
				dt.d.logger.Crit("valTransform: lost account full key",
					"shortened", hex.EncodeToString(key),
					"src", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
					"merging", rng.String("", dt.d.stepSize),
					"valBuf", fmt.Sprintf("l=%d %x", len(valBuf), valBuf),
				)
				return nil, fmt.Errorf("lookup account full key: %x", key)
			}
			return fullKey, nil
		}

		replacer := func(key []byte, isStorage bool) ([]byte, error) {
			if isStorage {
				fullKey, err := derefStorage(key)
				if err != nil {
					return nil, err
				}
				if !produceRefs {
					if len(key) == length.Addr+length.Hash {
						return nil, nil // unchanged full key
					}
					return fullKey, nil
				}
				// produceRefs: re-encode against merged storage file.
				off, found := storage.findShortenedKey(fullKey, ms, mergedStorage)
				if !found {
					if len(fullKey) == length.Addr+length.Hash {
						return fullKey, nil // lost shortening — save full key
					}
					dt.d.logger.Crit("valTransform: replacement for full storage key was not found",
						"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
						"toReplace", hex.EncodeToString(fullKey))
					return nil, fmt.Errorf("replacement not found for storage %x", fullKey)
				}
				return EncodeReferenceKey(nil, off), nil
			}

			fullKey, err := derefAccount(key)
			if err != nil {
				return nil, err
			}
			if !produceRefs {
				if len(key) == length.Addr {
					return nil, nil
				}
				return fullKey, nil
			}
			off, found := accounts.findShortenedKey(fullKey, ma, mergedAccount)
			if !found {
				if len(fullKey) == length.Addr {
					return fullKey, nil
				}
				dt.d.logger.Crit("valTransform: replacement for full account key was not found",
					"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
					"toReplace", hex.EncodeToString(fullKey))
				return nil, fmt.Errorf("replacement not found for account %x", fullKey)
			}
			return EncodeReferenceKey(nil, off), nil
		}

		branchData, err := commitment.BranchData(valBuf).ReplacePlainKeys(nil, replacer)
		if err != nil {
			return nil, err
		}
		return branchData, nil
	}

	return vt, nil
}
