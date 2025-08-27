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
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

type ValueMerger func(prev, current []byte) (merged []byte, err error)

// TODO revisit encoded commitmentState.
//   - Add versioning
//   - add trie variant marker
//   - simplify decoding. Rn it's 3 embedded structure: RootNode encoded, Trie state encoded and commitmentState wrapper for search.
//     | search through states seems mostly useless so probably commitmentState should become header of trie state.
type commitmentState struct {
	txNum     uint64
	blockNum  uint64
	trieState []byte
}

func (cs *commitmentState) Decode(buf []byte) error {
	if len(buf) < 10 {
		return fmt.Errorf("ivalid commitment state buffer size %d, expected at least 10b", len(buf))
	}
	pos := 0
	cs.txNum = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	cs.blockNum = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	cs.trieState = make([]byte, binary.BigEndian.Uint16(buf[pos:pos+2]))
	pos += 2
	if len(cs.trieState) == 0 && len(buf) == 10 {
		return nil
	}
	copy(cs.trieState, buf[pos:pos+len(cs.trieState)])
	return nil
}

func (cs *commitmentState) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var v [18]byte
	binary.BigEndian.PutUint64(v[:], cs.txNum)
	binary.BigEndian.PutUint64(v[8:16], cs.blockNum)
	binary.BigEndian.PutUint16(v[16:18], uint16(len(cs.trieState)))
	if _, err := buf.Write(v[:]); err != nil {
		return nil, err
	}
	if _, err := buf.Write(cs.trieState); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sd *SharedDomains) GetCommitmentContext() *SharedDomainsCommitmentContext {
	return sd.sdCtx
}

// SeekCommitment lookups latest available commitment and sets it as current
func (sd *SharedDomains) SeekCommitment(ctx context.Context, tx kv.TemporalTx) (err error) {
	_, _, _, err = sd.sdCtx.SeekCommitment(ctx, tx)
	if err != nil {
		return err
	}
	return nil
}

func (sd *SharedDomains) ComputeCommitment(ctx context.Context, saveStateAfter bool, blockNum, txNum uint64, logPrefix string) (rootHash []byte, err error) {
	rootHash, err = sd.sdCtx.ComputeCommitment(ctx, saveStateAfter, blockNum, sd.txNum, logPrefix)
	return
}

// ValuesPlainKeyReferencingThresholdReached checks if the range from..to is large enough to use plain key referencing
// Used for commitment branches - to store references to account and storage keys as shortened keys (file offsets)
func ValuesPlainKeyReferencingThresholdReached(stepSize, from, to uint64) bool {
	const minStepsForReferencing = 2

	return ((to-from)/stepSize)%minStepsForReferencing == 0
}

// replaceShortenedKeysInBranch expands shortened key references (file offsets) in branch data back to full keys
// by looking them up in the account and storage domain files.
func (at *AggregatorRoTx) replaceShortenedKeysInBranch(prefix []byte, branch commitment.BranchData, fStartTxNum uint64, fEndTxNum uint64) (commitment.BranchData, error) {
	logger := log.Root()
	aggTx := at

	commitmentUseReferencedBranches := at.a.d[kv.CommitmentDomain].ReplaceKeysInValues
	if !commitmentUseReferencedBranches || len(branch) == 0 || bytes.Equal(prefix, keyCommitmentState) ||
		aggTx.TxNumsInFiles(kv.StateDomains...) == 0 || !ValuesPlainKeyReferencingThresholdReached(at.StepSize(), fStartTxNum, fEndTxNum) {

		return branch, nil // do not transform, return as is
	}

	sto := aggTx.d[kv.StorageDomain]
	acc := aggTx.d[kv.AccountsDomain]
	storageItem, err := sto.rawLookupFileByRange(fStartTxNum, fEndTxNum)
	if err != nil {
		logger.Crit("dereference key during commitment read", "failed", err.Error())
		return nil, err
	}
	accountItem, err := acc.rawLookupFileByRange(fStartTxNum, fEndTxNum)
	if err != nil {
		logger.Crit("dereference key during commitment read", "failed", err.Error())
		return nil, err
	}
	storageGetter := sto.dataReader(storageItem.decompressor)
	accountGetter := acc.dataReader(accountItem.decompressor)
	metricI := 0
	for i, f := range aggTx.d[kv.CommitmentDomain].files {
		if i > 5 {
			metricI = 5
			break
		}
		if f.startTxNum == fStartTxNum && f.endTxNum == fEndTxNum {
			metricI = i
		}
	}

	aux := make([]byte, 0, 256)
	return branch.ReplacePlainKeys(aux, func(key []byte, isStorage bool) ([]byte, error) {
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
					"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, decodeShorterKey(key)))
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
				"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, decodeShorterKey(key)))
			return nil, fmt.Errorf("replace back lost account full key: %x", key)
		}
		return apkBuf, nil
	})
}

func decodeShorterKey(from []byte) uint64 {
	of, n := binary.Uvarint(from)
	if n == 0 {
		panic(fmt.Sprintf("shorter key %x decode failed", from))
	}
	return of
}

func encodeShorterKey(buf []byte, offset uint64) []byte {
	if len(buf) == 0 {
		buf = make([]byte, 0, 8)
	}
	return binary.AppendUvarint(buf, offset)
}

// Finds shorter replacement for full key in given file item. filesItem -- result of merging of multiple files.
// If item is nil, or shorter key was not found, or anything else goes wrong, nil key and false returned.
func (dt *DomainRoTx) findShortenedKey(fullKey []byte, itemGetter *seg.Reader, item *FilesItem) (shortened []byte, found bool) {
	if item == nil {
		return nil, false
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

	if dt.d.Accessors.Has(0) {
		reader := recsplit.NewIndexReader(item.index)
		defer reader.Close()

		offset, ok := reader.Lookup(fullKey)
		if !ok {
			return nil, false
		}

		itemGetter.Reset(offset)
		if !itemGetter.HasNext() {
			dt.d.logger.Warn("commitment branch key replacement seek failed",
				"key", hex.EncodeToString(fullKey), "idx", "hash", "file", item.decompressor.FileName())
			return nil, false
		}

		k, _ := itemGetter.Next(nil)
		if !bytes.Equal(fullKey, k) {
			dt.d.logger.Warn("commitment branch key replacement seek invalid key",
				"key", hex.EncodeToString(fullKey), "idx", "hash", "file", item.decompressor.FileName())

			return nil, false
		}
		return encodeShorterKey(nil, offset), true
	}
	if dt.d.Accessors.Has(statecfg.AccessorBTree) {
		if item.bindex == nil {
			dt.d.logger.Warn("[agg] commitment branch key replacement: file doesn't have index", "name", item.decompressor.FileName())
		}
		_, _, offsetInFile, ok, err := item.bindex.Get(fullKey, itemGetter)
		if err != nil {
			dt.d.logger.Warn("[agg] commitment branch key replacement seek failed",
				"key", hex.EncodeToString(fullKey), "idx", "bt", "err", err, "file", item.decompressor.FileName())
		}
		if !ok {
			return nil, false
		}
		return encodeShorterKey(nil, offsetInFile), true
	}
	return nil, false
}

// rawLookupFileByRange searches for a file that contains the given range of tx numbers.
// Given range should exactly match the range of some file, so expected to be multiple of stepSize.
// At first it checks range among visible files, then among dirty files.
// If file is not found anywhere, returns nil
func (dt *DomainRoTx) rawLookupFileByRange(txFrom uint64, txTo uint64) (*FilesItem, error) {
	for _, f := range dt.files {
		if f.startTxNum == txFrom && f.endTxNum == txTo && f.src != nil {
			return f.src, nil // found in visible files
		}
	}
	if dirty := dt.lookupDirtyFileByItsRange(txFrom, txTo); dirty != nil {
		return dirty, nil
	}
	return nil, fmt.Errorf("file %s-%s.%d-%d.kv was not found", dt.d.Version.DataKV.String(), dt.d.FilenameBase, txFrom/dt.d.stepSize, txTo/dt.d.stepSize)
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
		fileStepsss := "" + dt.d.Name.String() + ": "
		for _, item := range dt.d.dirtyFiles.Items() {
			fileStepsss += fmt.Sprintf("%d-%d;", item.startTxNum/dt.d.stepSize, item.endTxNum/dt.d.stepSize)
		}
		dt.d.logger.Warn("[agg] lookupDirtyFileByItsRange: file not found",
			"stepFrom", txFrom/dt.d.stepSize, "stepTo", txTo/dt.d.stepSize,
			"files", fileStepsss, "filesCount", dt.d.dirtyFiles.Len())

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
	offset := decodeShorterKey(shortKey)
	defer func() {
		if r := recover(); r != nil {
			dt.d.logger.Crit("lookupByShortenedKey panics",
				"err", r,
				"offset", offset, "short", hex.EncodeToString(shortKey),
				"cleanFilesCount", len(dt.files), "dirtyFilesCount", dt.d.dirtyFiles.Len(),
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

	fullKey, _ = getter.Next(fullKey[:0])
	return fullKey, true
}

// commitmentValTransform parses the value of the commitment record to extract references
// to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// the updated references
func (dt *DomainRoTx) commitmentValTransformDomain(rng MergeRange, accounts, storage *DomainRoTx, mergedAccount, mergedStorage *FilesItem) (valueTransformer, error) {
	var keyBuf [60]byte // 52b key and 8b for inverted step
	var err error

	hadToLookupStorage := mergedStorage == nil
	if mergedStorage == nil {
		if mergedStorage, err = storage.rawLookupFileByRange(rng.from, rng.to); err != nil {
			// TODO may allow to merge, but storage keys will be stored as plainkeys
			return nil, err
		}
	}

	hadToLookupAccount := mergedAccount == nil
	if mergedAccount == nil {
		if mergedAccount, err = accounts.rawLookupFileByRange(rng.from, rng.to); err != nil {
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

	vt := func(valBuf []byte, keyFromTxNum, keyEndTxNum uint64) (transValBuf []byte, err error) {
		if !dt.d.ReplaceKeysInValues || len(valBuf) == 0 || !ValuesPlainKeyReferencingThresholdReached(dt.d.stepSize, keyFromTxNum, keyEndTxNum) {
			return valBuf, nil
		}
		if _, ok := storageFileMap[keyFromTxNum]; !ok {
			storageFileMap[keyFromTxNum] = make(map[uint64]*seg.Reader)
		}
		sig, ok := storageFileMap[keyFromTxNum][keyEndTxNum]
		if !ok {
			dirty := storage.lookupDirtyFileByItsRange(keyFromTxNum, keyEndTxNum)
			if dirty == nil {
				return nil, fmt.Errorf("dirty storage file not found %d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize)
			}
			sig = storage.dataReader(dirty.decompressor)
			storageFileMap[keyFromTxNum][keyEndTxNum] = sig
		}

		if _, ok := accountFileMap[keyFromTxNum]; !ok {
			accountFileMap[keyFromTxNum] = make(map[uint64]*seg.Reader)
		}
		aig, ok := accountFileMap[keyFromTxNum][keyEndTxNum]
		if !ok {
			dirty := accounts.lookupDirtyFileByItsRange(keyFromTxNum, keyEndTxNum)
			if dirty == nil {
				return nil, fmt.Errorf("dirty account file not found %d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize)
			}
			aig = accounts.dataReader(dirty.decompressor)
			accountFileMap[keyFromTxNum][keyEndTxNum] = aig
		}

		replacer := func(key []byte, isStorage bool) ([]byte, error) {
			var found bool
			auxBuf := keyBuf[:0]
			if isStorage {
				if len(key) == length.Addr+length.Hash {
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

				shortened, found := storage.findShortenedKey(auxBuf, ms, mergedStorage)
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
				return shortened, nil
			}

			if len(key) == length.Addr {
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

			shortened, found := accounts.findShortenedKey(auxBuf, ma, mergedAccount)
			if !found {
				if len(auxBuf) == length.Addr {
					return auxBuf, nil // if plain key is lost, we can save original fullkey
				}
				dt.d.logger.Crit("valTransform: replacement for full account key was not found",
					"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.stepSize, keyEndTxNum/dt.d.stepSize),
					"shortened", hex.EncodeToString(shortened), "toReplace", hex.EncodeToString(auxBuf))
				return nil, fmt.Errorf("replacement not found for account  %x", auxBuf)
			}
			return shortened, nil
		}

		return commitment.BranchData(valBuf).ReplacePlainKeys(dt.comBuf[:0], replacer)
	}

	return vt, nil
}
