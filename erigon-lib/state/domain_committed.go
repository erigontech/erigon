/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/recsplit"
)

type ValueMerger func(prev, current []byte) (merged []byte, err error)

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
func (dt *DomainRoTx) findShortenedKey(fullKey []byte, itemGetter ArchiveGetter, item *filesItem) (shortened []byte, found bool) {
	if item == nil {
		return nil, false
	}
	if !strings.Contains(item.decompressor.FileName(), dt.d.filenameBase) {
		panic(fmt.Sprintf("findShortenedKeyEasier of %s called with merged file %s", dt.d.filenameBase, item.decompressor.FileName()))
	}
	if /*assert.Enable && */ itemGetter.FileName() != item.decompressor.FileName() {
		panic(fmt.Sprintf("findShortenedKey of %s itemGetter (%s) is different to item.decompressor (%s)",
			dt.d.filenameBase, itemGetter.FileName(), item.decompressor.FileName()))
	}

	//if idxList&withExistence != 0 {
	//	hi, _ := dt.ht.iit.hashKey(fullKey)
	//	if !item.existence.ContainsHash(hi) {
	//		continue
	//	}
	//}

	if dt.d.indexList&withHashMap != 0 {
		reader := recsplit.NewIndexReader(item.index)
		defer reader.Close()

		offset, ok := reader.Lookup(fullKey)
		if !ok {
			return nil, false
		}

		itemGetter.Reset(offset)
		if !itemGetter.HasNext() {
			dt.d.logger.Warn("commitment branch key replacement seek failed",
				"key", fmt.Sprintf("%x", fullKey), "idx", "hash", "file", item.decompressor.FileName())
			return nil, false
		}

		k, _ := itemGetter.Next(nil)
		if !bytes.Equal(fullKey, k) {
			dt.d.logger.Warn("commitment branch key replacement seek invalid key",
				"key", fmt.Sprintf("%x", fullKey), "idx", "hash", "file", item.decompressor.FileName())

			return nil, false
		}
		return encodeShorterKey(nil, offset), true
	}
	if dt.d.indexList&withBTree != 0 {
		cur, err := item.bindex.Seek(itemGetter, fullKey)
		if err != nil {
			dt.d.logger.Warn("commitment branch key replacement seek failed",
				"key", fmt.Sprintf("%x", fullKey), "idx", "bt", "err", err, "file", item.decompressor.FileName())
		}

		if cur == nil || !bytes.Equal(cur.Key(), fullKey) {
			return nil, false
		}

		offset := cur.offsetInFile()
		if uint64(itemGetter.Size()) <= offset {
			dt.d.logger.Warn("commitment branch key replacement seek gone too far",
				"key", fmt.Sprintf("%x", fullKey), "offset", offset, "size", itemGetter.Size(), "file", item.decompressor.FileName())
			return nil, false
		}
		return encodeShorterKey(nil, offset), true
	}
	return nil, false
}

func (dt *DomainRoTx) lookupFileByItsRange(txFrom uint64, txTo uint64) *filesItem {
	var item *filesItem
	for _, f := range dt.files {
		if f.startTxNum == txFrom && f.endTxNum == txTo {
			item = f.src
			break
		}
	}
	if item == nil {
		dt.d.dirtyFiles.Walk(func(files []*filesItem) bool {
			for _, f := range files {
				if f.startTxNum == txFrom && f.endTxNum == txTo {
					item = f
					return false
				}
			}
			return true
		})
	}

	if item == nil {
		fileStepsss := ""
		for _, item := range dt.d.dirtyFiles.Items() {
			fileStepsss += fmt.Sprintf("%d-%d;", item.startTxNum/dt.d.aggregationStep, item.endTxNum/dt.d.aggregationStep)
		}
		visibleFiles := ""
		for _, f := range dt.files {
			visibleFiles += fmt.Sprintf("%d-%d;", f.startTxNum/dt.d.aggregationStep, f.endTxNum/dt.d.aggregationStep)
		}
		dt.d.logger.Warn("lookupFileByItsRange: file not found",
			"stepFrom", txFrom/dt.d.aggregationStep, "stepTo", txTo/dt.d.aggregationStep,
			"domain", dt.d.keysTable, "files", fileStepsss, "_visibleFiles", visibleFiles,
			"visibleFilesCount", len(dt.files), "filesCount", dt.d.dirtyFiles.Len())
		return nil
	}
	return item
}

// searches in given list of files for a key or searches in domain files if list is empty
func (dt *DomainRoTx) lookupByShortenedKey(shortKey []byte, getter ArchiveGetter) (fullKey []byte, found bool) {
	if len(shortKey) < 1 {
		return nil, false
	}
	offset := decodeShorterKey(shortKey)
	defer func() {
		if r := recover(); r != nil {
			dt.d.logger.Crit("lookupByShortenedKey panics",
				"err", r,
				"domain", dt.d.keysTable,
				"offset", offset, "short", fmt.Sprintf("%x", shortKey),
				"cleanFilesCount", len(dt.files), "dirtyFilesCount", dt.d.dirtyFiles.Len(),
				"file", getter.FileName())
		}
	}()

	//getter := NewArchiveGetter(item.decompressor.MakeGetter(), dt.d.compression)
	getter.Reset(offset)
	n := getter.HasNext()
	if !n || uint64(getter.Size()) <= offset {
		dt.d.logger.Warn("lookupByShortenedKey failed", "file", getter.FileName(), "short", fmt.Sprintf("%x", shortKey), "offset", offset, "hasNext", n, "size", getter.Size(), "offsetBigger", uint64(getter.Size()) <= offset)
		return nil, false
	}

	fullKey, _ = getter.Next(nil)
	return fullKey, true
}

// commitmentValTransform parses the value of the commitment record to extract references
// to accounts and storage items, then looks them up in the new, merged files, and replaces them with
// the updated references
func (dt *DomainRoTx) commitmentValTransformDomain(accounts, storage *DomainRoTx, mergedAccount, mergedStorage *filesItem) valueTransformer {

	var accMerged, stoMerged string
	if mergedAccount != nil {
		accMerged = fmt.Sprintf("%d-%d", mergedAccount.startTxNum/dt.d.aggregationStep, mergedAccount.endTxNum/dt.d.aggregationStep)
	}
	if mergedStorage != nil {
		stoMerged = fmt.Sprintf("%d-%d", mergedStorage.startTxNum/dt.d.aggregationStep, mergedStorage.endTxNum/dt.d.aggregationStep)
	}

	return func(valBuf []byte, keyFromTxNum, keyEndTxNum uint64) (transValBuf []byte, err error) {
		if !dt.d.replaceKeysInValues || len(valBuf) == 0 || ((keyEndTxNum-keyFromTxNum)/dt.d.aggregationStep)%2 != 0 {
			return valBuf, nil
		}
		si := storage.lookupFileByItsRange(keyFromTxNum, keyEndTxNum)
		if si == nil {
			return nil, fmt.Errorf("storage file not found for %d-%d", keyFromTxNum, keyEndTxNum)
		}
		ai := accounts.lookupFileByItsRange(keyFromTxNum, keyEndTxNum)
		if ai == nil {
			return nil, fmt.Errorf("account file not found for %d-%d", keyFromTxNum, keyEndTxNum)
		}

		if si.decompressor == nil || ai.decompressor == nil {
			return nil, fmt.Errorf("decompressor is nil for existing storage or account")
		}
		if mergedStorage == nil || mergedAccount == nil {
			return nil, fmt.Errorf("mergedStorage or mergedAccount is nil")
		}

		sig := NewArchiveGetter(si.decompressor.MakeGetter(), storage.d.compression)
		aig := NewArchiveGetter(ai.decompressor.MakeGetter(), accounts.d.compression)
		ms := NewArchiveGetter(mergedStorage.decompressor.MakeGetter(), storage.d.compression)
		ma := NewArchiveGetter(mergedAccount.decompressor.MakeGetter(), accounts.d.compression)

		replacer := func(key []byte, isStorage bool) ([]byte, error) {
			var found bool
			auxBuf := dt.keyBuf[:0]
			if isStorage {
				if len(key) == length.Addr+length.Hash {
					// Non-optimised key originating from a database record
					auxBuf = append(auxBuf[:0], key...)
				} else {
					// Optimised key referencing a state file record (file number and offset within the file)
					auxBuf, found = storage.lookupByShortenedKey(key, sig)
					if !found {
						dt.d.logger.Crit("valTransform: lost storage full key",
							"shortened", fmt.Sprintf("%x", key),
							"merging", stoMerged,
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
						"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.aggregationStep, keyEndTxNum/dt.d.aggregationStep),
						"shortened", fmt.Sprintf("%x", shortened), "toReplace", fmt.Sprintf("%x", auxBuf))

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
						"shortened", fmt.Sprintf("%x", key),
						"merging", accMerged,
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
					"step", fmt.Sprintf("%d-%d", keyFromTxNum/dt.d.aggregationStep, keyEndTxNum/dt.d.aggregationStep),
					"shortened", fmt.Sprintf("%x", shortened), "toReplace", fmt.Sprintf("%x", auxBuf))
				return nil, fmt.Errorf("replacement not found for account  %x", auxBuf)
			}
			return shortened, nil
		}

		return commitment.BranchData(valBuf).ReplacePlainKeys(dt.comBuf[:0], replacer)
	}
}
