package decodedstate

import (
	"bytes"
	"encoding/binary"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// QueryEnumerateKeys returns all mapping keys for a contract/slot from the given read tx.
func QueryEnumerateKeys(tx kv.Tx, contract common.Address, slot common.Hash) ([]common.Hash, error) {
	prefix := make([]byte, 53)
	copy(prefix[0:20], contract[:])
	copy(prefix[20:52], slot[:])
	prefix[52] = byte(MappingEntry)

	cursor, err := tx.Cursor(kv.DecodedLatest)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var keys []common.Hash
	for k, v, err := cursor.Seek(prefix); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		rec := decodeLatestRecord(v)
		if len(rec.Keys) > 0 {
			keys = append(keys, rec.Keys[0])
		}
	}
	return keys, nil
}

// QueryEnumerateKeysAsOf returns all mapping keys for a contract/slot at a historical block.
//
// This uses a single-pass approach over the history table to resolve which keys
// existed at the given block, avoiding the O(keys * history) full-scan replay.
func QueryEnumerateKeysAsOf(tx kv.Tx, contract common.Address, slot common.Hash, blockNum uint64) ([]common.Hash, error) {
	prefix := make([]byte, 53)
	copy(prefix[0:20], contract[:])
	copy(prefix[20:52], slot[:])
	prefix[52] = byte(MappingEntry)

	// Track per-key: the earliest history entry after the target (tells us the pre-change state).
	type keyResolution struct {
		keyData     string
		changeTxNum uint64      // earliest change txNum > target
		prevVal     common.Hash // value before that change
		existed     bool        // whether the key existed before that change
		resolved    bool        // true if we found a history entry after target
	}
	resolved := make(map[string]*keyResolution)

	// Pass 1: Scan history entries for this prefix.
	histCursor, err := tx.Cursor(kv.DecodedHistory)
	if err != nil {
		return nil, err
	}
	defer histCursor.Close()
	for k, v, err := histCursor.Seek(prefix); k != nil; k, v, err = histCursor.Next() {
		if err != nil {
			histCursor.Close()
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k) <= 8 {
			continue
		}
		skPart := string(k[:len(k)-8])
		he := decodeHistoryEntry(v)

		r, ok := resolved[skPart]
		if !ok {
			keyData := string(k[53 : len(k)-8])
			r = &keyResolution{keyData: keyData}
			resolved[skPart] = r
		}

		if he.TxNum > blockNum {
			// This is a change after our target block. We want the earliest such change.
			if !r.resolved || he.TxNum < r.changeTxNum {
				r.changeTxNum = he.TxNum
				r.prevVal = he.PrevVal
				r.existed = he.Existed
				r.resolved = true
			}
		}
	}
	histCursor.Close()

	// Pass 2: Scan latest entries for keys with no history after blockNum
	// (these still have their current value at blockNum).
	latestCursor, err := tx.Cursor(kv.DecodedLatest)
	if err != nil {
		return nil, err
	}
	defer latestCursor.Close()

	var result []common.Hash
	for k, v, err := latestCursor.Seek(prefix); k != nil; k, v, err = latestCursor.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		skStr := string(k)
		keyData := string(k[53:])

		if r, ok := resolved[skStr]; ok && r.resolved {
			// We have a history entry after blockNum — use the pre-change state.
			if r.existed {
				var key common.Hash
				copy(key[:], keyData)
				result = append(result, key)
			}
			// Mark as consumed so we don't double-count
			delete(resolved, skStr)
		} else {
			// No change after blockNum — the current value is the value at blockNum.
			// But we need to check if this key even existed at blockNum by checking
			// if there's any history entry at all (first entry tells us creation block).
			rec := decodeLatestRecord(v)
			if len(rec.Keys) > 0 {
				result = append(result, rec.Keys[0])
			}
			delete(resolved, skStr)
		}
	}

	// Any remaining resolved entries are keys that don't exist in latest
	// but had history entries after blockNum — they existed before the change.
	for _, r := range resolved {
		if r.resolved && r.existed {
			var key common.Hash
			copy(key[:], r.keyData)
			result = append(result, key)
		}
	}

	return result, nil
}

// QueryDecodedStorage returns the full decoded state for a contract from the given read tx.
func QueryDecodedStorage(tx kv.Tx, contract common.Address) (map[common.Hash][]DecodedEntry, error) {
	prefix := make([]byte, 20)
	copy(prefix, contract[:])

	cursor, err := tx.Cursor(kv.DecodedLatest)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	result := make(map[common.Hash][]DecodedEntry)
	for k, v, err := cursor.Seek(prefix); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		sk := decodeStoreKey(k)
		rec := decodeLatestRecord(v)
		result[sk.MappingSlot] = append(result[sk.MappingSlot], DecodedEntry{
			Contract: sk.Contract, MappingSlot: sk.MappingSlot,
			Keys: rec.Keys, Value: rec.Value,
			EntryType: rec.EntryType, ArrayIndex: rec.ArrayIdx,
		})
	}
	return result, nil
}

// QueryDecodedStorageAsOf returns the decoded state at a historical block.
//
// This uses a single-pass approach: scan history entries to find the earliest
// change after blockNum for each key, then resolve values directly.
func QueryDecodedStorageAsOf(tx kv.Tx, contract common.Address, blockNum uint64) (map[common.Hash][]DecodedEntry, error) {
	prefix := make([]byte, 20)
	copy(prefix, contract[:])

	// Track per-key resolution from history.
	type keyResolution struct {
		sk          storeKey
		rec         latestRecord // metadata from latest (keys, entryType, arrayIdx)
		changeTxNum uint64       // earliest change txNum > target
		prevVal     common.Hash
		existed     bool
		resolved    bool // true if we found a history entry after target
		hasRec      bool // true if we loaded metadata from latest
	}
	resolved := make(map[string]*keyResolution)

	// Pass 1: Scan history entries for this contract.
	histCursor, err := tx.Cursor(kv.DecodedHistory)
	if err != nil {
		return nil, err
	}
	defer histCursor.Close()
	for k, v, err := histCursor.Seek(prefix); k != nil; k, v, err = histCursor.Next() {
		if err != nil {
			histCursor.Close()
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k) <= 8 {
			continue
		}
		skPart := string(k[:len(k)-8])
		he := decodeHistoryEntry(v)

		r, ok := resolved[skPart]
		if !ok {
			sk := decodeStoreKey(k[:len(k)-8])
			r = &keyResolution{sk: sk}
			resolved[skPart] = r
		}

		if he.TxNum > blockNum {
			if !r.resolved || he.TxNum < r.changeTxNum {
				r.changeTxNum = he.TxNum
				r.prevVal = he.PrevVal
				r.existed = he.Existed
				r.resolved = true
			}
		}
	}
	histCursor.Close()

	// Pass 2: Scan latest entries to get metadata and current values.
	latestCursor, err := tx.Cursor(kv.DecodedLatest)
	if err != nil {
		return nil, err
	}
	defer latestCursor.Close()

	result := make(map[common.Hash][]DecodedEntry)

	for k, v, err := latestCursor.Seek(prefix); k != nil; k, v, err = latestCursor.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		skStr := string(k)
		sk := decodeStoreKey(k)
		rec := decodeLatestRecord(v)

		if r, ok := resolved[skStr]; ok && r.resolved {
			// History says value changed after blockNum — use pre-change state.
			r.rec = rec
			r.hasRec = true
			if r.existed {
				entry := buildEntryFromResolution(sk, rec, r.prevVal)
				result[sk.MappingSlot] = append(result[sk.MappingSlot], entry)
			}
			delete(resolved, skStr)
		} else {
			// No change after blockNum — current value is the value at blockNum.
			entry := DecodedEntry{
				Contract: sk.Contract, MappingSlot: sk.MappingSlot,
				Value: rec.Value, EntryType: rec.EntryType, ArrayIndex: rec.ArrayIdx,
			}
			if len(rec.Keys) > 0 {
				entry.Keys = rec.Keys
			} else if sk.EntryType == MappingEntry && len(sk.KeyData) == 32 {
				var key common.Hash
				copy(key[:], sk.KeyData)
				entry.Keys = []common.Hash{key}
			}
			result[sk.MappingSlot] = append(result[sk.MappingSlot], entry)
			delete(resolved, skStr)
		}
	}

	// Remaining resolved entries: keys not in latest but with history after blockNum.
	for _, r := range resolved {
		if r.resolved && r.existed {
			entry := buildEntryFromResolution(r.sk, r.rec, r.prevVal)
			result[r.sk.MappingSlot] = append(result[r.sk.MappingSlot], entry)
		}
	}

	return result, nil
}

func buildEntryFromResolution(sk storeKey, rec latestRecord, value common.Hash) DecodedEntry {
	entry := DecodedEntry{
		Contract: sk.Contract, MappingSlot: sk.MappingSlot,
		Value: value, EntryType: sk.EntryType, ArrayIndex: rec.ArrayIdx,
	}
	if len(rec.Keys) > 0 {
		entry.Keys = rec.Keys
	} else if sk.EntryType == MappingEntry && len(sk.KeyData) == 32 {
		var key common.Hash
		copy(key[:], sk.KeyData)
		entry.Keys = []common.Hash{key}
	}
	// Recover array index from keyData if needed
	if sk.EntryType == ArrayEntry && len(sk.KeyData) == 8 {
		entry.ArrayIndex = binary.BigEndian.Uint64([]byte(sk.KeyData))
	}
	return entry
}

// QueryMappingValue reads a specific mapping value from the given read tx.
func QueryMappingValue(tx kv.Tx, contract common.Address, slot, key common.Hash) (common.Hash, bool, error) {
	skBytes := encodeStoreKey(makeMappingStoreKey(contract, slot, key))
	val, err := tx.GetOne(kv.DecodedLatest, skBytes)
	if err != nil {
		return common.Hash{}, false, err
	}
	if val == nil {
		return common.Hash{}, false, nil
	}
	rec := decodeLatestRecord(val)
	return rec.Value, true, nil
}

// QueryMappingValueAsOf reads a specific mapping value at a historical block.
func QueryMappingValueAsOf(tx kv.Tx, contract common.Address, slot, key common.Hash, blockNum uint64) (common.Hash, bool, error) {
	return getAsOf(tx, makeMappingStoreKey(contract, slot, key), blockNum)
}
