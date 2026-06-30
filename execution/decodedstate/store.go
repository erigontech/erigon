package decodedstate

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

const metaLatestBlock = "latestBlock"
const metaLatestTxNum = "latestTxNum"

// storeKey uniquely identifies a decoded entry in the store.
type storeKey struct {
	Contract    common.Address
	MappingSlot common.Hash
	EntryType   EntryType
	KeyData     string // mapping: key bytes; nested: concatenated keys; array: 8-byte big-endian index
}

type historyEntry struct {
	TxNum   uint64
	PrevVal common.Hash
	Existed bool
}

type latestRecord struct {
	Value     common.Hash
	EntryType EntryType
	Keys      []common.Hash
	ArrayIdx  uint64
}

// mdbxStore is an MDBX-backed Store.
type mdbxStore struct {
	mu         sync.RWMutex
	db         kv.RwDB
	temporalDB kv.TemporalRwDB // non-nil when db supports temporal domain writes
	ownedDB    bool            // true if we opened the DB and must close it
}

// NewStore creates a new decoded state Store with its own MDBX database at dataDir.
// Used by tests. Production code should use NewStoreFromDB with the existing chaindata DB.
func NewStore(dataDir string) Store {
	tableCfg := kv.TableCfg{
		kv.DecodedLatest:  {},
		kv.DecodedHistory: {},
		kv.DecodedMeta:    {},
	}
	logger := log.New()
	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(dataDir).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return tableCfg }).
		MapSize(2 * datasize.GB).
		GrowthStep(16 * datasize.MB).
		Open(context.Background())
	if err != nil {
		panic("decodedstate: failed to open MDBX: " + err.Error())
	}
	return &mdbxStore{db: db, ownedDB: true}
}

// NewStoreFromDB creates a decoded state Store backed by an existing kv.RwDB (e.g. chaindata).
// The caller retains ownership of db; Close() on the returned Store will not close the underlying DB.
// If db implements kv.TemporalRwDB, decoded writes will also flow through the temporal domain.
func NewStoreFromDB(db kv.RwDB) Store {
	s := &mdbxStore{db: db, ownedDB: false}
	if tdb, ok := db.(kv.TemporalRwDB); ok {
		s.temporalDB = tdb
	}
	return s
}

// --- key encoding ---

func encodeStoreKey(sk storeKey) []byte {
	// contract(20) + mappingSlot(32) + entryType(1) + keyData(variable)
	buf := make([]byte, 20+32+1+len(sk.KeyData))
	copy(buf[0:20], sk.Contract[:])
	copy(buf[20:52], sk.MappingSlot[:])
	buf[52] = byte(sk.EntryType)
	copy(buf[53:], sk.KeyData)
	return buf
}

func decodeStoreKey(b []byte) storeKey {
	var sk storeKey
	copy(sk.Contract[:], b[0:20])
	copy(sk.MappingSlot[:], b[20:52])
	sk.EntryType = EntryType(b[52])
	sk.KeyData = string(b[53:])
	return sk
}

func encodeHistoryKey(sk storeKey, txNum uint64) []byte {
	skBytes := encodeStoreKey(sk)
	buf := make([]byte, len(skBytes)+8)
	copy(buf, skBytes)
	binary.BigEndian.PutUint64(buf[len(skBytes):], txNum)
	return buf
}

// encodeLatestRecord: value(32) + entryType(1) + arrayIdx(8) + numKeys(2) + keys(numKeys*32)
func encodeLatestRecord(rec latestRecord) []byte {
	numKeys := len(rec.Keys)
	buf := make([]byte, 32+1+8+2+numKeys*32)
	copy(buf[0:32], rec.Value[:])
	buf[32] = byte(rec.EntryType)
	binary.BigEndian.PutUint64(buf[33:41], rec.ArrayIdx)
	binary.BigEndian.PutUint16(buf[41:43], uint16(numKeys))
	for i, k := range rec.Keys {
		copy(buf[43+i*32:43+(i+1)*32], k[:])
	}
	return buf
}

func decodeLatestRecord(data []byte) latestRecord {
	if len(data) < 43 {
		return latestRecord{}
	}
	var rec latestRecord
	copy(rec.Value[:], data[0:32])
	rec.EntryType = EntryType(data[32])
	rec.ArrayIdx = binary.BigEndian.Uint64(data[33:41])
	numKeys := int(binary.BigEndian.Uint16(data[41:43]))
	if numKeys > 0 && len(data) >= 43+numKeys*32 {
		rec.Keys = make([]common.Hash, numKeys)
		for i := range numKeys {
			copy(rec.Keys[i][:], data[43+i*32:43+(i+1)*32])
		}
	}
	return rec
}

// encodeHistoryEntry: txNum(8) + prevVal(32) + existed(1)
func encodeHistoryEntry(he historyEntry) []byte {
	buf := make([]byte, 41)
	binary.BigEndian.PutUint64(buf[0:8], he.TxNum)
	copy(buf[8:40], he.PrevVal[:])
	if he.Existed {
		buf[40] = 1
	}
	return buf
}

func decodeHistoryEntry(data []byte) historyEntry {
	if len(data) < 41 {
		return historyEntry{}
	}
	var he historyEntry
	he.TxNum = binary.BigEndian.Uint64(data[0:8])
	copy(he.PrevVal[:], data[8:40])
	he.Existed = data[40] == 1
	return he
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodeUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// LatestBlockTx returns the highest block number recorded in decoded meta.
func LatestBlockTx(tx kv.Tx) (uint64, error) {
	val, err := tx.GetOne(kv.DecodedMeta, []byte(metaLatestBlock))
	if err != nil {
		return 0, err
	}
	return decodeUint64(val), nil
}

// LatestTxNumTx returns the highest tx number recorded in decoded meta.
func LatestTxNumTx(tx kv.Tx) (uint64, error) {
	val, err := tx.GetOne(kv.DecodedMeta, []byte(metaLatestTxNum))
	if err != nil {
		return 0, err
	}
	return decodeUint64(val), nil
}

// AdvanceLatestTxNumTx moves the decoded latest-txNum marker forward.
func AdvanceLatestTxNumTx(tx kv.RwTx, txNum uint64) error {
	current, err := LatestTxNumTx(tx)
	if err != nil {
		return err
	}
	if txNum <= current {
		return nil
	}
	return tx.Put(kv.DecodedMeta, []byte(metaLatestTxNum), encodeUint64(txNum))
}

// AdvanceLatestBlockTx moves the decoded latest-block marker forward.
func AdvanceLatestBlockTx(tx kv.RwTx, blockNum uint64) error {
	current, err := LatestBlockTx(tx)
	if err != nil {
		return err
	}
	if blockNum <= current {
		return nil
	}
	return tx.Put(kv.DecodedMeta, []byte(metaLatestBlock), encodeUint64(blockNum))
}

// --- helpers ---

func makeStoreKey(entry *DecodedEntry) storeKey {
	sk := storeKey{
		Contract:    entry.Contract,
		MappingSlot: entry.MappingSlot,
		EntryType:   entry.EntryType,
	}
	switch entry.EntryType {
	case MappingEntry:
		if len(entry.Keys) > 0 {
			sk.KeyData = string(entry.Keys[0][:])
		}
	case NestedMappingEntry:
		var buf bytes.Buffer
		for _, k := range entry.Keys {
			buf.Write(k[:])
		}
		sk.KeyData = buf.String()
	case ArrayEntry:
		var idx [8]byte
		idx[0] = byte(entry.ArrayIndex >> 56)
		idx[1] = byte(entry.ArrayIndex >> 48)
		idx[2] = byte(entry.ArrayIndex >> 40)
		idx[3] = byte(entry.ArrayIndex >> 32)
		idx[4] = byte(entry.ArrayIndex >> 24)
		idx[5] = byte(entry.ArrayIndex >> 16)
		idx[6] = byte(entry.ArrayIndex >> 8)
		idx[7] = byte(entry.ArrayIndex)
		sk.KeyData = string(idx[:])
	}
	return sk
}

// UnwindDecodedStateToTxNum is the canonical tx-granular decoded unwind helper.
// It restores DecodedLatest to pre-change values using DecodedHistory entries,
// then removes all history entries beyond toTxNum.
func UnwindDecodedStateToTxNum(tx kv.RwTx, toTxNum uint64) error {
	type undoInfo struct {
		sk      storeKey
		entries []historyEntry
	}
	undoMap := make(map[string]*undoInfo)

	histCursor, err := tx.Cursor(kv.DecodedHistory)
	if err != nil {
		return err
	}
	defer histCursor.Close()
	for k, v, err := histCursor.First(); k != nil; k, v, err = histCursor.Next() {
		if err != nil {
			histCursor.Close()
			return err
		}
		he := decodeHistoryEntry(v)
		skPart := k[:len(k)-8]
		skStr := string(skPart)
		info, ok := undoMap[skStr]
		if !ok {
			sk := decodeStoreKey(skPart)
			info = &undoInfo{sk: sk}
			undoMap[skStr] = info
		}
		info.entries = append(info.entries, he)
	}
	histCursor.Close()

	for skStr, info := range undoMap {
		sort.Slice(info.entries, func(i, j int) bool {
			return info.entries[i].TxNum < info.entries[j].TxNum
		})

		skBytes := []byte(skStr)

		// Restore DecodedLatest to the pre-change value
		for _, h := range info.entries {
			if h.TxNum > toTxNum {
				if h.Existed {
					existingData, err := tx.GetOne(kv.DecodedLatest, skBytes)
					if err != nil {
						return err
					}
					rec := latestRecord{}
					if existingData != nil {
						rec = decodeLatestRecord(existingData)
					}
					rec.Value = h.PrevVal
					if err := tx.Put(kv.DecodedLatest, skBytes, encodeLatestRecord(rec)); err != nil {
						return err
					}
				} else {
					if err := tx.Delete(kv.DecodedLatest, skBytes); err != nil {
						return err
					}
				}
				break
			}
		}

		// Delete all history entries beyond toTxNum
		for _, h := range info.entries {
			if h.TxNum > toTxNum {
				histKey := encodeHistoryKey(info.sk, h.TxNum)
				if err := tx.Delete(kv.DecodedHistory, histKey); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// UnwindDecodedStateTx is a block-oriented compatibility wrapper.
// It resolves the block boundary to a canonical tx cutoff and delegates
// to UnwindDecodedStateToTxNum.
func UnwindDecodedStateTx(tx kv.RwTx, toBlock uint64) error {
	// In the flat-table model, block boundaries map directly to tx cutoffs.
	return UnwindDecodedStateToTxNum(tx, toBlock)
}

// PruneDecodedHistoryToTxNum is the canonical tx-granular decoded prune helper.
// It removes decoded history entries with txNum older than the given cutoff.
func PruneDecodedHistoryToTxNum(tx kv.RwTx, olderThanTxNum uint64) error {
	cursor, err := tx.RwCursor(kv.DecodedHistory)
	if err != nil {
		return err
	}
	defer cursor.Close()

	for k, v, err := cursor.First(); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			return err
		}
		he := decodeHistoryEntry(v)
		if he.TxNum < olderThanTxNum {
			if err := cursor.DeleteCurrent(); err != nil {
				return err
			}
		}
	}

	return nil
}

// PruneDecodedHistoryTx is a block-oriented compatibility wrapper.
// It resolves the block-based retention boundary to a canonical tx cutoff
// and delegates to PruneDecodedHistoryToTxNum.
func PruneDecodedHistoryTx(tx kv.RwTx, olderThan uint64) error {
	return PruneDecodedHistoryToTxNum(tx, olderThan)
}

// WriteEntriesToFlatTx writes decoded entries to the DecodedLatest and DecodedHistory
// flat tables only, without touching the temporal domain. Use this when the caller
// manages temporal domain writes separately (e.g., via WriteEntriesToDomains).
func WriteEntriesToFlatTx(tx kv.RwTx, txNum uint64, entries []DecodedEntry) error {
	return writeEntriesToFlatTables(tx, txNum, entries)
}

// WriteEntriesToTx writes decoded entries to the DecodedLatest and DecodedHistory
// tables using the given transaction. When the transaction supports temporal
// writes (kv.TemporalRwTx), it also writes through the DecodedStorageDomain so
// the aggregator can merge/freeze/prune them.
//
// This is used during staged sync to write decoded state within the same
// transaction as the block execution, and by the "live commit path" tests.
func WriteEntriesToTx(tx kv.RwTx, txNum uint64, entries []DecodedEntry) error {
	if err := writeEntriesToFlatTables(tx, txNum, entries); err != nil {
		return err
	}

	// Write through temporal domain when the tx supports it, so domain
	// progress advances and the aggregator can freeze decoded data.
	if ttx, ok := tx.(kv.TemporalRwTx); ok {
		if err := writeTemporalDomainFromTx(ttx, txNum, entries); err != nil {
			return err
		}
	}

	return nil
}

func writeEntriesToFlatTables(tx kv.RwTx, txNum uint64, entries []DecodedEntry) error {
	for i := range entries {
		entry := &entries[i]
		sk := makeStoreKey(entry)
		skBytes := encodeStoreKey(sk)
		isZero := entry.Value == (common.Hash{})

		// Record history: read previous value before overwriting.
		prevData, err := tx.GetOne(kv.DecodedLatest, skBytes)
		if err != nil {
			return err
		}
		hasPrev := prevData != nil

		// Only record a history entry if something is actually changing.
		if hasPrev || !isZero {
			he := historyEntry{
				TxNum:   txNum,
				Existed: hasPrev,
			}
			if hasPrev {
				he.PrevVal = decodeLatestRecord(prevData).Value
			}
			histKey := encodeHistoryKey(sk, txNum)
			// Don't overwrite if there's already a history entry for this key+txNum
			// (first change at this txNum wins — records the pre-change value).
			existing, err := tx.GetOne(kv.DecodedHistory, histKey)
			if err != nil {
				return err
			}
			if existing == nil {
				if err := tx.Put(kv.DecodedHistory, histKey, encodeHistoryEntry(he)); err != nil {
					return err
				}
			}
		}

		// Write latest value.
		if isZero {
			if err := tx.Delete(kv.DecodedLatest, skBytes); err != nil {
				return err
			}
		} else {
			rec := latestRecord{
				Value: entry.Value, EntryType: entry.EntryType,
				Keys: entry.Keys, ArrayIdx: entry.ArrayIndex,
			}
			if err := tx.Put(kv.DecodedLatest, skBytes, encodeLatestRecord(rec)); err != nil {
				return err
			}
		}
	}
	return AdvanceLatestTxNumTx(tx, txNum)
}

func makeMappingStoreKey(contract common.Address, slot, key common.Hash) storeKey {
	return storeKey{
		Contract:    contract,
		MappingSlot: slot,
		EntryType:   MappingEntry,
		KeyData:     string(key[:]),
	}
}

// getHistoryEntries reads all history entries for a given storeKey from tx, sorted by block ascending.
func getHistoryEntries(tx kv.Tx, sk storeKey) ([]historyEntry, error) {
	prefix := encodeStoreKey(sk)
	cursor, err := tx.Cursor(kv.DecodedHistory)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var entries []historyEntry
	for k, v, err := cursor.Seek(prefix); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		entries = append(entries, decodeHistoryEntry(v))
	}
	return entries, nil
}

// getAsOf resolves the historical value at the given coordinate.
// History entry (T, prev, existed) means "at txNum T the value changed FROM prev".
// The coordinate parameter is compared against TxNum in history entries.
func getAsOf(tx kv.Tx, sk storeKey, asOfCoord uint64) (common.Hash, bool, error) {
	hist, err := getHistoryEntries(tx, sk)
	if err != nil {
		return common.Hash{}, false, err
	}

	for _, h := range hist {
		if h.TxNum > asOfCoord {
			if !h.Existed {
				return common.Hash{}, false, nil
			}
			return h.PrevVal, true, nil
		}
	}
	// No change after asOfCoord
	if len(hist) == 0 {
		skBytes := encodeStoreKey(sk)
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
	if asOfCoord < hist[0].TxNum {
		return common.Hash{}, false, nil
	}
	skBytes := encodeStoreKey(sk)
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

// --- Store methods ---

func (s *mdbxStore) WriteEntries(blockNum uint64, entries []DecodedEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// In the Store interface, blockNum serves as the canonical coordinate (txNum).
	txNum := blockNum

	tx, err := s.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	recorded := make(map[string]bool)

	for i := range entries {
		entry := &entries[i]
		sk := makeStoreKey(entry)
		skBytes := encodeStoreKey(sk)
		skStr := string(skBytes)
		isZero := entry.Value == (common.Hash{})

		existingData, err := tx.GetOne(kv.DecodedLatest, skBytes)
		if err != nil {
			return err
		}
		existed := existingData != nil

		if !recorded[skStr] {
			histKey := encodeHistoryKey(sk, txNum)
			existingHist, err := tx.GetOne(kv.DecodedHistory, histKey)
			if err != nil {
				return err
			}
			alreadyRecorded := existingHist != nil

			if !alreadyRecorded && (existed || !isZero) {
				he := historyEntry{TxNum: txNum, Existed: existed}
				if existed {
					he.PrevVal = decodeLatestRecord(existingData).Value
				}
				if err := tx.Put(kv.DecodedHistory, histKey, encodeHistoryEntry(he)); err != nil {
					return err
				}
			}
			recorded[skStr] = true
		}

		if isZero {
			if existed {
				if err := tx.Delete(kv.DecodedLatest, skBytes); err != nil {
					return err
				}
			}
		} else {
			rec := latestRecord{
				Value: entry.Value, EntryType: entry.EntryType,
				Keys: entry.Keys, ArrayIdx: entry.ArrayIndex,
			}
			if err := tx.Put(kv.DecodedLatest, skBytes, encodeLatestRecord(rec)); err != nil {
				return err
			}
		}
	}

	// Advance both block and txNum markers
	currentBlock := decodeUint64(func() []byte {
		v, _ := tx.GetOne(kv.DecodedMeta, []byte(metaLatestBlock))
		return v
	}())
	if blockNum > currentBlock {
		if err := tx.Put(kv.DecodedMeta, []byte(metaLatestBlock), encodeUint64(blockNum)); err != nil {
			return err
		}
	}
	if err := AdvanceLatestTxNumTx(tx, txNum); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Also write through the temporal domain when available
	if s.temporalDB != nil {
		return s.writeTemporalDomain(txNum, entries)
	}
	return nil
}

// writeTemporalDomain writes decoded entries through the temporal domain pipeline
// so the aggregator can merge/freeze/prune them and DomainProgress advances.
func (s *mdbxStore) writeTemporalDomain(blockNum uint64, entries []DecodedEntry) error {
	return writeTemporalDomainNewSD(s.temporalDB, blockNum, entries)
}

func (s *mdbxStore) GetLatest(contract common.Address, mappingSlot, key common.Hash) (common.Hash, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return common.Hash{}, false, err
	}
	defer tx.Rollback()

	skBytes := encodeStoreKey(makeMappingStoreKey(contract, mappingSlot, key))
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

func (s *mdbxStore) GetAsOf(contract common.Address, mappingSlot, key common.Hash, blockNum uint64) (common.Hash, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return common.Hash{}, false, err
	}
	defer tx.Rollback()

	return getAsOf(tx, makeMappingStoreKey(contract, mappingSlot, key), blockNum)
}

func (s *mdbxStore) EnumerateKeys(contract common.Address, mappingSlot common.Hash) ([]common.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	prefix := make([]byte, 53)
	copy(prefix[0:20], contract[:])
	copy(prefix[20:52], mappingSlot[:])
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

func (s *mdbxStore) EnumerateKeysAsOf(contract common.Address, mappingSlot common.Hash, blockNum uint64) ([]common.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	prefix := make([]byte, 53)
	copy(prefix[0:20], contract[:])
	copy(prefix[20:52], mappingSlot[:])
	prefix[52] = byte(MappingEntry)

	allKeys := make(map[string]storeKey)

	cursor, err := tx.Cursor(kv.DecodedLatest)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	for k, _, err := cursor.Seek(prefix); k != nil; k, _, err = cursor.Next() {
		if err != nil {
			cursor.Close()
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		allKeys[string(k)] = decodeStoreKey(k)
	}
	cursor.Close()

	histCursor, err := tx.Cursor(kv.DecodedHistory)
	if err != nil {
		return nil, err
	}
	defer histCursor.Close()
	for k, _, err := histCursor.Seek(prefix); k != nil; k, _, err = histCursor.Next() {
		if err != nil {
			histCursor.Close()
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k) > 8 {
			skPart := k[:len(k)-8]
			if _, ok := allKeys[string(skPart)]; !ok {
				allKeys[string(skPart)] = decodeStoreKey(skPart)
			}
		}
	}
	histCursor.Close()

	var result []common.Hash
	for _, sk := range allKeys {
		if _, ok, _ := getAsOf(tx, sk, blockNum); ok {
			var key common.Hash
			copy(key[:], sk.KeyData)
			result = append(result, key)
		}
	}
	return result, nil
}

func (s *mdbxStore) GetDecodedStorage(contract common.Address) (map[common.Hash][]DecodedEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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

func (s *mdbxStore) GetDecodedStorageAsOf(contract common.Address, blockNum uint64) (map[common.Hash][]DecodedEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	prefix := make([]byte, 20)
	copy(prefix, contract[:])

	type skInfo struct {
		sk  storeKey
		rec latestRecord
	}
	all := make(map[string]skInfo)

	cursor, err := tx.Cursor(kv.DecodedLatest)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	for k, v, err := cursor.Seek(prefix); k != nil; k, v, err = cursor.Next() {
		if err != nil {
			cursor.Close()
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		sk := decodeStoreKey(k)
		rec := decodeLatestRecord(v)
		all[string(k)] = skInfo{sk: sk, rec: rec}
	}
	cursor.Close()

	histCursor, err := tx.Cursor(kv.DecodedHistory)
	if err != nil {
		return nil, err
	}
	defer histCursor.Close()
	for k, _, err := histCursor.Seek(prefix); k != nil; k, _, err = histCursor.Next() {
		if err != nil {
			histCursor.Close()
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if len(k) > 8 {
			skPart := k[:len(k)-8]
			if _, ok := all[string(skPart)]; !ok {
				all[string(skPart)] = skInfo{sk: decodeStoreKey(skPart)}
			}
		}
	}
	histCursor.Close()

	result := make(map[common.Hash][]DecodedEntry)
	for _, info := range all {
		val, ok, err := getAsOf(tx, info.sk, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		entry := DecodedEntry{
			Contract: info.sk.Contract, MappingSlot: info.sk.MappingSlot,
			Value: val, EntryType: info.sk.EntryType, ArrayIndex: info.rec.ArrayIdx,
		}
		if len(info.rec.Keys) > 0 {
			entry.Keys = info.rec.Keys
		} else if info.sk.EntryType == MappingEntry && len(info.sk.KeyData) == 32 {
			var key common.Hash
			copy(key[:], info.sk.KeyData)
			entry.Keys = []common.Hash{key}
		}
		result[info.sk.MappingSlot] = append(result[info.sk.MappingSlot], entry)
	}
	return result, nil
}

func (s *mdbxStore) DeleteContract(contract common.Address) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	prefix := make([]byte, 20)
	copy(prefix, contract[:])

	cursor, err := tx.RwCursor(kv.DecodedLatest)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for k, _, err := cursor.Seek(prefix); k != nil; k, _, err = cursor.Next() {
		if err != nil {
			cursor.Close()
			return err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := cursor.DeleteCurrent(); err != nil {
			cursor.Close()
			return err
		}
	}
	cursor.Close()

	histCursor, err := tx.RwCursor(kv.DecodedHistory)
	if err != nil {
		return err
	}
	defer histCursor.Close()
	for k, _, err := histCursor.Seek(prefix); k != nil; k, _, err = histCursor.Next() {
		if err != nil {
			histCursor.Close()
			return err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if err := histCursor.DeleteCurrent(); err != nil {
			histCursor.Close()
			return err
		}
	}
	histCursor.Close()

	return tx.Commit()
}

func (s *mdbxStore) Unwind(toBlock uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// In the Store interface, block boundaries map directly to tx coordinates.
	toTxNum := toBlock

	tx, err := s.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := UnwindDecodedStateToTxNum(tx, toTxNum); err != nil {
		return err
	}

	if err := tx.Put(kv.DecodedMeta, []byte(metaLatestBlock), encodeUint64(toBlock)); err != nil {
		return err
	}
	if err := tx.Put(kv.DecodedMeta, []byte(metaLatestTxNum), encodeUint64(toTxNum)); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *mdbxStore) Prune(olderThan uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// In the Store interface, the threshold maps directly to tx coordinates.
	olderThanTxNum := olderThan

	tx, err := s.db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := PruneDecodedHistoryToTxNum(tx, olderThanTxNum); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *mdbxStore) LatestBlock() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tx, err := s.db.BeginRo(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	val, err := tx.GetOne(kv.DecodedMeta, []byte(metaLatestBlock))
	if err != nil {
		return 0, err
	}
	return decodeUint64(val), nil
}

func (s *mdbxStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ownedDB {
		s.db.Close()
	}
	return nil
}
