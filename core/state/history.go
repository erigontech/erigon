package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

//MaxChangesetsSearch -
const MaxChangesetsSearch = 256

func GetAsOf(tx ethdb.Tx, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	v, err := FindByHistory(tx, storage, key, timestamp)
	if err == nil {
		dat = make([]byte, len(v))
		copy(dat, v)
		return dat, nil
	}
	if !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	v, err = tx.GetOne(dbutils.PlainStateBucket, key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ethdb.ErrKeyNotFound
	}
	dat = make([]byte, len(v))
	copy(dat, v)
	return dat, nil
}

func FindByHistory(tx ethdb.Tx, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var hBucket string
	if storage {
		hBucket = dbutils.StorageHistoryBucket
	} else {
		hBucket = dbutils.AccountsHistoryBucket
	}
	c := tx.Cursor(hBucket)
	defer c.Close()
	k, v, seekErr := c.Seek(dbutils.IndexChunkKey(key, timestamp))
	if seekErr != nil {
		return nil, seekErr
	}
	if k == nil {
		return nil, ethdb.ErrKeyNotFound
	}
	if storage {
		if !bytes.Equal(k[:common.AddressLength], key[:common.AddressLength]) ||
			!bytes.Equal(k[common.AddressLength:common.AddressLength+common.HashLength], key[common.AddressLength+common.IncarnationLength:]) {
			return nil, ethdb.ErrKeyNotFound
		}
	} else {
		if !bytes.HasPrefix(k, key) {
			return nil, ethdb.ErrKeyNotFound
		}
	}
	index := dbutils.WrapHistoryIndex(v)

	changeSetBlock, set, ok := index.Search(timestamp)
	var data []byte
	if ok {
		// set == true if this change was from empty record (non-existent account) to non-empty
		// In such case, we do not need to examine changeSet and return empty data
		if set && !storage {
			return []byte{}, nil
		}
		csBucket, _ := dbutils.ChangeSetByIndexBucket(storage)
		c := tx.CursorDupSort(csBucket)
		defer c.Close()
		var err error
		if storage {
			data, err = changeset.Mapper[csBucket].WalkerAdapter(c).(changeset.StorageChangeSetPlain).FindWithIncarnation(changeSetBlock, key)
		} else {
			data, err = changeset.Mapper[csBucket].WalkerAdapter(c).Find(changeSetBlock, key)
		}
		if err != nil {
			if !errors.Is(err, changeset.ErrNotFound) {
				return nil, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
			}
			return nil, ethdb.ErrKeyNotFound
		}
	} else {
		//fmt.Printf("Not Found changeSetBlock in [%s]\n", index)
		return nil, ethdb.ErrKeyNotFound
	}

	//restore codehash
	if !storage {
		var acc accounts.Account
		if err := acc.DecodeForStorage(data); err != nil {
			return nil, err
		}
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			var codeHash []byte
			var err error
			codeHash, err = tx.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(key, acc.Incarnation))
			if err != nil {
				return nil, err
			}
			if len(codeHash) > 0 {
				acc.CodeHash = common.BytesToHash(codeHash)
			}
			data = make([]byte, acc.EncodingLengthForStorage())
			acc.EncodeForStorage(data)
		}
		return data, nil
	}

	return data, nil
}

func WalkAsOf(db ethdb.Tx, bucket string, hBucket string, startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	//fmt.Printf("WalkAsOf %x %x %x %d %d\n", bucket, hBucket, startkey, fixedbits, timestamp)
	if !(bucket == dbutils.PlainStateBucket || bucket == dbutils.CurrentStateBucket) {
		return fmt.Errorf("unsupported state bucket: %s", string(bucket))
	}
	if hBucket == dbutils.AccountsHistoryBucket {
		return walkAsOfThinAccounts(db, bucket, hBucket, startkey, fixedbits, timestamp, walker)
	} else if hBucket == dbutils.StorageHistoryBucket {
		return walkAsOfThinStorage(db, bucket, hBucket, startkey, fixedbits, timestamp, func(k1, k2, v []byte) (bool, error) {
			return walker(append(common.CopyBytes(k1), k2...), v)
		})
	}

	panic(fmt.Sprintf("Not implemented for arbitrary buckets: %s, %s", string(bucket), string(hBucket)))
}

func walkAsOfThinStorage(tx ethdb.Tx, bucket string, hBucket string, startkey []byte, fixedbits int, timestamp uint64, walker func(k1, k2, v []byte) (bool, error)) error {
	csBucket := dbutils.StorageChangeSetBucket
	if bucket == dbutils.PlainStateBucket {
		csBucket = dbutils.PlainStorageChangeSetBucket
	}

	generatedTo, executedTo, innerErr := getIndexGenerationProgress(tx, stages.StorageHistoryIndex)
	if innerErr != nil {
		return innerErr
	}
	if executedTo > generatedTo+MaxChangesetsSearch {
		return fmt.Errorf("too high difference between last generated index block(%v) and last executed block(%v)", generatedTo, executedTo)
	}

	startkeyNoInc := dbutils.CompositeKeyWithoutIncarnation(startkey)
	part1End := common.HashLength
	part2Start := common.HashLength + common.IncarnationLength
	part3Start := common.HashLength + common.IncarnationLength + common.HashLength
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength + common.IncarnationLength
		part3Start = common.AddressLength + common.IncarnationLength + common.HashLength
	}

	//for storage
	mainCursor := ethdb.NewSplitCursor(
		tx.Cursor(bucket),
		startkey,
		fixedbits,
		part1End,
		part2Start,
		part3Start,
	)
	fixetBitsForHistory := fixedbits - 8*common.IncarnationLength
	if fixetBitsForHistory < 0 {
		fixetBitsForHistory = 0
	}

	part1End = common.HashLength
	part2Start = common.HashLength
	part3Start = common.HashLength * 2
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength
		part3Start = common.AddressLength + common.HashLength
	}

	//for historic data
	var historyCursor historyCursor = ethdb.NewSplitCursor(
		tx.Cursor(dbutils.StorageHistoryBucket),
		startkeyNoInc,
		fixetBitsForHistory,
		part1End,   /* part1end */
		part2Start, /* part2start */
		part3Start, /* part3start */
	)

	part1End = common.HashLength
	part2Start = common.HashLength + common.IncarnationLength
	part3Start = common.HashLength + common.IncarnationLength + common.HashLength
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength + common.IncarnationLength
		part3Start = common.AddressLength + common.IncarnationLength + common.HashLength
	}

	cs, cursor := returnCorrectWalker2(bucket, hBucket, tx)
	defer cursor.Close()

	decorator := NewChangesetSearchDecorator(historyCursor, tx, csBucket, startkey, fixetBitsForHistory, part1End, part2Start, part3Start, timestamp, cs)
	err := decorator.buildChangeset(generatedTo, executedTo)
	if err != nil {
		return err
	}
	historyCursor = decorator

	addrHash, keyHash, _, v, err1 := mainCursor.Seek()
	if err1 != nil {
		return err1
	}

	hAddrHash, hKeyHash, _, hV, err2 := historyCursor.Seek()
	if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
		return err2
	}
	goOn := true
	for goOn {
		cmp, br := common.KeyCmp(addrHash, hAddrHash)
		if br {
			break
		}
		if cmp == 0 {
			cmp, br = common.KeyCmp(keyHash, hKeyHash)
		}
		if br {
			break
		}

		//next key in state
		if cmp < 0 {
			goOn, err = walker(addrHash, keyHash, v)
		} else {
			if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
				return err2
			}
			if len(hV) > 0 && err2 == nil { // Skip accounts did not exist
				goOn, err = walker(hAddrHash, hKeyHash, hV)
			} else if errors.Is(err2, ErrNotInHistory) && cmp == 0 {
				goOn, err = walker(addrHash, keyHash, v)
			}
		}
		if err != nil {
			return err
		}
		if goOn {
			if cmp <= 0 {
				addrHash, keyHash, _, v, err1 = mainCursor.Next()
				if err1 != nil {
					return err1
				}
			}
			if cmp >= 0 {
				hAddrHash, hKeyHash, _, hV, err2 = historyCursor.Next()
				if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
					return err2
				}
			}
		}
	}
	return err
}

func walkAsOfThinAccounts(tx ethdb.Tx, bucket string, hBucket string, startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	fixedbytes, mask := ethdb.Bytesmask(fixedbits)
	csBucket := dbutils.AccountChangeSetBucket
	if bucket == dbutils.PlainStateBucket {
		csBucket = dbutils.PlainAccountChangeSetBucket
	}

	generatedTo, executedTo, innerErr := getIndexGenerationProgress(tx, stages.AccountHistoryIndex)
	if innerErr != nil {
		return innerErr
	}
	if executedTo > generatedTo+MaxChangesetsSearch {
		return fmt.Errorf("too high difference between last generated index block(%v) and last executed block(%v)", generatedTo, executedTo)
	}

	mainCursor := tx.Cursor(bucket)
	part1End := common.HashLength
	part2Start := common.HashLength
	part3Start := common.HashLength
	maxKeyLen := common.HashLength
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength
		part3Start = common.AddressLength
		maxKeyLen = common.AddressLength
	}

	var hCursor historyCursor = ethdb.NewSplitCursor(
		tx.Cursor(dbutils.AccountsHistoryBucket),
		startkey,
		fixedbits,
		part1End,   /* part1end */
		part2Start, /* part2start */
		part3Start, /* part3start */
	)
	cs, cursor := returnCorrectWalker2(bucket, hBucket, tx)
	defer cursor.Close()

	decorator := NewChangesetSearchDecorator(hCursor, tx, csBucket, startkey, fixedbits, part1End, part2Start, part3Start, timestamp, cs)
	innerErr = decorator.buildChangeset(generatedTo, executedTo)
	if innerErr != nil {
		return innerErr
	}
	hCursor = decorator

	k, v, err1 := mainCursor.Seek(startkey)
	if err1 != nil {
		return err1
	}
	for k != nil && len(k) > maxKeyLen {
		k, v, err1 = mainCursor.Next()
		if err1 != nil {
			return err1
		}
	}
	hK, _, _, hV, err2 := hCursor.Seek()
	if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
		return err2
	}

	goOn := true
	var err error
	for goOn {
		//exit or next conditions
		if k != nil && fixedbits > 0 && !bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) {
			k = nil
		}
		if k != nil && fixedbits > 0 && (k[fixedbytes-1]&mask) != (startkey[fixedbytes-1]&mask) {
			k = nil
		}
		var cmp int
		cmp, br := common.KeyCmp(k, hK)
		if br {
			break
		}
		if cmp < 0 {
			goOn, err = walker(k, v)
		} else {
			if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
				return err2
			}
			if len(hV) > 0 && err2 == nil { // Skip accounts did not exist
				goOn, err = walker(hK, hV)
			} else if errors.Is(err2, ErrNotInHistory) && cmp == 0 {
				goOn, err = walker(k, v)
			}
		}
		if goOn {
			if cmp <= 0 {
				k, v, err1 = mainCursor.Next()
				if err1 != nil {
					return err1
				}
				for k != nil && len(k) > maxKeyLen {
					k, v, err1 = mainCursor.Next()
					if err1 != nil {
						return err1
					}
				}
			}
			if cmp >= 0 {
				hK, _, _, hV, err2 = hCursor.Next()
				if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
					return err2
				}
			}
		}
	}
	return err

}

func findInHistory(hK, hV []byte, timestamp uint64, csWalker changeset.Walker2) ([]byte, bool, error) {
	index := dbutils.WrapHistoryIndex(hV)
	if changeSetBlock, set, ok := index.Search(timestamp); ok {
		// set == true if this change was from empty record (non-existent account) to non-empty
		// In such case, we do not need to examine changeSet and simply skip the record
		if !set {
			// Extract value from the changeSet
			data, err := csWalker.Find(changeSetBlock, hK)
			if err != nil {
				return nil, false, fmt.Errorf("could not find key %x in the ChangeSet record for index entry %d (query timestamp %d): %v",
					hK,
					changeSetBlock,
					timestamp,
					err)
			}
			return data, true, nil
		}
		return nil, true, nil
	}
	return nil, false, nil
}

// Important! It leaks cursor, please close it outside
func returnCorrectWalker2(bucket, hBucket string, tx ethdb.Tx) (changeset.Walker2, ethdb.CursorDupSort) {
	switch {
	case bucket == dbutils.CurrentStateBucket && hBucket == dbutils.StorageHistoryBucket:
		c := tx.CursorDupSort(dbutils.StorageChangeSetBucket)
		return changeset.Mapper[dbutils.StorageChangeSetBucket].WalkerAdapter(c), c
	case bucket == dbutils.CurrentStateBucket && hBucket == dbutils.AccountsHistoryBucket:
		c := tx.CursorDupSort(dbutils.AccountChangeSetBucket)
		return changeset.Mapper[dbutils.AccountChangeSetBucket].WalkerAdapter(c), c
	case bucket == dbutils.PlainStateBucket && hBucket == dbutils.StorageHistoryBucket:
		c := tx.CursorDupSort(dbutils.PlainStorageChangeSetBucket)
		return changeset.Mapper[dbutils.PlainStorageChangeSetBucket].WalkerAdapter(c), c
	case bucket == dbutils.PlainStateBucket && hBucket == dbutils.AccountsHistoryBucket:
		c := tx.CursorDupSort(dbutils.PlainAccountChangeSetBucket)
		return changeset.Mapper[dbutils.PlainAccountChangeSetBucket].WalkerAdapter(c), c
	default:
		panic("not implemented")
	}
}

func getIndexGenerationProgress(tx ethdb.Tx, stage stages.SyncStage) (generatedTo uint64, executedTo uint64, err error) {
	v, err := tx.GetOne(dbutils.SyncStageProgress, stage)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, 0, err
	}
	if len(v) >= 8 {
		generatedTo = binary.BigEndian.Uint64(v[:8])
	}
	v, err = tx.GetOne(dbutils.SyncStageProgress, stages.Execution)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, 0, err
	}
	if len(v) >= 8 {
		executedTo = binary.BigEndian.Uint64(v[:8])
	}
	return generatedTo, executedTo, nil
}

type historyCursor interface {
	Seek() (key1, key2, key3, val []byte, err error)
	Next() (key1, key2, key3, val []byte, err error)
}

func NewChangesetSearchDecorator(historyCursor historyCursor, tx ethdb.Tx, bucketName string, startKey []byte, matchBits, part1End, part2Start, part3Start int, timestamp uint64, csWalker changeset.Walker2) *changesetSearchDecorator {
	matchBytes, mask := ethdb.Bytesmask(matchBits)
	return &changesetSearchDecorator{
		startKey:      startKey,
		historyCursor: historyCursor,
		part1End:      part1End,
		part2Start:    part2Start,
		part3Start:    part3Start,

		bucketName: bucketName,
		tx:         tx,
		timestamp:  timestamp,
		matchBytes: matchBytes,
		byteMask:   mask,
		csWalker:   csWalker,
	}
}

type changesetSearchDecorator struct {
	startKey      []byte
	historyCursor historyCursor
	part1End      int
	part2Start    int
	part3Start    int
	matchBytes    int
	byteMask      byte

	bucketName string
	tx         ethdb.Tx
	timestamp  uint64
	csWalker   changeset.Walker2

	pos    int
	values []changeset.Change

	kd1, kd2, kd3, dv []byte
	kc1, kc2, kc3, cv []byte
	cerr              error
}

func NextForChunkedData(oldAddr, oldKey []byte, cursor historyCursor, timestamp uint64) ([]byte, []byte, []byte, []byte, error) {
	hAddrHash, hKeyHash, tsEnc, hV, err2 := cursor.Next()
	if err2 != nil {
		return nil, nil, nil, nil, err2
	}

	for bytes.Equal(hAddrHash, oldAddr) && bytes.Equal(hKeyHash, oldKey) {
		hAddrHash, hKeyHash, tsEnc, hV, err2 = cursor.Next()
		if err2 != nil {
			return nil, nil, nil, nil, err2
		}
	}

	hAddrHash0 := hAddrHash
	hKeyHash0 := hKeyHash
	for bytes.Equal(hAddrHash, hAddrHash0) && bytes.Equal(hKeyHash, hKeyHash0) && tsEnc != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
		hAddrHash, hKeyHash, tsEnc, hV, err2 = cursor.Next()
		if err2 != nil {
			return nil, nil, nil, nil, err2
		}
	}
	return hAddrHash, hKeyHash, tsEnc, hV, nil
}

func (csd *changesetSearchDecorator) Seek() ([]byte, []byte, []byte, []byte, error) {
	pos := sort.Search(len(csd.values), func(i int) bool {
		return bytes.Compare(csd.startKey, csd.values[i].Key) < 0
	})

	if pos < len(csd.values) {
		csd.pos = pos
		if !csd.matchKey(csd.values[csd.pos].Key) {
			csd.kd1 = nil
			csd.kd2 = nil
			csd.kd3 = nil
			csd.dv = nil
		} else {
			csd.kd1 = csd.values[csd.pos].Key[:csd.part1End]
			csd.kd2 = csd.values[csd.pos].Key[csd.part2Start:csd.part3Start]
			csd.kd3 = csd.values[csd.pos].Key[csd.part3Start:]
			csd.dv = csd.values[csd.pos].Value
		}
	}

	hAddrHash, hKeyHash, tsEnc, hV, err2 := csd.historyCursor.Seek()
	if err2 != nil {
		return nil, nil, nil, nil, err2
	}

	hAddrHash0 := hAddrHash
	hKeyHash0 := hKeyHash
	//find first chunk after timestamp
	for hAddrHash != nil && tsEnc != nil && bytes.Equal(hAddrHash, hAddrHash0) && bytes.Equal(hKeyHash, hKeyHash0) && binary.BigEndian.Uint64(tsEnc) < csd.timestamp {
		hAddrHash, hKeyHash, tsEnc, hV, err2 = csd.historyCursor.Next()
		if err2 != nil {
			return nil, nil, nil, nil, err2
		}
	}

	if len(hAddrHash) > 0 {
		hK := make([]byte, len(hAddrHash)+len(hKeyHash))
		copy(hK[:len(hAddrHash)], hAddrHash)
		copy(hK[len(hAddrHash):], hKeyHash)
		data, found, innerErr := findInHistory(hK, hV, csd.timestamp, csd.csWalker)
		if innerErr != nil {
			return nil, nil, nil, nil, innerErr
		}
		csd.kc1, csd.kc2, csd.kc3, csd.cv = hAddrHash, hKeyHash, tsEnc, data
		if !found {
			csd.cerr = ErrNotInHistory
		} else {
			csd.cerr = nil
		}
	} else {
		csd.kc1, csd.kc2, csd.kc3, csd.cv, csd.cerr = nil, nil, nil, nil, nil
	}

	cmp, br := common.KeyCmp(csd.kd1, csd.kc1)
	if br {
		return nil, nil, nil, nil, nil
	}
	if cmp == 0 {
		cmp, br = common.KeyCmp(csd.kd2, csd.kc2)
	}
	if br {
		return nil, nil, nil, nil, nil
	}

	var key1, key2, key3, val []byte
	var err error
	if cmp < 0 {
		key1 = csd.kd1
		key2 = csd.kd2
		key3 = csd.kd3
		val = csd.dv
	} else {
		key1 = csd.kc1
		key2 = csd.kc2
		key3 = csd.kc3
		val = csd.cv
		err = csd.cerr

	}
	//shift changesets cursor
	if cmp <= 0 {
		csd.pos++
		if csd.pos < len(csd.values) {
			if !csd.matchKey(csd.values[csd.pos].Key) {
				csd.kd1 = nil
				csd.kd2 = nil
				csd.kd3 = nil
				csd.dv = nil
			} else {
				csd.kd1 = csd.values[csd.pos].Key[:csd.part1End]
				csd.kd2 = csd.values[csd.pos].Key[csd.part2Start:csd.part3Start]
				csd.kd3 = csd.values[csd.pos].Key[csd.part3Start:]
				csd.dv = csd.values[csd.pos].Value
			}
		} else {
			csd.kd1 = nil
			csd.kd2 = nil
			csd.kd3 = nil
			csd.dv = nil
		}
	}

	//shift history cursor
	if cmp >= 0 {
		hAddrHash, hKeyHash, tsEnc, hV, err2 := NextForChunkedData(common.CopyBytes(csd.kc1), common.CopyBytes(csd.kc2), csd.historyCursor, csd.timestamp)
		if err2 != nil {
			return nil, nil, nil, nil, err2
		}

		if len(hAddrHash) > 0 {
			hK := make([]byte, len(hAddrHash)+len(hKeyHash))
			copy(hK[:len(hAddrHash)], hAddrHash)
			copy(hK[len(hAddrHash):], hKeyHash)
			data, found, innderErr := findInHistory(hK, hV, csd.timestamp, csd.csWalker)
			if innderErr != nil {
				return nil, nil, nil, nil, innderErr
			}
			csd.kc1, csd.kc2, csd.kc3, csd.cv = hAddrHash, hKeyHash, tsEnc, data
			if !found {
				csd.cerr = ErrNotInHistory
			} else {
				csd.cerr = nil
			}
		} else {
			csd.kc1, csd.kc2, csd.kc3, csd.cv, csd.cerr = nil, nil, nil, nil, nil
		}

	}

	return key1, key2, key3, val, err
}
func (csd *changesetSearchDecorator) Next() ([]byte, []byte, []byte, []byte, error) {
	cmp, br := common.KeyCmp(csd.kd1, csd.kc1)
	if br {
		return nil, nil, nil, nil, nil
	}
	if cmp == 0 {
		cmp, br = common.KeyCmp(csd.kd2, csd.kc2)
	}
	if br {
		return nil, nil, nil, nil, nil
	}

	var key1, key2, key3, val []byte
	var err error
	if cmp < 0 {
		key1 = csd.kd1
		key2 = csd.kd2
		key3 = csd.kd3
		val = csd.dv
	} else {
		key1 = csd.kc1
		key2 = csd.kc2
		key3 = csd.kc3
		val = csd.cv
		err = csd.cerr

	}
	//shift changesets cursor
	if cmp <= 0 {
		csd.pos++
		if csd.pos < len(csd.values) {
			if !csd.matchKey(csd.values[csd.pos].Key) {
				csd.kd1 = nil
				csd.kd2 = nil
				csd.kd3 = nil
				csd.dv = nil
			} else {
				csd.kd1 = csd.values[csd.pos].Key[:csd.part1End]
				csd.kd2 = csd.values[csd.pos].Key[csd.part2Start:csd.part3Start]
				csd.kd3 = csd.values[csd.pos].Key[csd.part3Start:]
				csd.dv = csd.values[csd.pos].Value
			}
		} else {
			csd.kd1 = nil
			csd.kd2 = nil
			csd.kd3 = nil
			csd.dv = nil
		}
	}

	//shift history cursor
	if cmp >= 0 {
		hAddrHash, hKeyHash, tsEnc, hV, err2 := NextForChunkedData(common.CopyBytes(csd.kc1), common.CopyBytes(csd.kc2), csd.historyCursor, csd.timestamp)
		if err2 != nil {
			return nil, nil, nil, nil, err2
		}
		if len(hAddrHash) > 0 {
			hK := make([]byte, len(hAddrHash)+len(hKeyHash))
			copy(hK[:len(hAddrHash)], hAddrHash)
			copy(hK[len(hAddrHash):], hKeyHash)
			data, found, innderErr := findInHistory(hK, hV, csd.timestamp, csd.csWalker)
			if innderErr != nil {
				return nil, nil, nil, nil, innderErr
			}
			csd.kc1, csd.kc2, csd.kc3, csd.cv = hAddrHash, hKeyHash, tsEnc, data
			if !found {
				csd.cerr = ErrNotInHistory
			} else {
				csd.cerr = nil
			}
		} else {
			csd.kc1, csd.kc2, csd.kc3, csd.cv = nil, nil, nil, nil
		}
	}

	return key1, key2, key3, val, err
}

var ErrNotInHistory = errors.New("not in history")

func (csd *changesetSearchDecorator) matchKey(k []byte) bool {
	if k == nil {
		return false
	}
	if csd.matchBytes == 0 {
		return true
	}
	if len(k) < csd.matchBytes {
		return false
	}
	if !bytes.Equal(k[:csd.matchBytes-1], csd.startKey[:csd.matchBytes-1]) {
		return false
	}
	return (k[csd.matchBytes-1] & csd.byteMask) == (csd.startKey[csd.matchBytes-1] & csd.byteMask)
}

func (csd *changesetSearchDecorator) buildChangeset(from, to uint64) error {
	if from >= to {
		return nil
	}

	mp := make(map[string][]byte)
	err := csd.csWalker.WalkReverse(from, to, func(bN uint64, k, v []byte) error {
		replace := bN >= csd.timestamp
		if replace {
			mp[string(k)] = v
		}
		return nil
	})
	if err != nil {
		return err
	}

	res := make([]changeset.Change, len(mp))
	i := 0
	for k := range mp {
		res[i] = changeset.Change{
			Key:   []byte(k),
			Value: mp[k],
		}
		i++
	}
	sort.Slice(res, func(i, j int) bool {
		cmp := bytes.Compare(res[i].Key, res[j].Key)
		return cmp <= 0
	})
	csd.values = res
	return nil
}
