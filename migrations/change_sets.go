package migrations

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

var accChangeSetDupSort = Migration{
	Name: "acc_change_set_dup_sort_18",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "data migration: change_set_dup_sort"

		const loadStep = "load"

		changeSetBucket := dbutils.PlainAccountChangeSetBucket
		cmp := db.(ethdb.HasTx).Tx().Comparator(dbutils.PlainStorageChangeSetBucket)
		buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
		buf.SetComparator(cmp)

		collectorR, err1 := etl.NewCollectorFromFiles(tmpdir)
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if collectorR != nil {
				collectorR.Close(logPrefix)
				collectorR = nil
			}

		case loadStep:
			if collectorR == nil {
				return ErrMigrationETLFilesDeleted
			}
			defer func() {
				// don't clean if error or panic happened
				if err != nil {
					return
				}
				if rec := recover(); rec != nil {
					panic(rec)
				}
				collectorR.Close(logPrefix)
			}()
			goto LoadStep
		}

		collectorR = etl.NewCriticalCollector(tmpdir, buf)
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			collectorR.Close(logPrefix)
		}()

		if err = db.Walk(changeSetBucket, nil, 0, func(kk, changesetBytes []byte) (bool, error) {
			blockNum, _ := dbutils.DecodeTimestamp(kk)

			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Progress2", logPrefix), "blockNum", blockNum)
			}

			if err = accountChangeSetPlainBytesOld(changesetBytes).Walk(func(k, v []byte) error {
				newK := make([]byte, 8)
				binary.BigEndian.PutUint64(newK, blockNum)

				newV := make([]byte, len(k)+len(v))
				copy(newV, k)
				copy(newV[len(k):], v)
				return collectorR.Collect(newK, newV)
			}); err != nil {
				return false, err
			}

			return true, nil
		}); err != nil {
			return err
		}

		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.PlainAccountChangeSetBucket); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table: %w", err)
		}

	LoadStep:
		// Commit again
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table: %w", err)
		}
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = collectorR.Load(logPrefix, db, dbutils.PlainAccountChangeSetBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the receipts table: %w", err)
		}
		return nil
	},
}

var storageChangeSetDupSort = Migration{
	Name: "storage_change_set_dup_sort_22",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		logPrefix := "data migration: storage_change_set_dup_sort"

		const loadStep = "load"
		changeSetBucket := dbutils.PlainStorageChangeSetBucket
		cmp := db.(ethdb.HasTx).Tx().Comparator(dbutils.PlainStorageChangeSetBucket)
		buf := etl.NewSortableBuffer(etl.BufferOptimalSize)
		buf.SetComparator(cmp)

		collectorR, err1 := etl.NewCollectorFromFiles(tmpdir)
		if err1 != nil {
			return err1
		}
		switch string(progress) {
		case "":
			// can't use files if progress field not set, clear them
			if collectorR != nil {
				collectorR.Close(logPrefix)
				collectorR = nil
			}

		case loadStep:
			if collectorR == nil {
				return ErrMigrationETLFilesDeleted
			}
			defer func() {
				// don't clean if error or panic happened
				if err != nil {
					return
				}
				if rec := recover(); rec != nil {
					panic(rec)
				}
				collectorR.Close(logPrefix)
			}()
			goto LoadStep
		}

		collectorR = etl.NewCriticalCollector(tmpdir, buf)
		defer func() {
			// don't clean if error or panic happened
			if err != nil {
				return
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
			collectorR.Close(logPrefix)
		}()

		if err = db.Walk(changeSetBucket, nil, 0, func(kk, changesetBytes []byte) (bool, error) {
			blockNum, _ := dbutils.DecodeTimestamp(kk)

			select {
			default:
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Progress", logPrefix), "blockNum", blockNum)
			}

			if err = storageChangeSetPlainBytesOld(changesetBytes).Walk(func(k, v []byte) error {
				newK := make([]byte, 8+20+8)
				binary.BigEndian.PutUint64(newK, blockNum)
				copy(newK[8:], k[:20+8])

				newV := make([]byte, 32+len(v))
				copy(newV, k[20+8:])
				copy(newV[32:], v)
				return collectorR.Collect(newK, newV)
			}); err != nil {
				return false, err
			}

			return true, nil
		}); err != nil {
			return err
		}

		if err = db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.PlainStorageChangeSetBucket); err != nil {
			return fmt.Errorf("clearing the receipt bucket: %w", err)
		}

		// Commit clearing of the bucket - freelist should now be written to the database
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table: %w", err)
		}

	LoadStep:
		// Commit again
		if err = CommitProgress(db, []byte(loadStep), false); err != nil {
			return fmt.Errorf("committing the removal of receipt table: %w", err)
		}
		// Now transaction would have been re-opened, and we should be re-using the space
		if err = collectorR.Load(logPrefix, db, dbutils.PlainStorageChangeSetBucket, etl.IdentityLoadFunc, etl.TransformArgs{
			OnLoadCommit: CommitProgress,
			Comparator:   cmp,
		}); err != nil {
			return fmt.Errorf("loading the transformed data back into the receipts table: %w", err)
		}
		return nil
	},
}

// ---- Copy-Paste of code to decode ChangeSets: Begin -----

type accountChangeSetPlainBytesOld []byte

func (b accountChangeSetPlainBytesOld) Walk(f func(k, v []byte) error) error {
	return walkAccountChangeSet(b, common.AddressLength, f)
}

// walkAccountChangeSet iterates the account bytes with the keys of provided size
func walkAccountChangeSet(b []byte, keyLen uint32, f func(k, v []byte) error) error {
	if len(b) == 0 {
		return nil
	}
	if len(b) < 4 {
		return fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	n := binary.BigEndian.Uint32(b[0:4])

	if n == 0 {
		return nil
	}
	valOffset := 4 + n*keyLen + 4*n
	if uint32(len(b)) < valOffset {
		fmt.Println("walkAccounts account")
		return fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset)
	}

	totalValLength := binary.BigEndian.Uint32(b[valOffset-4 : valOffset])
	if uint32(len(b)) < valOffset+totalValLength {
		return fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset+totalValLength)
	}

	for i := uint32(0); i < n; i++ {
		key := b[4+i*keyLen : 4+(i+1)*keyLen]
		idx0 := uint32(0)
		if i > 0 {
			idx0 = binary.BigEndian.Uint32(b[4+n*keyLen+4*(i-1) : 4+n*keyLen+4*i])
		}
		idx1 := binary.BigEndian.Uint32(b[4+n*keyLen+4*i : 4+n*keyLen+4*(i+1)])
		val := b[valOffset+idx0 : valOffset+idx1]

		err := f(key, val)
		if err != nil {
			return err
		}
	}
	return nil
}

type storageChangeSetPlainBytesOld []byte

func (b storageChangeSetPlainBytesOld) Walk(f func(k, v []byte) error) error {
	return walkStorageChangeSet(b, common.AddressLength, f)
}

func walkStorageChangeSet(b []byte, keyPrefixLen int, f func(k, v []byte) error) error {
	if len(b) == 0 {
		return nil
	}

	if len(b) < 4 {
		return fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfUniqueElements := int(binary.BigEndian.Uint32(b))
	if numOfUniqueElements == 0 {
		return nil
	}
	incarnatonsInfo := 4 + numOfUniqueElements*(keyPrefixLen+4)
	numOfNotDefaultIncarnations := int(binary.BigEndian.Uint32(b[incarnatonsInfo:]))
	incarnatonsStart := incarnatonsInfo + 4

	notDefaultIncarnations := make(map[uint32]uint64, numOfNotDefaultIncarnations)
	if numOfNotDefaultIncarnations > 0 {
		for i := 0; i < numOfNotDefaultIncarnations; i++ {
			notDefaultIncarnations[binary.BigEndian.Uint32(b[incarnatonsStart+i*12:])] = binary.BigEndian.Uint64(b[incarnatonsStart+i*12+4:])
		}
	}

	keysStart := incarnatonsStart + numOfNotDefaultIncarnations*12
	numOfElements := int(binary.BigEndian.Uint32(b[incarnatonsInfo-4:]))
	valsInfoStart := keysStart + numOfElements*common.HashLength

	var addressHashID uint32
	var id int
	k := make([]byte, keyPrefixLen+common.HashLength+common.IncarnationLength)
	for i := 0; i < numOfUniqueElements; i++ {
		var (
			startKeys int
			endKeys   int
		)

		if i > 0 {
			startKeys = int(binary.BigEndian.Uint32(b[4+i*(keyPrefixLen)+(i-1)*4 : 4+i*(keyPrefixLen)+(i)*4]))
		}
		endKeys = int(binary.BigEndian.Uint32(b[4+(i+1)*(keyPrefixLen)+i*4:]))
		addrBytes := b[4+i*(keyPrefixLen)+i*4:] // hash or raw address
		incarnation := changeset.DefaultIncarnation
		if inc, ok := notDefaultIncarnations[addressHashID]; ok {
			incarnation = inc
		}

		for j := startKeys; j < endKeys; j++ {
			copy(k[:keyPrefixLen], addrBytes[:keyPrefixLen])
			binary.BigEndian.PutUint64(k[keyPrefixLen:], incarnation)
			copy(k[keyPrefixLen+common.IncarnationLength:keyPrefixLen+common.HashLength+common.IncarnationLength], b[keysStart+j*common.HashLength:])
			val, innerErr := findValue(b[valsInfoStart:], id)
			if innerErr != nil {
				return innerErr
			}
			err := f(k, val)
			if err != nil {
				return err
			}
			id++
		}
		addressHashID++
	}

	return nil
}

func findValue(b []byte, i int) ([]byte, error) {
	numOfUint8 := int(binary.BigEndian.Uint32(b[0:]))
	numOfUint16 := int(binary.BigEndian.Uint32(b[4:]))
	numOfUint32 := int(binary.BigEndian.Uint32(b[8:]))
	//after num of values
	lenOfValsStartPointer := 12
	valsPointer := lenOfValsStartPointer + numOfUint8 + numOfUint16*2 + numOfUint32*4
	var (
		lenOfValStart int
		lenOfValEnd   int
	)

	switch {
	case i < numOfUint8:
		lenOfValEnd = int(b[lenOfValsStartPointer+i])
		if i > 0 {
			lenOfValStart = int(b[lenOfValsStartPointer+i-1])
		}
	case i < numOfUint8+numOfUint16:
		one := (i-numOfUint8)*2 + numOfUint8
		lenOfValEnd = int(binary.BigEndian.Uint16(b[lenOfValsStartPointer+one : lenOfValsStartPointer+one+2]))
		if i-1 < numOfUint8 {
			lenOfValStart = int(b[lenOfValsStartPointer+i-1])
		} else {
			one = (i-1)*2 - numOfUint8
			lenOfValStart = int(binary.BigEndian.Uint16(b[lenOfValsStartPointer+one : lenOfValsStartPointer+one+2]))
		}
	case i < numOfUint8+numOfUint16+numOfUint32:
		one := lenOfValsStartPointer + numOfUint8 + numOfUint16*2 + (i-numOfUint8-numOfUint16)*4
		lenOfValEnd = int(binary.BigEndian.Uint32(b[one : one+4]))
		if i-1 < numOfUint8+numOfUint16 {
			one = lenOfValsStartPointer + (i-1)*2 - numOfUint8
			lenOfValStart = int(binary.BigEndian.Uint16(b[one : one+2]))
		} else {
			one = lenOfValsStartPointer + numOfUint8 + numOfUint16*2 + (i-1-numOfUint8-numOfUint16)*4
			lenOfValStart = int(binary.BigEndian.Uint32(b[one : one+4]))
		}
	default:
		return nil, changeset.ErrFindValue
	}
	return b[valsPointer+lenOfValStart : valsPointer+lenOfValEnd], nil
}

//nolint:unused,deadcode
type contractKeys struct {
	AddrBytes   []byte // either a hash of address or raw address
	Incarnation uint64
	Keys        [][]byte
	Vals        [][]byte
}

//nolint:unused,deadcode
func encodeStorage(s *changeset.ChangeSet, keyPrefixLen uint32) ([]byte, error) {
	sort.Sort(s)
	var err error
	buf := new(bytes.Buffer)
	uint16Arr := make([]byte, 2)
	uint32Arr := make([]byte, 4)
	numOfElements := s.Len()

	keys := make([]contractKeys, 0, numOfElements)
	valLengthes := make([]byte, 0, numOfElements)
	var (
		currentContract contractKeys
		numOfUint8      uint32
		numOfUint16     uint32
		numOfUint32     uint32
		lengthOfValues  uint32
	)
	var nonDefaultIncarnationCounter uint32
	//first 4 bytes - len. body -  []{idOfAddrHash(4) +  incarnation(8)}
	notDefaultIncarnationsBytes := make([]byte, 4)
	b := make([]byte, 12)

	currentKey := -1
	for i, change := range s.Changes {
		addrBytes := change.Key[0:keyPrefixLen] // hash or raw address
		incarnation := binary.BigEndian.Uint64(change.Key[keyPrefixLen:])
		keyBytes := change.Key[keyPrefixLen+common.IncarnationLength : keyPrefixLen+common.HashLength+common.IncarnationLength] // hash or raw key
		//found new contract address
		if i == 0 || !bytes.Equal(currentContract.AddrBytes, addrBytes) || currentContract.Incarnation != incarnation {
			currentKey++
			currentContract.AddrBytes = addrBytes
			currentContract.Incarnation = incarnation
			//add to incarnations part only if it's not default
			if incarnation != changeset.DefaultIncarnation {
				binary.BigEndian.PutUint32(b[0:], uint32(currentKey))
				binary.BigEndian.PutUint64(b[4:], incarnation)
				notDefaultIncarnationsBytes = append(notDefaultIncarnationsBytes, b...)
				nonDefaultIncarnationCounter++
			}
			currentContract.Keys = [][]byte{keyBytes}
			currentContract.Vals = [][]byte{change.Value}
			keys = append(keys, currentContract)
		} else {
			//add key and value
			currentContract.Keys = append(currentContract.Keys, keyBytes)
			currentContract.Vals = append(currentContract.Vals, change.Value)
		}

		//calculate lengths of values
		lengthOfValues += uint32(len(change.Value))
		switch {
		case lengthOfValues <= 255:
			valLengthes = append(valLengthes, uint8(lengthOfValues))
			numOfUint8++

		case lengthOfValues <= 65535:
			binary.BigEndian.PutUint16(uint16Arr, uint16(lengthOfValues))
			valLengthes = append(valLengthes, uint16Arr...)
			numOfUint16++

		default:
			binary.BigEndian.PutUint32(uint32Arr, lengthOfValues)
			valLengthes = append(valLengthes, uint32Arr...)
			numOfUint32++
		}

		//save to array
		keys[currentKey] = currentContract
	}

	// save numOfUniqueContracts
	binary.BigEndian.PutUint32(uint32Arr, uint32(len(keys)))
	if _, err = buf.Write(uint32Arr); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("incorrect data")
	}

	// save addrHashes + endOfKeys
	var endNumOfKeys int
	for i := 0; i < len(keys); i++ {
		if _, err = buf.Write(keys[i].AddrBytes); err != nil {
			return nil, err
		}
		endNumOfKeys += len(keys[i].Keys)

		//end of keys
		binary.BigEndian.PutUint32(uint32Arr, uint32(endNumOfKeys))
		if _, err = buf.Write(uint32Arr); err != nil {
			return nil, err
		}
	}

	if endNumOfKeys != numOfElements {
		return nil, fmt.Errorf("incorrect number of elements must:%v current:%v", numOfElements, endNumOfKeys)
	}

	// save not default incarnations
	binary.BigEndian.PutUint32(notDefaultIncarnationsBytes, nonDefaultIncarnationCounter)
	if _, err = buf.Write(notDefaultIncarnationsBytes); err != nil {
		return nil, err
	}

	// save keys
	for _, group := range keys {
		for _, v := range group.Keys {
			if _, err = buf.Write(v); err != nil {
				return nil, err
			}
		}
	}

	// save lengths of values
	binary.BigEndian.PutUint32(uint32Arr, numOfUint8)
	if _, err = buf.Write(uint32Arr); err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(uint32Arr, numOfUint16)
	if _, err = buf.Write(uint32Arr); err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(uint32Arr, numOfUint32)
	if _, err = buf.Write(uint32Arr); err != nil {
		return nil, err
	}

	if _, err = buf.Write(valLengthes); err != nil {
		return nil, err
	}

	// save values
	for _, v := range keys {
		for _, val := range v.Vals {
			if _, err = buf.Write(val); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

/*
AccountChangeSet is serialized in the following manner in order to facilitate binary search:
1. The number of keys N (uint32, 4 bytes).
2. Contiguous array of keys (N*M bytes).
3. Contiguous array of accumulating value indexes:
len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{N-1})
(4*N bytes since the lengths are treated as uint32).
4. Contiguous array of values.

uint32 integers are serialized as big-endian.
*/
//nolint:unused,deadcode
func encodeAccounts(s *changeset.ChangeSet) ([]byte, error) {
	sort.Sort(s)
	buf := new(bytes.Buffer)
	intArr := make([]byte, 4)
	n := s.Len()
	binary.BigEndian.PutUint32(intArr, uint32(n))
	_, err := buf.Write(intArr)
	if err != nil {
		return nil, err
	}

	for i := 0; i < n; i++ {
		_, err = buf.Write(s.Changes[i].Key)
		if err != nil {
			return nil, err
		}
	}

	var l int
	for i := 0; i < n; i++ {
		l += len(s.Changes[i].Value)
		binary.BigEndian.PutUint32(intArr, uint32(l))
		_, err = buf.Write(intArr)
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < n; i++ {
		_, err = buf.Write(s.Changes[i].Value)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// ---- Copy-Paste of code to decode ChangeSets: End -----

var clearHashedChangesets = Migration{
	Name: "clear_hashed_changesets",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {
		if err := db.(ethdb.BucketsMigrator).ClearBuckets(dbutils.AccountChangeSetBucket, dbutils.StorageChangeSetBucket); err != nil {
			return err
		}

		return OnLoadCommit(db, nil, true)
	},
}
