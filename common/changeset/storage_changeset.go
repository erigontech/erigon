package changeset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

const (
	DefaultIncarnation = uint64(1)
	storageKeyLen      = common.AddressLength + common.HashLength + common.IncarnationLength
)

var (
	ErrNotFound      = errors.New("not found")
	errIncorrectData = errors.New("empty prepared data")
	ErrFindValue     = errors.New("find value error")
)

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  storageKeyLen,
	}
}

/**
numOfElements uint32
numOfUniqueContracts uint16
[]{
	addrhash common.Hash
	numOfStorageKeysToSkip uint16
}
keys     []common.Hash numOfElements
 lenToValue []uint16,
numOfUint8Values uint16
numOfUint16Values uint16
numOfUint32Values uint16
[len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint8Values-1})] []uint8
[len(valnumOfUint8Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint16Values-1})] []uint16
[len(valnumOfUint16Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint32Values-1})] []uint32
[elementNum:incarnation] -  optional [uint32:uint64...]

*/

func EncodeStorage(s *ChangeSet) ([]byte, error) {
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
		address, incarnation, key := dbutils.PlainParseCompositeStorageKey(change.Key)
		//found new contract address
		if i == 0 || !bytes.Equal(currentContract.Address[:], address[:]) || currentContract.Incarnation != incarnation {
			currentKey++
			currentContract.Address = address
			currentContract.Incarnation = incarnation
			//add to incarnations part only if it's not default
			if incarnation != DefaultIncarnation {
				binary.BigEndian.PutUint32(b[0:], uint32(currentKey))
				binary.BigEndian.PutUint64(b[4:], ^incarnation)
				notDefaultIncarnationsBytes = append(notDefaultIncarnationsBytes, b...)
				nonDefaultIncarnationCounter++
			}
			currentContract.Keys = [][]byte{key[:]}
			currentContract.Vals = [][]byte{change.Value}
			keys = append(keys, currentContract)
		} else {
			//add key and value
			currentContract.Keys = append(currentContract.Keys, key[:])
			currentContract.Vals = append(currentContract.Vals, change.Value)
		}

		//calculate lengthes of values
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
		return nil, errIncorrectData
	}

	// save addrHashes + endOfKeys
	var endNumOfKeys int
	for i := 0; i < len(keys); i++ {
		if _, err = buf.Write(keys[i].Address[:]); err != nil {
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

func DecodeStorage(b []byte) (*ChangeSet, error) {
	numOfUniqueElements := int(binary.BigEndian.Uint32(b))
	if numOfUniqueElements == 0 {
		return &ChangeSet{
			Changes: make([]Change, 0),
			keyLen:  72,
		}, nil
	}

	keys := make([]contractKeys, numOfUniqueElements)
	numOfSkipKeys := make([]int, numOfUniqueElements+1)
	for i := 0; i < numOfUniqueElements; i++ {
		start := 4 + i*(common.AddressLength+4)
		keys[i].Address = common.BytesToAddress(b[start : start+common.AddressLength])
		numOfSkipKeys[i+1] = int(binary.BigEndian.Uint32(b[start+common.AddressLength:]))
		keys[i].Incarnation = DefaultIncarnation
	}
	numOfElements := numOfSkipKeys[numOfUniqueElements]
	incarnatonsInfo := 4 + numOfUniqueElements*(common.AddressLength+4)
	numOfNotDefaultIncarnations := int(binary.BigEndian.Uint32(b[incarnatonsInfo:]))

	incarnationsStart := incarnatonsInfo + 4
	if numOfNotDefaultIncarnations > 0 {
		for i := 0; i < numOfNotDefaultIncarnations; i++ {
			id := binary.BigEndian.Uint32(b[incarnationsStart+i*12:])
			keys[id].Incarnation = ^binary.BigEndian.Uint64(b[incarnationsStart+i*12+4:])
		}
	}

	keysStart := incarnationsStart + numOfNotDefaultIncarnations*12

	for i := 0; i < numOfUniqueElements; i++ {
		keys[i].Keys = make([][]byte, 0, numOfSkipKeys[i+1]-numOfSkipKeys[i])
		for j := numOfSkipKeys[i]; j < numOfSkipKeys[i+1]; j++ {
			keys[i].Keys = append(keys[i].Keys, b[keysStart+j*common.HashLength:keysStart+(j+1)*common.HashLength])
		}
	}

	valsInfoStart := keysStart + numOfElements*common.HashLength
	cs := NewStorageChangeSet()
	cs.Changes = make([]Change, numOfElements)
	id := 0
	for _, v := range keys {
		for i := range v.Keys {
			k := dbutils.PlainGenerateCompositeStorageKey(v.Address, v.Incarnation, common.BytesToHash(v.Keys[i]))
			val, innerErr := FindValue(b[valsInfoStart:], id)
			if innerErr != nil {
				return nil, innerErr
			}
			cs.Changes[id] = Change{
				Key:   k,
				Value: val,
			}

			id++
		}
	}

	return cs, nil
}

func FindValue(b []byte, i int) ([]byte, error) {
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
		return nil, ErrFindValue
	}
	return b[valsPointer+lenOfValStart : valsPointer+lenOfValEnd], nil
}

type StorageChangeSetBytes []byte

func (b StorageChangeSetBytes) Walk(f func(k, v []byte) error) error {
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
	incarnatonsInfo := 4 + numOfUniqueElements*(common.AddressLength+4)
	numOfNotDefaultIncarnations := int(binary.BigEndian.Uint32(b[incarnatonsInfo:]))
	incarnatonsStart := incarnatonsInfo + 4

	notDefaultIncarnations := make(map[uint32]uint64, numOfNotDefaultIncarnations)
	if numOfNotDefaultIncarnations > 0 {
		for i := 0; i < numOfNotDefaultIncarnations; i++ {
			notDefaultIncarnations[binary.BigEndian.Uint32(b[incarnatonsStart+i*12:])] = ^binary.BigEndian.Uint64(b[incarnatonsStart+i*12+4:])
		}
	}

	keysStart := incarnatonsStart + numOfNotDefaultIncarnations*12
	numOfElements := int(binary.BigEndian.Uint32(b[incarnatonsInfo-4:]))
	valsInfoStart := keysStart + numOfElements*common.HashLength

	var addressHashID uint32
	var id int
	for i := 0; i < numOfUniqueElements; i++ {
		var (
			startKeys int
			endKeys   int
		)

		if i > 0 {
			startKeys = int(binary.BigEndian.Uint32(b[4+i*(common.AddressLength)+(i-1)*4 : 4+i*(common.AddressLength)+(i)*4]))
		}
		endKeys = int(binary.BigEndian.Uint32(b[4+(i+1)*(common.AddressLength)+i*4:]))
		addrBytes := b[4+i*(common.AddressLength)+i*4:]
		incarnation := DefaultIncarnation
		if inc, ok := notDefaultIncarnations[addressHashID]; ok {
			incarnation = inc
		}

		for j := startKeys; j < endKeys; j++ {
			k := dbutils.PlainGenerateCompositeStorageKey(common.BytesToAddress(addrBytes[:common.AddressLength]), incarnation, common.BytesToHash(b[keysStart+j*common.HashLength:keysStart+j*common.HashLength+common.HashLength]))
			val, innerErr := FindValue(b[valsInfoStart:], id)
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

func (b StorageChangeSetBytes) Find(k []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfUniqueElements := int(binary.BigEndian.Uint32(b))
	if numOfUniqueElements == 0 {
		return nil, ErrNotFound
	}

	incarnatonsInfo := 4 + numOfUniqueElements*(common.AddressLength+4)
	numOfElements := int(binary.BigEndian.Uint32(b[incarnatonsInfo-4:]))
	numOfNotDefaultIncarnations := int(binary.BigEndian.Uint32(b[incarnatonsInfo:]))
	incarnatonsStart := incarnatonsInfo + 4
	keysStart := incarnatonsStart + numOfNotDefaultIncarnations*12
	valsInfoStart := keysStart + numOfElements*common.HashLength

	addressID := sort.Search(numOfUniqueElements, func(i int) bool {
		addrBytes := b[4+i*(4+common.AddressLength) : 4+i*(4+common.AddressLength)+common.AddressLength]
		cmp := bytes.Compare(addrBytes, k[0:common.AddressLength])
		return cmp >= 0
	})

	if addressID == numOfUniqueElements {
		return nil, ErrNotFound
	}

	from := 0
	if addressID > 0 {
		from = int(binary.BigEndian.Uint32(b[4+addressID*(common.AddressLength+4)-4:]))
	}
	to := int(binary.BigEndian.Uint32(b[4+addressID*(common.AddressLength+4)+common.AddressLength:]))

	keyIndex := sort.Search(to-from, func(i int) bool {
		index := from + i
		key := b[keysStart+common.HashLength*index : keysStart+common.HashLength*index+common.HashLength]
		cmp := bytes.Compare(key, k[common.AddressLength+common.IncarnationLength:common.AddressLength+common.HashLength+common.IncarnationLength])
		return cmp >= 0
	})
	index := from + keyIndex
	if index == to {
		return nil, ErrNotFound
	}
	return FindValue(b[valsInfoStart:], index)
}

func (b StorageChangeSetBytes) FindWithoutIncarnation(addrBytesToFind []byte, keyBytesToFind []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfUniqueElements := int(binary.BigEndian.Uint32(b))
	if numOfUniqueElements == 0 {
		return nil, nil
	}
	incarnatonsInfo := 4 + numOfUniqueElements*(common.HashLength+4)
	numOfElements := int(binary.BigEndian.Uint32(b[incarnatonsInfo-4:]))
	numOfNotDefaultIncarnations := int(binary.BigEndian.Uint32(b[incarnatonsInfo:]))
	incarnatonsStart := incarnatonsInfo + 4
	keysStart := incarnatonsStart + numOfNotDefaultIncarnations*12
	valsInfoStart := keysStart + numOfElements*common.HashLength

	addHashID := sort.Search(numOfUniqueElements, func(i int) bool {
		addrBytes := b[4+i*(common.AddressLength+4) : 4+i*(common.AddressLength+4)+common.AddressLength]
		cmp := bytes.Compare(addrBytes, addrBytesToFind)
		return cmp >= 0
	})

	if addHashID == numOfUniqueElements {
		return nil, ErrNotFound
	}
	from := 0
	if addHashID > 0 {
		from = int(binary.BigEndian.Uint32(b[4+addHashID*(common.HashLength+4)-4:]))
	}
	to := int(binary.BigEndian.Uint32(b[4+addHashID*(common.HashLength+4)+common.HashLength:]))
	keyIndex := sort.Search(to-from, func(i int) bool {
		index := from + i
		key := b[keysStart+common.HashLength*index : keysStart+common.HashLength*index+common.HashLength]
		cmp := bytes.Compare(key, keyBytesToFind)
		return cmp >= 0
	})
	index := from + keyIndex
	if index == to {
		return nil, ErrNotFound
	}

	return FindValue(b[valsInfoStart:], index)
}

type contractKeys struct {
	Address     common.Address
	Incarnation uint64
	Keys        [][]byte
	Vals        [][]byte
}
