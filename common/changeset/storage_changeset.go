package changeset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"sort"
)

const (
	DefaultIncarnation                  = uint64(1)
	storageEnodingLengthOfNumOfElements = 4
	storageEnodingLengthOfUniqueElemens = 2
)

var ErrNotFound = errors.New("not found")

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  2*common.HashLength + common.IncarnationLength,
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

	//save numOfElements
	binary.BigEndian.PutUint32(uint32Arr, uint32(numOfElements))
	if _, err := buf.Write(uint32Arr); err != nil {
		return nil, err
	}

	keys := make([]contractKeys, 0, numOfElements)
	valLengthes := make([]byte, 0, numOfElements)
	var (
		currentContract contractKeys
		numOfUint8      uint16
		numOfUint16     uint16
		numOfUint32     uint16
		lengthOfValues  uint32
	)
	notDefaultIncarnationsBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(notDefaultIncarnationsBytes, 0)

	currentKey := -1
	for _, change := range s.Changes {
		//found new contract address
		if !bytes.Equal(currentContract.AddrHash, change.Key[0:common.HashLength]) || currentContract.Incarnation != binary.BigEndian.Uint64(change.Key[common.HashLength:common.HashLength+common.IncarnationLength]) {
			currentKey++
			currentContract.AddrHash = change.Key[0:common.HashLength]
			currentContract.Incarnation = binary.BigEndian.Uint64(change.Key[common.HashLength : common.HashLength+common.IncarnationLength])
			//add to incarnations part
			if currentContract.Incarnation != DefaultIncarnation {
				b := make([]byte, 10)
				binary.BigEndian.PutUint16(b[0:2], uint16(currentKey))
				binary.BigEndian.PutUint64(b[2:10], currentContract.Incarnation)
				notDefaultIncarnationsBytes = append(notDefaultIncarnationsBytes, b...)
				binary.BigEndian.PutUint16(notDefaultIncarnationsBytes[0:2], binary.BigEndian.Uint16(notDefaultIncarnationsBytes[0:2])+1)
			}
			currentContract.Keys = [][]byte{change.Key[common.HashLength+common.IncarnationLength : 2*common.HashLength+common.IncarnationLength]}
			currentContract.Vals = [][]byte{change.Value}
			keys = append(keys, currentContract)
		} else {
			//add key and value
			currentContract.Keys = append(currentContract.Keys, change.Key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength])
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
	binary.BigEndian.PutUint16(uint16Arr, uint16(len(keys)))
	if _, err = buf.Write(uint16Arr); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, errors.New("empty prepared data")
	}

	keysBuf := make([]byte, numOfElements*common.HashLength)
	var endNumOfKeys int

	for i := 0; i < len(keys); i++ {
		_, err = buf.Write(keys[i].AddrHash)
		if err != nil {
			return nil, err
		}
		numOfKeysToSkip := endNumOfKeys
		endNumOfKeys += len(keys[i].Keys)

		//end of keys
		binary.BigEndian.PutUint16(uint16Arr, uint16(endNumOfKeys))
		_, err = buf.Write(uint16Arr)
		if err != nil {
			return nil, err
		}

		//add key to keys
		for j, v := range keys[i].Keys {
			copy(keysBuf[(numOfKeysToSkip+j)*common.HashLength:(numOfKeysToSkip+j+1)*common.HashLength], v[:])
		}
	}

	if endNumOfKeys != numOfElements {
		return nil, fmt.Errorf("incorrect number of elements must:%v current:%v", numOfElements, endNumOfKeys)
	}

	_, err = buf.Write(notDefaultIncarnationsBytes)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(keysBuf)
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint16(uint16Arr, numOfUint8)
	_, err = buf.Write(uint16Arr)
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint16(uint16Arr, numOfUint16)
	_, err = buf.Write(uint16Arr)
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint16(uint16Arr, numOfUint32)
	_, err = buf.Write(uint16Arr)
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(uint32Arr, lengthOfValues)
	_, err = buf.Write(uint32Arr)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(valLengthes)
	if err != nil {
		return nil, err
	}

	for _, v := range keys {
		for _, val := range v.Vals {
			_, err = buf.Write(val)
			if err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func DecodeStorage(b []byte) (*ChangeSet, error) {
	numOfElements := binary.BigEndian.Uint32(b[0:4])
	numOfUniqueElements := binary.BigEndian.Uint16(b[4:6])

	keys := make([]contractKeys, numOfUniqueElements)

	changeSet := &ChangeSet{
		Changes: make([]Change, numOfElements),
		keyLen:  72,
	}

	if numOfElements == 0 {
		return changeSet, nil
	}

	numOfSkipKeys := make([]uint16, numOfUniqueElements+1)
	numOfSkipKeys[numOfUniqueElements] = uint16(numOfElements)
	for i := uint32(0); i < uint32(numOfUniqueElements); i++ {
		var start uint32
		if i > 0 {
			numOfSkipKeys[i] = binary.BigEndian.Uint16(b[6+i*(common.HashLength)+(i-1)*2 : 6+i*(common.HashLength)+(i)*2])
			start = 6 + i*common.HashLength + i*2
		} else {
			start = 6
		}
		keys[i].AddrHash = b[start : start+common.HashLength]
		keys[i].Incarnation = DefaultIncarnation
	}

	incarnatonsInfo := 6 + uint32(numOfUniqueElements)*(common.HashLength+2)
	numOfNotDefaultIncarnations := binary.BigEndian.Uint16(b[incarnatonsInfo : incarnatonsInfo+2])

	incarnationsStart := incarnatonsInfo + 2
	if numOfNotDefaultIncarnations > 0 {
		for i := uint32(0); i < uint32(numOfNotDefaultIncarnations); i++ {
			id := binary.BigEndian.Uint16(b[incarnationsStart+i*10 : incarnationsStart+i*10+2])
			keys[id].Incarnation = binary.BigEndian.Uint64(b[incarnationsStart+i*10+2 : incarnationsStart+i*10+10])
		}
	}

	keysStart := incarnationsStart + uint32(numOfNotDefaultIncarnations)*10

	for i := uint32(0); i < uint32(numOfUniqueElements); i++ {
		for j := numOfSkipKeys[i]; j < numOfSkipKeys[i+1]; j++ {
			if keys[i].Keys == nil {
				keys[i].Keys = make([][]byte, 0, numOfSkipKeys[i+1]-numOfSkipKeys[i])
			}
			keys[i].Keys = append(keys[i].Keys, b[keysStart+uint32(j)*common.HashLength:keysStart+(uint32(j)+1)*common.HashLength])
		}
	}

	valsInfoStart := keysStart + numOfElements*common.HashLength
	cs := NewStorageChangeSet()
	cs.Changes = make([]Change, numOfElements)
	id := uint32(0)
	for _, v := range keys {
		for i := range v.Keys {
			k := make([]byte, common.HashLength*2+common.IncarnationLength)
			copy(k[:common.HashLength], v.AddrHash)
			binary.BigEndian.PutUint64(k[common.HashLength:common.HashLength+common.IncarnationLength], v.Incarnation)
			copy(k[common.HashLength+common.IncarnationLength:common.HashLength*2+common.IncarnationLength], v.Keys[i])
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

func FindValue(b []byte, i uint32) ([]byte, error) {
	numOfUint8 := uint32(binary.BigEndian.Uint16(b[0:2]))
	numOfUint16 := uint32(binary.BigEndian.Uint16(b[2:4]))
	numOfUint32 := uint32(binary.BigEndian.Uint16(b[4:6]))
	lenOfValsStartPointer := uint32(10)
	valsPointer := lenOfValsStartPointer + numOfUint8 + numOfUint16*2 + numOfUint32*4
	var (
		lenOfValStart uint32
		lenOfValEnd   uint32
	)

	switch {
	case i < numOfUint8:
		lenOfValEnd = uint32(b[lenOfValsStartPointer+i])
		if i > 0 {
			lenOfValStart = uint32(b[lenOfValsStartPointer+i-1])
		}
	case i < numOfUint8+numOfUint16:
		one := (i-numOfUint8)*2 + numOfUint8
		lenOfValEnd = uint32(binary.BigEndian.Uint16(b[lenOfValsStartPointer+one : lenOfValsStartPointer+one+2]))
		if i-1 < numOfUint8 {
			lenOfValStart = uint32(b[lenOfValsStartPointer+i-1])
		} else {
			one = (i-1)*2 - numOfUint8
			lenOfValStart = uint32(binary.BigEndian.Uint16(b[lenOfValsStartPointer+one : lenOfValsStartPointer+one+2]))
		}
	case i < numOfUint8+numOfUint16+numOfUint32:
		one := lenOfValsStartPointer + numOfUint8 + numOfUint16*2 + (i-numOfUint8-numOfUint16)*4
		lenOfValEnd = binary.BigEndian.Uint32(b[one : one+4])
		if i-1 < numOfUint8+numOfUint16 {
			one = lenOfValsStartPointer + (i-1)*2 - numOfUint8
			lenOfValStart = uint32(binary.BigEndian.Uint16(b[one : one+2]))
		} else {
			one = lenOfValsStartPointer + numOfUint8 + numOfUint16*2 + (i-1-numOfUint8-numOfUint16)*4
			lenOfValStart = binary.BigEndian.Uint32(b[one : one+4])
		}
	default:
		return nil, errors.New("find value error")
	}
	return common.CopyBytes(b[valsPointer+lenOfValStart : valsPointer+lenOfValEnd]), nil
}

type StorageChangeSetBytes []byte

func (b StorageChangeSetBytes) Walk(f func(k, v []byte) error) error {
	if len(b) == 0 {
		return nil
	}

	if len(b) < 8 {
		return fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfElements := binary.BigEndian.Uint32(b[0:4])
	if numOfElements == 0 {
		return nil
	}

	numOfUniqueElements := binary.BigEndian.Uint16(b[storageEnodingLengthOfNumOfElements : storageEnodingLengthOfNumOfElements+storageEnodingLengthOfUniqueElemens])
	incarnatonsInfo := 6 + uint32(numOfUniqueElements)*(common.HashLength) + uint32((numOfUniqueElements)*2)
	numOfNotDefaultIncarnations := binary.BigEndian.Uint16(b[incarnatonsInfo : incarnatonsInfo+2])
	incarnatonsStart := incarnatonsInfo + 2

	notDefaultIncarnations := make(map[uint16]uint64)
	if numOfNotDefaultIncarnations > 0 {
		for i := uint32(0); i < uint32(numOfNotDefaultIncarnations); i++ {
			notDefaultIncarnations[binary.BigEndian.Uint16(b[incarnatonsStart+i*10:incarnatonsStart+i*10+2])] = binary.BigEndian.Uint64(b[incarnatonsStart+i*10+2 : incarnatonsStart+i*10+10])
		}
	}

	keysStart := incarnatonsStart + uint32(numOfNotDefaultIncarnations)*10
	valsInfoStart := keysStart + numOfElements*common.HashLength

	var addressHashID uint16
	var id uint32
	for i := uint32(0); i < uint32(numOfUniqueElements); i++ {
		var (
			startKeys uint16
			endKeys   uint16
		)

		if i > 0 {
			startKeys = binary.BigEndian.Uint16(b[6+i*(common.HashLength)+(i-1)*2 : 6+i*(common.HashLength)+i*2])
		}
		endKeys = binary.BigEndian.Uint16(b[6+(i+1)*(common.HashLength)+i*2 : 6+(i+1)*(common.HashLength)+(i+1)*2])
		addrHash := b[6+i*(common.HashLength)+i*2 : 6+(i+1)*(common.HashLength)+i*2]
		incarnation := DefaultIncarnation
		if inc, ok := notDefaultIncarnations[addressHashID]; ok {
			incarnation = inc
		}

		for j := startKeys; j < endKeys; j++ {
			k := make([]byte, 72)
			copy(k[:common.HashLength], addrHash[:common.HashLength])
			copy(k[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength], b[keysStart+uint32(j)*common.HashLength:keysStart+(uint32(j)+1)*common.HashLength])
			binary.BigEndian.PutUint64(k[common.HashLength:common.HashLength+common.IncarnationLength], incarnation)
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
	if len(b) < 8 {
		return nil, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfElements := binary.BigEndian.Uint32(b[0:storageEnodingLengthOfNumOfElements])
	if numOfElements == 0 {
		return nil, nil
	}

	numOfUniqueElements := binary.BigEndian.Uint16(b[storageEnodingLengthOfNumOfElements : storageEnodingLengthOfNumOfElements+storageEnodingLengthOfUniqueElemens])
	incarnatonsInfo := 6 + uint32(numOfUniqueElements)*(common.HashLength) + uint32((numOfUniqueElements)*2)
	numOfNotDefaultIncarnations := binary.BigEndian.Uint16(b[incarnatonsInfo : incarnatonsInfo+2])
	incarnatonsStart := incarnatonsInfo + 2
	notDefaultIncarnations := make(map[uint16]uint64)
	if numOfNotDefaultIncarnations > 0 {
		for i := uint32(0); i < uint32(numOfNotDefaultIncarnations); i++ {
			notDefaultIncarnations[binary.BigEndian.Uint16(b[incarnatonsStart+i*10:incarnatonsStart+i*10+2])] = binary.BigEndian.Uint64(b[incarnatonsStart+i*10+2 : incarnatonsStart+i*10+10])
		}
	}
	keysStart := incarnatonsStart + uint32(numOfNotDefaultIncarnations)*10
	valsInfoStart := keysStart + numOfElements*common.HashLength

	addHashID := sort.Search(int(numOfUniqueElements), func(i int) bool {
		addrHash := b[6+i*(common.HashLength)+i*2 : 6+(i+1)*(common.HashLength)+i*2]
		cmp := bytes.Compare(addrHash, k[0:common.HashLength])
		return cmp >= 0
	})

	if addHashID >= int(numOfUniqueElements) {
		return nil, ErrNotFound
	}

	from := uint16(0)
	if addHashID > 0 {
		from = binary.BigEndian.Uint16(b[6+addHashID*common.HashLength+addHashID*2-2 : 6+(addHashID)*common.HashLength+addHashID*2])
	}
	to := binary.BigEndian.Uint16(b[6+(addHashID+1)*common.HashLength+addHashID*2 : 6+(addHashID+1)*common.HashLength+addHashID*2+2])

	keyIndex := sort.Search(int(to-from), func(i int) bool {
		index := uint32(int(from) + i)
		key := b[keysStart+common.HashLength*index : keysStart+common.HashLength*index+common.HashLength]
		cmp := bytes.Compare(key, k[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength])
		return cmp >= 0
	})
	if keyIndex >= int(to-from) {
		return nil, ErrNotFound
	}
	index := uint32(int(from) + keyIndex)
	return FindValue(b[valsInfoStart:], index)
}

func (b StorageChangeSetBytes) FindWithoutIncarnation(addrHashToFind []byte, keyHashToFind []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) < 8 {
		return nil, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfElements := binary.BigEndian.Uint32(b[0:storageEnodingLengthOfNumOfElements])
	if numOfElements == 0 {
		return nil, nil
	}

	numOfUniqueElements := binary.BigEndian.Uint16(b[storageEnodingLengthOfNumOfElements : storageEnodingLengthOfNumOfElements+storageEnodingLengthOfUniqueElemens])
	incarnatonsInfo := 6 + uint32(numOfUniqueElements)*(common.HashLength) + uint32((numOfUniqueElements)*2)
	numOfNotDefaultIncarnations := binary.BigEndian.Uint16(b[incarnatonsInfo : incarnatonsInfo+2])
	incarnatonsStart := incarnatonsInfo + 2
	notDefaultIncarnations := make(map[uint16]uint64)
	if numOfNotDefaultIncarnations > 0 {
		for i := uint32(0); i < uint32(numOfNotDefaultIncarnations); i++ {
			notDefaultIncarnations[binary.BigEndian.Uint16(b[incarnatonsStart+i*10:incarnatonsStart+i*10+2])] = binary.BigEndian.Uint64(b[incarnatonsStart+i*10+2 : incarnatonsStart+i*10+10])
		}
	}
	keysStart := incarnatonsStart + uint32(numOfNotDefaultIncarnations)*10
	valsInfoStart := keysStart + numOfElements*common.HashLength

	addHashID := sort.Search(int(numOfUniqueElements), func(i int) bool {
		addrHash := b[6+i*(common.HashLength)+i*2 : 6+(i+1)*(common.HashLength)+i*2]
		cmp := bytes.Compare(addrHash, addrHashToFind)
		return cmp >= 0

	})

	if addHashID >= int(numOfUniqueElements) {
		return nil, ErrNotFound
	}
	from := uint16(0)
	if addHashID > 0 {
		from = binary.BigEndian.Uint16(b[6+addHashID*common.HashLength+addHashID*2-2 : 6+(addHashID)*common.HashLength+addHashID*2])
	}
	to := binary.BigEndian.Uint16(b[6+(addHashID+1)*common.HashLength+addHashID*2 : 6+(addHashID+1)*common.HashLength+addHashID*2+2])
	keyIndex := sort.Search(int(to-from), func(i int) bool {
		index := uint32(int(from) + i)
		key := b[keysStart+common.HashLength*index : keysStart+common.HashLength*index+common.HashLength]
		cmp := bytes.Compare(key, keyHashToFind)
		return cmp >= 0
	})
	if keyIndex >= int(to-from) {
		return nil, ErrNotFound
	}
	index := uint32(int(from) + keyIndex)
	return FindValue(b[valsInfoStart:], index)
}

type contractKeys struct {
	AddrHash    []byte
	Incarnation uint64
	Keys        [][]byte
	Vals        [][]byte
}
