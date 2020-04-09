package changeset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
)

const (
	storageEnodingIndexSize                 = 4
	storageEnodingStartElem                 = uint32(4)
	storageEnodingLengthOfNumOfElements     = 4
	storageEnodingLengthOfDict              = 2
	storageEnodingLengthOfNumTypeOfElements = 2
)

var ErrNotFound = errors.New("not found")

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  2*common.HashLength + common.IncarnationLength,
	}
}

/*
Storage ChangeSet is serialized in the following manner in order to facilitate binary search:

numOfElements uint32
numOfUniqAddrHashes uint16
[addrHashes] []common.Hash
[idOfAddr:idOfKey] [uint8/uint16/uint32:common.Hash...] (depends on numOfUniqAddrHashes)
numOfUint8Values uint16
numOfUint16Values uint16
numOfUint32Values uint16
[len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint8Values-1})] []uint8
[len(valnumOfUint8Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint16Values-1})] []uint16
[len(valnumOfUint16Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint32Values-1})] []uint32
*/

func EncodeStorage(s *ChangeSet) ([]byte, error) {
	sort.Sort(s)
	buf := new(bytes.Buffer)
	uint32Arr := make([]byte, storageEnodingLengthOfNumOfElements)
	n := s.Len()

	//write numOfElements
	binary.BigEndian.PutUint32(uint32Arr, uint32(n))
	if _, err := buf.Write(uint32Arr); err != nil {
		return nil, err
	}

	addrHashesMap := make(map[common.Hash]uint32)
	addrHashList := make([]byte, 0)

	//collect information about unique addHashes and non default incarnations
	nextIDAddrHash := uint32(0)
	var addrHash common.Hash
	for i := 0; i < n; i++ {
		//copy addrHash
		copy(
			addrHash[:],
			s.Changes[i].Key[0:common.HashLength],
		)

		//fill addrHashesMap and addrHashList
		if _, ok := addrHashesMap[addrHash]; !ok {
			addrHashesMap[addrHash] = nextIDAddrHash
			nextIDAddrHash++
			addrHashList = append(addrHashList, addrHash[:]...)
		}
	}

	//write numOfUniqAddrHashes
	numOfUniqAddrHashes := make([]byte, storageEnodingLengthOfDict)
	binary.BigEndian.PutUint16(numOfUniqAddrHashes, uint16(len(addrHashesMap)))
	if _, err := buf.Write(numOfUniqAddrHashes); err != nil {
		return nil, err
	}

	//Write contiguous array of address hashes
	if _, err := buf.Write(addrHashList); err != nil {
		return nil, err
	}

	lenOfAddr := getNumOfBytesByLen(len(addrHashesMap))
	values := new(bytes.Buffer)
	lengthes := make([]byte, storageEnodingLengthOfNumTypeOfElements*3)
	numOfUint8 := uint16(0)
	numOfUint16 := uint16(0)
	numOfUint32 := uint16(0)

	keys := new(bytes.Buffer)
	lengthOfValues := uint32(0)
	row := make([]byte, lenOfAddr+common.HashLength)
	for i := 0; i < len(s.Changes); i++ {
		writeKeyRow(
			addrHashesMap[common.BytesToHash(s.Changes[i].Key[0:common.HashLength])],
			row[0:lenOfAddr],
		)
		copy(row[lenOfAddr:lenOfAddr+common.HashLength], common.CopyBytes(s.Changes[i].Key[common.IncarnationLength+common.HashLength:common.IncarnationLength+2*common.HashLength]))
		keys.Write(row)

		lengthOfValues += uint32(len(s.Changes[i].Value))
		switch {
		case lengthOfValues <= 255:
			numOfUint8++
			lengthes = append(lengthes, uint8(lengthOfValues))
		case lengthOfValues <= 65535:
			numOfUint16++
			uint16b := make([]byte, 2)
			binary.BigEndian.PutUint16(uint16b, uint16(lengthOfValues))
			lengthes = append(lengthes, uint16b...)
		default:
			numOfUint32++
			uint32b := make([]byte, 4)
			binary.BigEndian.PutUint32(uint32b, lengthOfValues)
			lengthes = append(lengthes, uint32b...)
		}
		values.Write(s.Changes[i].Value)
	}

	binary.BigEndian.PutUint16(lengthes[0:storageEnodingLengthOfNumTypeOfElements], numOfUint8)
	binary.BigEndian.PutUint16(lengthes[storageEnodingLengthOfNumTypeOfElements:2*storageEnodingLengthOfNumTypeOfElements], numOfUint16)
	binary.BigEndian.PutUint16(lengthes[2*storageEnodingLengthOfNumTypeOfElements:3*storageEnodingLengthOfNumTypeOfElements], numOfUint32)
	if _, err := buf.Write(keys.Bytes()); err != nil {
		return nil, err
	}

	if _, err := buf.Write(lengthes); err != nil {
		return nil, err
	}

	if _, err := buf.Write(values.Bytes()); err != nil {
		return nil, err
	}

	byt := buf.Bytes()
	return byt, nil
}

// Only to be used for testing
func decodeStorage(b []byte) (*ChangeSet, error) {
	h := NewStorageChangeSet()
	if len(b) == 0 {
		h.Changes = make([]Change, 0)
		return h, nil
	}
	if len(b) < 4 {
		return h, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	//numOfElements uint32
	numOfElements := binary.BigEndian.Uint32(b[0:storageEnodingLengthOfNumOfElements])
	h.Changes = make([]Change, numOfElements)

	if numOfElements == 0 {
		return h, nil
	}

	dictLen := binary.BigEndian.Uint16(b[storageEnodingLengthOfNumOfElements : storageEnodingLengthOfNumOfElements+storageEnodingLengthOfDict])
	addMap := make(map[uint32]common.Hash)
	for i := 0; i < int(dictLen); i++ {
		elemStart := storageEnodingLengthOfNumOfElements + storageEnodingLengthOfDict + i*common.HashLength
		addMap[uint32(i)] = common.BytesToHash(b[elemStart : elemStart+common.HashLength])
	}

	lenOfValsPos := storageEnodingStartElem +
		2 + uint32(dictLen)*common.HashLength +
		numOfElements*uint32(getNumOfBytesByLen(int(dictLen))+common.HashLength)

	numOfUint8 := int(binary.BigEndian.Uint16(b[lenOfValsPos : lenOfValsPos+2]))
	numOfUint16 := int(binary.BigEndian.Uint16(b[lenOfValsPos+2 : lenOfValsPos+4]))
	numOfUint32 := int(binary.BigEndian.Uint16(b[lenOfValsPos+4 : lenOfValsPos+6]))
	lenOfValsPos = lenOfValsPos + 3*storageEnodingLengthOfNumTypeOfElements
	valuesPos := lenOfValsPos + uint32(numOfUint8) + uint32(numOfUint16*2) + uint32(numOfUint32*4)

	elementStart := storageEnodingStartElem + storageEnodingLengthOfDict + uint32(dictLen)*common.HashLength
	key := make([]byte, common.HashLength*2+common.IncarnationLength)

	lenOfAddHash := uint32(getNumOfBytesByLen(len(addMap)))
	//lastValLen:=0
	for i := uint32(0); i < numOfElements; i++ {
		//copy addrHash
		elem := elementStart + i*(lenOfAddHash+common.HashLength)
		copy(
			key[0:common.HashLength],
			readFromMap(addMap, b[elem:elem+lenOfAddHash]).Bytes(),
		)
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			common.CopyBytes(b[elem+lenOfAddHash:elem+lenOfAddHash+common.HashLength]),
		)
		binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], ^uint64(1))
		h.Changes[i].Key = common.CopyBytes(key)
		h.Changes[i].Value = findVal(b[lenOfValsPos:valuesPos], b[valuesPos:], i, numOfUint8, numOfUint16, numOfUint32)
	}
	return h, nil
}

// no more than len(changeset)
func getNumOfBytesByLen(n int) int {
	switch {
	case n < 255:
		return 1
	case n < 65535:
		return 2
	case n < 4294967295:
		return 4
	default:
		return 8
	}
}

func findVal(lenOfVals []byte, values []byte, i uint32, numOfUint8, numOfUint16, numOfUint32 int) []byte {
	lenOfValStart := uint32(0)
	lenOfValEnd := uint32(0)
	switch {
	case i < uint32(numOfUint8):
		lenOfValEnd = uint32(lenOfVals[i])
		if i > 0 {
			lenOfValStart = uint32(lenOfVals[i-1])
		}
		return common.CopyBytes(values[lenOfValStart:lenOfValEnd])
	case i < uint32(numOfUint8)+uint32(numOfUint16):
		one := uint32(numOfUint8) + (i-uint32(numOfUint8))*2
		lenOfValEnd = uint32(binary.BigEndian.Uint16(lenOfVals[one : one+2]))
		if i-1 < uint32(numOfUint8) {
			lenOfValStart = uint32(lenOfVals[i-1])
		} else {
			one = uint32(numOfUint8) + (i-1-uint32(numOfUint8))*2
			lenOfValStart = uint32(binary.BigEndian.Uint16(lenOfVals[one : one+2]))
		}
		return common.CopyBytes(values[lenOfValStart:lenOfValEnd])
	case i < uint32(numOfUint8)+uint32(numOfUint16)+uint32(numOfUint32):
		fmt.Println(i, uint32(numOfUint8), numOfUint16, numOfUint32)
		one := uint32(numOfUint8) + uint32(numOfUint16)*2 + (i-uint32(numOfUint8)-uint32(numOfUint16))*4
		lenOfValEnd = binary.BigEndian.Uint32(lenOfVals[one : one+4])
		if i-1 < uint32(numOfUint8)+uint32(numOfUint16) {
			one = uint32(numOfUint8) + (i-1-uint32(numOfUint8))*2
			lenOfValStart = uint32(binary.BigEndian.Uint16(lenOfVals[one : one+2]))
		} else {
			one := uint32(numOfUint8) + uint32(numOfUint16)*2 + (i-1-uint32(numOfUint8)-uint32(numOfUint16))*4
			lenOfValStart = binary.BigEndian.Uint32(lenOfVals[one : one+4])
		}
		return common.CopyBytes(values[lenOfValStart:lenOfValEnd])
	default:
		panic("findval err")
	}
}

func writeKeyRow(id uint32, row []byte) {
	switch len(row) {
	case 1:
		row[0] = uint8(id)
	case 2:
		binary.BigEndian.PutUint16(row, uint16(id))
	case 4:
		binary.BigEndian.PutUint32(row, id)
	case 8:
		binary.BigEndian.PutUint64(row, uint64(id))
	default:
		panic("wrong size of row")
	}
}
func readFromMap(m map[uint32]common.Hash, row []byte) common.Hash {
	switch len(row) {
	case 1:
		return m[uint32(row[0])]
	case 2:
		return m[uint32(binary.BigEndian.Uint16(row))]
	case 4:
		return m[binary.BigEndian.Uint32(row)]
	case 8:
		return m[uint32(binary.BigEndian.Uint64(row))]
	default:
		panic("wrong")
	}
}

type StorageChangeSetBytes []byte

func (b StorageChangeSetBytes) Walk(f func(k, v []byte) error) error {
	if len(b) == 0 {
		return nil
	}
	if len(b) < 8 {
		return fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfItems := binary.BigEndian.Uint32(b[0:4])

	if numOfItems == 0 {
		return nil
	}

	numOfUniqueItems := binary.BigEndian.Uint16(b[storageEnodingLengthOfNumOfElements : storageEnodingLengthOfNumOfElements+storageEnodingLengthOfDict])
	lenOfValsPos := storageEnodingStartElem +
		storageEnodingLengthOfDict +
		uint32(numOfUniqueItems)*common.HashLength +
		numOfItems*uint32(getNumOfBytesByLen(int(numOfUniqueItems))+common.HashLength)

	numOfUint8 := int(binary.BigEndian.Uint16(b[lenOfValsPos : lenOfValsPos+storageEnodingLengthOfNumTypeOfElements]))
	numOfUint16 := int(binary.BigEndian.Uint16(b[lenOfValsPos+storageEnodingLengthOfNumTypeOfElements : lenOfValsPos+storageEnodingLengthOfNumTypeOfElements*2]))
	numOfUint32 := int(binary.BigEndian.Uint16(b[lenOfValsPos+storageEnodingLengthOfNumTypeOfElements*2 : lenOfValsPos+storageEnodingLengthOfNumTypeOfElements*3]))

	lenOfValsPos += storageEnodingLengthOfNumTypeOfElements * 3
	valuesPos := lenOfValsPos +
		uint32(numOfUint8) +
		uint32(numOfUint16*2) +
		uint32(numOfUint32*4)

	addrHashMap := make(map[uint32]common.Hash, numOfUniqueItems)
	for i := uint32(0); i < uint32(numOfUniqueItems); i++ {
		elemStart := storageEnodingStartElem + storageEnodingLengthOfDict + i*(common.HashLength)
		addrHashMap[i] = common.BytesToHash(b[elemStart : elemStart+common.HashLength])
	}

	key := make([]byte, common.HashLength*2+common.IncarnationLength)
	elemLength := uint32(getNumOfBytesByLen(int(numOfUniqueItems)))
	for i := uint32(0); i < numOfItems; i++ {
		elemStart := storageEnodingStartElem +
			storageEnodingLengthOfDict +
			uint32(numOfUniqueItems)*(common.HashLength) +
			i*(elemLength+common.HashLength)

		//copy addrHash
		copy(key[0:common.HashLength], readFromMap(addrHashMap, b[elemStart:elemStart+elemLength]).Bytes())
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			b[elemStart+elemLength:elemStart+elemLength+common.HashLength],
		)
		binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], ^uint64(1))
		err := f(common.CopyBytes(key), findVal(b[lenOfValsPos:valuesPos], b[valuesPos:], i, numOfUint8, numOfUint16, numOfUint32))
		if err != nil {
			return err
		}
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

	numOfItems := binary.BigEndian.Uint32(b[0:storageEnodingLengthOfNumOfElements])
	if numOfItems == 0 {
		return nil, nil
	}

	numOfUniqueItems := binary.BigEndian.Uint16(b[storageEnodingLengthOfNumOfElements : storageEnodingLengthOfNumOfElements+storageEnodingLengthOfDict])
	var addHashID uint32
	found := false

	var elemStart uint32
	//todo[boris] here should be binary search
	for i := uint32(0); i < uint32(numOfUniqueItems); i++ {
		elemStart = storageEnodingLengthOfNumOfElements + storageEnodingLengthOfDict + i*common.HashLength
		if bytes.Equal(k[0:common.HashLength], b[elemStart:elemStart+common.HashLength]) {
			found = true
			addHashID = i
			break
		}
	}
	if !found {
		return nil, ErrNotFound
	}

	lenOfValsPos := storageEnodingStartElem +
		storageEnodingLengthOfDict +
		uint32(numOfUniqueItems)*common.HashLength +
		numOfItems*uint32(getNumOfBytesByLen(int(numOfUniqueItems))+common.HashLength)

	numOfUint8 := int(binary.BigEndian.Uint16(b[lenOfValsPos : lenOfValsPos+storageEnodingLengthOfNumTypeOfElements]))
	numOfUint16 := int(binary.BigEndian.Uint16(b[lenOfValsPos+storageEnodingLengthOfNumTypeOfElements : lenOfValsPos+storageEnodingLengthOfNumTypeOfElements*2]))
	numOfUint32 := int(binary.BigEndian.Uint16(b[lenOfValsPos+storageEnodingLengthOfNumTypeOfElements*2 : lenOfValsPos+storageEnodingLengthOfNumTypeOfElements*3]))

	lenOfValsPos += storageEnodingLengthOfNumTypeOfElements * 3
	valuesPos := lenOfValsPos +
		uint32(numOfUint8) +
		uint32(numOfUint16*2) +
		uint32(numOfUint32*4)

	//here should be binary search too
	elemLength := uint32(getNumOfBytesByLen(int(numOfUniqueItems)))
	encodedAddHashID := make([]byte, elemLength)
	writeKeyRow(addHashID, encodedAddHashID)
	for i := uint32(0); i < numOfItems; i++ {
		elemStart := storageEnodingStartElem +
			storageEnodingLengthOfDict +
			uint32(numOfUniqueItems)*(common.HashLength) +
			i*(elemLength+common.HashLength)

		if !bytes.Equal(encodedAddHashID, b[elemStart:elemStart+elemLength]) {
			continue
		}

		if !bytes.Equal(k[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength], b[elemStart+elemLength:elemStart+elemLength+common.HashLength]) {
			continue
		}
		return findVal(b[lenOfValsPos:valuesPos], b[valuesPos:], i, numOfUint8, numOfUint16, numOfUint32), nil
	}

	return nil, ErrNotFound
}
