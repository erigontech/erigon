package dbutils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/ledgerwatch/turbo-geth/common"
)

const (
	ItemLen      = 3
	MaxChunkSize = 1000
)

func NewHistoryIndex() HistoryIndexBytes {
	return make(HistoryIndexBytes, 8)
}

func WrapHistoryIndex(b []byte) HistoryIndexBytes {
	index := HistoryIndexBytes(b)
	if len(index) == 0 {
		index = make(HistoryIndexBytes, 8)
	}
	return index
}

type HistoryIndexBytes []byte

func (hi HistoryIndexBytes) String() string {
	var buffer bytes.Buffer
	numbers, _, _ := hi.Decode()
	for _, n := range numbers {
		buffer.WriteString(fmt.Sprintf("%d ", n))
	}
	return buffer.String()
}

// decode is used for debugging and in tests
func (hi HistoryIndexBytes) Decode() ([]uint64, []bool, error) {
	if len(hi) < 8 {
		return nil, nil, fmt.Errorf("minimal length of index chunk is %d, got %d", 8, len(hi))
	}
	if (len(hi)-8)%ItemLen != 0 {
		return nil, nil, fmt.Errorf("length of index chunk should be 8 (mod %d), got %d", ItemLen, len(hi))
	}
	numElements := (len(hi) - 8) / 3
	minElement := binary.BigEndian.Uint64(hi[:8])
	numbers := make([]uint64, 0, numElements)
	sets := make([]bool, 0, numElements)
	for i := 8; i < len(hi); i += 3 {
		numbers = append(numbers, minElement+(uint64(hi[i]&0x7f)<<16)+(uint64(hi[i+1])<<8)+uint64(hi[i+2]))
		sets = append(sets, hi[i]&0x80 != 0)
	}
	return numbers, sets, nil
}

func (hi HistoryIndexBytes) Append(v uint64, emptyValue bool) HistoryIndexBytes {
	if len(hi) < 8 {
		panic(fmt.Errorf("minimal length of index chunk is %d, got %d", 8, len(hi)))
	}
	if (len(hi)-8)%ItemLen != 0 {
		panic(fmt.Errorf("length of index chunk should be 8 (mod %d), got %d", ItemLen, len(hi)))
	}
	numElements := (len(hi) - 8) / 3
	var minElement uint64
	if numElements == 0 {
		minElement = v
		binary.BigEndian.PutUint64(hi[:], minElement)
	} else {
		minElement = binary.BigEndian.Uint64(hi[:8])
		// last value
		lastIdx := len(hi) - 3
		lastElement := minElement + (uint64(hi[lastIdx]&0x7f) << 16) + (uint64(hi[lastIdx+1]) << 8) + uint64(hi[lastIdx+2])
		// Do not append if the value is less or equal to the last - to make operation idempotent
		if v <= lastElement {
			return hi
		}
	}
	if v > minElement+0x7fffff { // Maximum number representable in 23 bits
		panic(fmt.Errorf("item %d cannot be placed into the chunk with minElement %d", v, minElement))
	}
	v -= minElement
	if emptyValue {
		hi = append(hi, 0x80|byte(v>>16))
	} else {
		hi = append(hi, byte(v>>16))
	}
	hi = append(hi, byte(v>>8))
	hi = append(hi, byte(v))
	return hi
}

func (hi HistoryIndexBytes) Len() int {
	if len(hi) < 8 {
		panic(fmt.Errorf("minimal length of index chunk is %d, got %d", 8, len(hi)))
	}
	if (len(hi)-8)%ItemLen != 0 {
		panic(fmt.Errorf("length of index chunk should be 8 (mod %d), got %d", ItemLen, len(hi)))
	}
	return (len(hi) - 8) / 3
}

// Truncate all the timestamps that are strictly greater than the given bound
func (hi HistoryIndexBytes) TruncateGreater(lower uint64) HistoryIndexBytes {
	if len(hi) < 8 {
		panic(fmt.Errorf("minimal length of index chunk is %d, got %d", 8, len(hi)))
	}
	if (len(hi)-8)%ItemLen != 0 {
		panic(fmt.Errorf("length of index chunk should be 8 (mod %d), got %d", ItemLen, len(hi)))
	}
	numElements := (len(hi) - 8) / 3
	minElement := binary.BigEndian.Uint64(hi[:8])
	elements := hi[8:]
	// We are looking for the truncation point, i.e. the index of the first element which is strictly greater
	// than `lower`. Then, we will use that truncation point to shrink the slice
	truncationPoint := sort.Search(numElements, func(i int) bool {
		return lower < minElement+(uint64(elements[i*ItemLen]&0x7f)<<16)+(uint64(elements[i*ItemLen+1])<<8)+uint64(elements[i*ItemLen+2])
	})
	return hi[:8+truncationPoint*ItemLen] // We preserve minElement field and all elements prior to the truncation point
}

// Search looks for the element which is equal or greater of given timestamp
func (hi HistoryIndexBytes) Search(v uint64) (uint64, bool, bool) {
	if len(hi) < 8 {
		panic(fmt.Errorf("minimal length of index chunk is %d, got %d", 8, len(hi)))
	}
	if (len(hi)-8)%ItemLen != 0 {
		panic(fmt.Errorf("length of index chunk should be 8 (mod %d), got %d", ItemLen, len(hi)))
	}
	numElements := (len(hi) - 8) / 3
	minElement := binary.BigEndian.Uint64(hi[:8])
	elements := hi[8:]
	idx := sort.Search(numElements, func(i int) bool {
		return v <= minElement+(uint64(elements[i*ItemLen]&0x7f)<<16)+(uint64(elements[i*ItemLen+1])<<8)+uint64(elements[i*ItemLen+2])
	}) * ItemLen
	if idx == len(elements) {
		return 0, false, false
	}
	return minElement +
			(uint64(elements[idx]&0x7f) << 16) +
			(uint64(elements[idx+1]) << 8) +
			uint64(elements[idx+2]),
		(elements[idx] & 0x80) != 0, true
}

func (hi HistoryIndexBytes) Key(key []byte) ([]byte, error) {
	blockNum, ok := hi.LastElement()
	if !ok {
		return nil, errors.New("empty index")
	}
	return IndexChunkKey(key, blockNum), nil
}

func (hi HistoryIndexBytes) LastElement() (uint64, bool) {
	if len(hi) < 8 {
		panic(fmt.Errorf("minimal length of index chunk is %d, got %d", 8, len(hi)))
	}
	if (len(hi)-8)%ItemLen != 0 {
		panic(fmt.Errorf("length of index chunk should be 8 (mod %d), got %d", ItemLen, len(hi)))
	}
	numElements := (len(hi) - 8) / 3
	if numElements == 0 {
		return 0, false
	}
	minElement := binary.BigEndian.Uint64(hi[:8])
	idx := 8 + ItemLen*(numElements-1)
	return minElement + (uint64(hi[idx]&0x7f) << 16) + (uint64(hi[idx+1]) << 8) + uint64(hi[idx+2]), true
}

func CurrentChunkKey(key []byte) []byte {
	return IndexChunkKey(key, ^uint64(0))
}

//
func IndexChunkKey(key []byte, blockNumber uint64) []byte {
	var blockNumBytes []byte // make([]byte, len(key)+8)
	switch len(key) {
	//plain state, accounts
	case common.AddressLength:
		blockNumBytes = make([]byte, common.AddressLength+8)
		copy(blockNumBytes, key)
		binary.BigEndian.PutUint64(blockNumBytes[common.AddressLength:], blockNumber)
	//plain state storage
	case common.AddressLength + common.HashLength + common.IncarnationLength:
		//remove incarnation and add block number
		blockNumBytes = make([]byte, common.AddressLength+common.HashLength+8)
		copy(blockNumBytes, key[:common.AddressLength])
		copy(blockNumBytes[common.AddressLength:], key[common.AddressLength+common.IncarnationLength:])
		binary.BigEndian.PutUint64(blockNumBytes[common.AddressLength+common.HashLength:], blockNumber)
	default:
		panic("unexpected length " + strconv.Itoa(len(key)))
	}

	return blockNumBytes
}
func CompositeKeyWithoutIncarnation(key []byte) []byte {
	if len(key) == common.HashLength*2+common.IncarnationLength {
		kk := make([]byte, common.HashLength*2)
		copy(kk, key[:common.HashLength])
		copy(kk[common.HashLength:], key[common.HashLength+common.IncarnationLength:])
		return kk
	}
	if len(key) == common.AddressLength+common.HashLength+common.IncarnationLength {
		kk := make([]byte, common.AddressLength+common.HashLength)
		copy(kk, key[:common.AddressLength])
		copy(kk[common.AddressLength:], key[common.AddressLength+common.IncarnationLength:])
		return kk
	}
	return key
}

func CheckNewIndexChunk(b []byte, v uint64) bool {
	if len(b) < 8 {
		panic(fmt.Errorf("minimal length of index chunk is %d, got %d", 8, len(b)))
	}
	if (len(b)-8)%ItemLen != 0 {
		panic(fmt.Errorf("length of index chunk should be 8 (mod %d), got %d", ItemLen, len(b)))
	}
	numElements := (len(b) - 8) / 3
	if numElements == 0 {
		return false
	}
	minElement := binary.BigEndian.Uint64(b[:8])
	// last value
	lastIdx := len(b) - 3
	lastElement := minElement + (uint64(b[lastIdx]&0x7f) << 16) + (uint64(b[lastIdx+1]) << 8) + uint64(b[lastIdx+2])
	// Do not append if the value is less or equal to the last - to make operation idempotent
	if v <= lastElement {
		return false
	}
	return (numElements >= MaxChunkSize) || (v > minElement+0x7fffff)
}
