package changeset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"sort"
)

func NewAccountChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  common.HashLength,
	}
}

type AccountChangeSetBytes []byte

const accountKeySize = common.HashLength

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
func EncodeAccounts(s *ChangeSet) ([]byte, error) {
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

func (b AccountChangeSetBytes) Walk(f func(k, v []byte) error) error {
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
	valOffset := 4 + n*accountKeySize + 4*n
	if uint32(len(b)) < valOffset {
		fmt.Println("walkAccounts account")
		return fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset)
	}

	totalValLength := binary.BigEndian.Uint32(b[valOffset-4 : valOffset])
	if uint32(len(b)) < valOffset+totalValLength {
		return fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset+totalValLength)
	}

	for i := uint32(0); i < n; i++ {
		key := b[4+i*accountKeySize : 4+(i+1)*accountKeySize]
		idx0 := uint32(0)
		if i > 0 {
			idx0 = binary.BigEndian.Uint32(b[4+n*accountKeySize+4*(i-1) : 4+n*accountKeySize+4*i])
		}
		idx1 := binary.BigEndian.Uint32(b[4+n*accountKeySize+4*i : 4+n*accountKeySize+4*(i+1)])
		val := b[valOffset+idx0 : valOffset+idx1]

		err := f(key, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b AccountChangeSetBytes) FindLast(k []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}

	if len(b) < 8 {
		return nil, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	n := binary.BigEndian.Uint32(b[0:4])

	if n == 0 {
		return nil, nil
	}

	valOffset := 4 + n*accountKeySize + 4*n
	if uint32(len(b)) < valOffset {
		fmt.Println("FindLastAccounts account")
		return nil, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset)
	}

	totalValLength := binary.BigEndian.Uint32(b[valOffset-4 : valOffset])
	if uint32(len(b)) < valOffset+totalValLength {
		return nil, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset+totalValLength)
	}

	for i := n - 1; int(i) >= 0; i-- {
		key := b[4+i*accountKeySize : 4+(i+1)*accountKeySize]
		idx0 := uint32(0)
		if i > 0 {
			idx0 = binary.BigEndian.Uint32(b[4+n*accountKeySize+4*(i-1) : 4+n*accountKeySize+4*i])
		}
		idx1 := binary.BigEndian.Uint32(b[4+n*accountKeySize+4*i : 4+n*accountKeySize+4*(i+1)])
		val := b[valOffset+idx0 : valOffset+idx1]

		if bytes.Equal(key, k) {
			return val, nil
		}
	}
	return nil, errors.New("not found")
}

////////////////////////////////////////////////
func DecodeAccounts(b []byte) (*ChangeSet, error) {
	h := NewAccountChangeSet()
	h.Changes = make([]Change, 0)
	if len(b) == 0 {
		return h, nil
	}

	if len(b) < 4 {
		return h, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfAccounts := binary.BigEndian.Uint32(b[0:4])

	if numOfAccounts == 0 {
		return h, nil
	}

	h.Changes = make([]Change, numOfAccounts)

	valOffset := 4 + numOfAccounts*accountKeySize + 4*numOfAccounts
	if uint32(len(b)) < valOffset {
		fmt.Println("DecodeAccounts account")
		return h, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset)
	}

	totalValLength := binary.BigEndian.Uint32(b[valOffset-4 : valOffset])
	if uint32(len(b)) < valOffset+totalValLength {
		return h, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset+totalValLength)
	}

	for i := uint32(0); i < numOfAccounts; i++ {
		key := b[4+i*accountKeySize : 4+(i+1)*accountKeySize]
		idx0 := uint32(0)
		if i > 0 {
			idx0 = binary.BigEndian.Uint32(b[4+numOfAccounts*accountKeySize+4*(i-1) : 4+numOfAccounts*accountKeySize+4*i])
		}
		idx1 := binary.BigEndian.Uint32(b[4+numOfAccounts*accountKeySize+4*i : 4+numOfAccounts*accountKeySize+4*(i+1)])
		val := b[valOffset+idx0 : valOffset+idx1]

		h.Changes[i].Key = common.CopyBytes(key)
		h.Changes[i].Value = common.CopyBytes(val)
	}

	sort.Sort(h)
	return h, nil
}
