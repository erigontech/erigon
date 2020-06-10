package changeset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
)

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

func findLastKeyInAccountChangeSet(b []byte, k []byte, keyLen int) ([]byte, error) {
	if len(b) == 0 {
		return nil, ErrNotFound
	}

	if len(b) < 8 {
		return nil, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	n := int(binary.BigEndian.Uint32(b[0:]))

	if n == 0 {
		return nil, ErrNotFound
	}

	valOffset := 4 + n*keyLen + 4*n
	if len(b) < valOffset {
		fmt.Println("FindLastAccounts account")
		return nil, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset)
	}

	totalValLength := int(binary.BigEndian.Uint32(b[valOffset-4:]))
	if len(b) < valOffset+totalValLength {
		return nil, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset+totalValLength)
	}

	for i := n - 1; int(i) >= 0; i-- {
		key := b[4+i*keyLen : 4+(i+1)*keyLen]
		idx0 := 0
		if i > 0 {
			idx0 = int(binary.BigEndian.Uint32(b[4+n*keyLen+4*(i-1):]))
		}
		idx1 := int(binary.BigEndian.Uint32(b[4+n*keyLen+4*i:]))
		val := b[valOffset+idx0 : valOffset+idx1]

		if bytes.Equal(key, k) {
			return val, nil
		}
	}
	return nil, ErrNotFound
}

func decodeAccountsWithKeyLen(b []byte, keyLen uint32, h *ChangeSet) error {
	h.Changes = make([]Change, 0)
	if len(b) == 0 {
		return nil
	}

	if len(b) < 4 {
		return fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfAccounts := binary.BigEndian.Uint32(b[0:4])

	if numOfAccounts == 0 {
		return nil
	}

	h.Changes = make([]Change, numOfAccounts)

	valOffset := 4 + numOfAccounts*keyLen + 4*numOfAccounts
	if uint32(len(b)) < valOffset {
		fmt.Println("DecodeAccounts account")
		return fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset)
	}

	totalValLength := binary.BigEndian.Uint32(b[valOffset-4 : valOffset])
	if uint32(len(b)) < valOffset+totalValLength {
		return fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), valOffset+totalValLength)
	}

	for i := uint32(0); i < numOfAccounts; i++ {
		key := b[4+i*keyLen : 4+(i+1)*keyLen]
		idx0 := uint32(0)
		if i > 0 {
			idx0 = binary.BigEndian.Uint32(b[4+numOfAccounts*keyLen+4*(i-1) : 4+numOfAccounts*keyLen+4*i])
		}
		idx1 := binary.BigEndian.Uint32(b[4+numOfAccounts*keyLen+4*i : 4+numOfAccounts*keyLen+4*(i+1)])
		val := b[valOffset+idx0 : valOffset+idx1]

		h.Changes[i].Key = common.CopyBytes(key)
		h.Changes[i].Value = common.CopyBytes(val)
	}

	sort.Sort(h)
	return nil
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
func encodeAccounts(s *ChangeSet) ([]byte, error) {
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
