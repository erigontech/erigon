package bridge

import (
	"bytes"
	"encoding/binary"
)

type IDRange struct {
	start, end uint64
}

// ToBytes converts IDRange to []byte
func (r IDRange) ToBytes() ([]byte, error) {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, r)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// IDRangeFromBytes converts []byte to IDRange
func IDRangeFromBytes(data []byte) (IDRange, error) {
	var r IDRange
	err := binary.Read(bytes.NewReader(data), binary.BigEndian, &r)
	if err != nil {
		return IDRange{}, err
	}
	return r, nil
}
