package bridge

import (
	"bytes"
	"encoding/binary"
)

type IDRange struct {
	Start, End uint64
}

// MarshalBytes converts IDRange to []byte
func (r *IDRange) MarshalBytes() ([]byte, error) {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, r)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// UnmarshalBytes converts []byte to IDRange
func (r *IDRange) UnmarshalBytes(data []byte) error {
	err := binary.Read(bytes.NewReader(data), binary.BigEndian, r)
	if err != nil {
		return err
	}
	return nil
}
