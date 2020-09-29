package dbutils

import (
	"bytes"

	"github.com/ledgerwatch/turbo-geth/ethdb/codecpool"
)

const KeyIDs = "ids"

// IDs - store id of last inserted entity to db - increment before use
type IDs struct {
	Topic uint32
}

const KeyAggregates = "aggregates"

// Aggregates - store some statistical aggregates of data: for example min/max of values in some bucket
type Aggregates struct {
}

func (c *IDs) Unmarshal(data []byte) error {
	decoder := codecpool.Decoder(bytes.NewReader(data))
	defer codecpool.Return(decoder)

	if err := decoder.Decode(c); err != nil {
		return err
	}
	return nil
}

func (c *IDs) Marshal() (data []byte, err error) {
	var buf bytes.Buffer
	encoder := codecpool.Encoder(&buf)
	defer codecpool.Return(encoder)
	if err := encoder.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Aggregates) Unmarshal(data []byte) error {
	decoder := codecpool.Decoder(bytes.NewReader(data))
	defer codecpool.Return(decoder)
	if err := decoder.Decode(c); err != nil {
		return err
	}
	return nil
}

func (c *Aggregates) Marshal() (data []byte, err error) {
	var buf bytes.Buffer
	encoder := codecpool.Encoder(&buf)
	defer codecpool.Return(encoder)
	if err := encoder.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
