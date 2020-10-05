package dbutils

import "github.com/ledgerwatch/turbo-geth/ethdb/cbor"

//go:generate codecgen -o counters_codecgen_gen.go -r "IDs|Aggregates" -rt "codec" -nx=1 -d=1 counters.go

// This file helps to manage AutoIncrements and other Counters

const (
	KeyIDs        = "ids"
	KeyAggregates = "aggregates"
)

// IDs - store id of last inserted entity to db - increment before use
type IDs struct {
	Example uint64 `codec:"1"`
}

// Aggregates - store some statistical aggregates of data: for example min/max of values in some bucket
type Aggregates struct {
	ExampleAvg uint64 `codec:"1"`
}

func (c *IDs) Unmarshal(data []byte) error {
	return cbor.Unmarshal(c, data)
}

func (c *IDs) Marshal() (data []byte, err error) {
	data = []byte{}
	err = cbor.Marshal(&data, c)
	return data, err
}

func (c *Aggregates) Unmarshal(data []byte) error {
	return cbor.Unmarshal(c, data)
}

func (c *Aggregates) Marshal() (data []byte, err error) {
	data = []byte{}
	err = cbor.Marshal(&data, c)
	return data, err
}
