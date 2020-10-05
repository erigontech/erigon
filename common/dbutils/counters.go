package dbutils

import (
	"bytes"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb/cbor"
)

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

var countersWriter = bytes.NewBuffer(nil)

func (c *IDs) Unmarshal(data []byte) error {
	return cbor.Unmarshal(c, bytes.NewReader(data))
}

func (c *IDs) Marshal() ([]byte, error) {
	err := cbor.Marshal(countersWriter, c)
	return common.CopyBytes(countersWriter.Bytes()), err
}

func (c *Aggregates) Unmarshal(data []byte) error {
	return cbor.Unmarshal(c, bytes.NewReader(data))
}

func (c *Aggregates) Marshal() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := cbor.Marshal(buf, c)
	return buf.Bytes(), err
}
