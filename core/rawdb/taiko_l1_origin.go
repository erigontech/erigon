package rawdb

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/rlp"
)

var (
	// Database key prefix for L2 block's L1Origin.
	l1OriginPrefix  = []byte("TKO:L1O")
	headL1OriginKey = []byte("TKO:LastL1O")
)

// l1OriginKey calculates the L1Origin key.
// l1OriginPrefix + l2HeaderHash -> l1OriginKey
func l1OriginKey(blockID *big.Int) []byte {
	data, _ := (*math.HexOrDecimal256)(blockID).MarshalText()
	return append(l1OriginPrefix, data...)
}

//go:generate go run github.com/fjl/gencodec -type L1Origin -field-override l1OriginMarshaling -out gen_taiko_l1_origin.go

// L1Origin represents a L1Origin of a L2 block.
type L1Origin struct {
	BlockID       *big.Int    `json:"blockID" gencodec:"required"`
	L2BlockHash   common.Hash `json:"l2BlockHash"`
	L1BlockHeight *big.Int    `json:"l1BlockHeight" rlp:"optional"`
	L1BlockHash   common.Hash `json:"l1BlockHash" rlp:"optional"`
}

type l1OriginMarshaling struct {
	BlockID       *math.HexOrDecimal256
	L1BlockHeight *math.HexOrDecimal256
}

// IsPreconfBlock returns true if the L1Origin is for a preconfirmation block.
func (l *L1Origin) IsPreconfBlock() bool {
	return l.L1BlockHeight == nil
}

// WriteL1Origin stores a L1Origin into the database.
func WriteL1Origin(db kv.Putter, blockID *big.Int, l1Origin *L1Origin) error {
	data, err := rlp.EncodeToBytes(l1Origin)
	if err != nil {
		return fmt.Errorf("failed to encode L1Origin %w", err)
	}
	if err := db.Put(kv.TaikoL1Origin, l1OriginKey(blockID), data); err != nil {
		return fmt.Errorf("failed to store L1Origin %w", err)
	}
	return nil
}

// ReadL1Origin retrieves the given L2 block's L1Origin from database.
func ReadL1Origin(db kv.Getter, blockID *big.Int) (*L1Origin, error) {
	data, err := db.GetOne(kv.TaikoL1Origin, l1OriginKey(blockID))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	l1Origin := new(L1Origin)
	if err := rlp.Decode(bytes.NewReader(data), l1Origin); err != nil {
		return nil, fmt.Errorf("invalid L1Origin RLP bytes: %w", err)
	}

	return l1Origin, nil
}

// WriteHeadL1Origin stores the given L1Origin as the last L1Origin.
func WriteHeadL1Origin(db kv.Putter, blockID *big.Int) error {
	data, _ := (*math.HexOrDecimal256)(blockID).MarshalText()
	if err := db.Put(kv.TaikoL1Origin, headL1OriginKey, data); err != nil {
		return fmt.Errorf("Failed to store head L1Origin %w", err)
	}
	return nil
}

// ReadHeadL1Origin retrieves the last L1Origin from database.
func ReadHeadL1Origin(db kv.Tx) (*big.Int, error) {
	data, err := db.GetOne(kv.TaikoL1Origin, headL1OriginKey)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	blockID := new(math.HexOrDecimal256)
	if err := blockID.UnmarshalText(data); err != nil {
		return nil, fmt.Errorf("invalid L1Origin unmarshal: %w", err)
	}

	return (*big.Int)(blockID), nil
}
