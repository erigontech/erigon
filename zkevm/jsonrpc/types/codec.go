package types

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/ledgerwatch/erigon/zkevm/encoding"
	"github.com/ledgerwatch/erigon/zkevm/hex"
)

const (
	// PendingBlockNumber represents the pending block number
	PendingBlockNumber = BlockNumber(-3)
	// LatestBlockNumber represents the latest block number
	LatestBlockNumber = BlockNumber(-2)
	// EarliestBlockNumber represents the earliest block number
	EarliestBlockNumber = BlockNumber(-1)

	// LatestBatchNumber represents the latest batch number
	LatestBatchNumber = BatchNumber(-2)
	// EarliestBatchNumber represents the earliest batch number
	EarliestBatchNumber = BatchNumber(-1)

	// Earliest contains the string to represent the earliest block known.
	Earliest = "earliest"
	// Latest contains the string to represent the latest block known.
	Latest = "latest"
	// Pending contains the string to represent pending blocks.
	Pending = "pending"

	// EIP-1898: https://eips.ethereum.org/EIPS/eip-1898 //

	// BlockNumberKey is the key for the block number for EIP-1898
	BlockNumberKey = "blockNumber"
	// BlockHashKey is the key for the block hash for EIP-1898
	BlockHashKey = "blockHash"
	// RequireCanonicalKey is the key for the require canonical for EIP-1898
	RequireCanonicalKey = "requireCanonical"
)

// Request is a jsonrpc request
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is a jsonrpc  success response
type Response struct {
	JSONRPC string
	ID      interface{}
	Result  json.RawMessage
	Error   *ErrorObject
}

// ErrorObject is a jsonrpc error
type ErrorObject struct {
	Code    int       `json:"code"`
	Message string    `json:"message"`
	Data    *ArgBytes `json:"data,omitempty"`
}

// NewResponse returns Success/Error response object
func NewResponse(req Request, reply []byte, err Error) Response {
	var result json.RawMessage
	if reply != nil {
		result = reply
	}

	var errorObj *ErrorObject
	if err != nil {
		errorObj = &ErrorObject{
			Code:    err.ErrorCode(),
			Message: err.Error(),
		}
		if err.ErrorData() != nil {
			errorObj.Data = ArgBytesPtr(*err.ErrorData())
		}
	}

	return Response{
		JSONRPC: req.JSONRPC,
		ID:      req.ID,
		Result:  result,
		Error:   errorObj,
	}
}

// MarshalJSON customizes the JSON representation of the response.
func (r Response) MarshalJSON() ([]byte, error) {
	if r.Error != nil {
		return json.Marshal(struct {
			JSONRPC string       `json:"jsonrpc"`
			ID      interface{}  `json:"id"`
			Error   *ErrorObject `json:"error"`
		}{
			JSONRPC: r.JSONRPC,
			ID:      r.ID,
			Error:   r.Error,
		})
	}

	return json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      interface{}     `json:"id"`
		Result  json.RawMessage `json:"result"`
	}{
		JSONRPC: r.JSONRPC,
		ID:      r.ID,
		Result:  r.Result,
	})
}

// Bytes return the serialized response
func (s Response) Bytes() ([]byte, error) {
	return json.Marshal(s)
}

// SubscriptionResponse used to push response for filters
// that have an active web socket connection
type SubscriptionResponse struct {
	JSONRPC string                     `json:"jsonrpc"`
	Method  string                     `json:"method"`
	Params  SubscriptionResponseParams `json:"params"`
}

// SubscriptionResponseParams parameters for subscription responses
type SubscriptionResponseParams struct {
	Subscription string          `json:"subscription"`
	Result       json.RawMessage `json:"result"`
}

// Bytes return the serialized response
func (s SubscriptionResponse) Bytes() ([]byte, error) {
	return json.Marshal(s)
}

// BlockNumber is the number of a ethereum block
type BlockNumber int64

// UnmarshalJSON automatically decodes the user input for the block number, when a JSON RPC method is called
func (b *BlockNumber) UnmarshalJSON(buffer []byte) error {
	num, err := StringToBlockNumber(string(buffer))
	if err != nil {
		return err
	}
	*b = num
	return nil
}

// GetNumericBlockNumber returns a numeric block number based on the BlockNumber instance
func (b *BlockNumber) GetNumericBlockNumber(ctx context.Context, s StateInterface, dbTx pgx.Tx) (uint64, Error) {
	bValue := LatestBlockNumber
	if b != nil {
		bValue = *b
	}

	switch bValue {
	case LatestBlockNumber, PendingBlockNumber:
		lastBlockNumber, err := s.GetLastL2BlockNumber(ctx, dbTx)
		if err != nil {
			return 0, NewRPCError(DefaultErrorCode, "failed to get the last block number from state")
		}

		return lastBlockNumber, nil

	case EarliestBlockNumber:
		return 0, nil

	default:
		if bValue < 0 {
			return 0, NewRPCError(InvalidParamsErrorCode, "invalid block number: %v", bValue)
		}
		return uint64(bValue), nil
	}
}

// StringOrHex returns the block number as a string or hex
// n == -3 = pending
// n == -2 = latest
// n == -1 = earliest
// n >=  0 = hex(n)
func (b *BlockNumber) StringOrHex() string {
	switch *b {
	case EarliestBlockNumber:
		return Earliest
	case PendingBlockNumber:
		return Pending
	case LatestBlockNumber:
		return Latest
	default:
		return hex.EncodeUint64(uint64(*b))
	}
}

// StringToBlockNumber converts a string like "latest" or "0x1" to a BlockNumber instance
func StringToBlockNumber(str string) (BlockNumber, error) {
	str = strings.Trim(str, "\"")
	switch str {
	case Earliest:
		return EarliestBlockNumber, nil
	case Pending:
		return PendingBlockNumber, nil
	case Latest, "":
		return LatestBlockNumber, nil
	}

	n, err := encoding.DecodeUint64orHex(&str)
	if err != nil {
		return 0, err
	}
	return BlockNumber(n), nil
}

// BlockNumberOrHash allows a string value to be parsed
// into a block number or a hash, it's used by methods
// like eth_call that allows the block to be specified
// either by the block number or the block hash
type BlockNumberOrHash struct {
	number           *BlockNumber
	hash             *ArgHash
	requireCanonical bool
}

// IsHash checks if the hash has value
func (b *BlockNumberOrHash) IsHash() bool {
	return b.hash != nil
}

// IsNumber checks if the number has value
func (b *BlockNumberOrHash) IsNumber() bool {
	return b.number != nil
}

// SetHash sets the hash and nullify the number
func (b *BlockNumberOrHash) SetHash(hash ArgHash, requireCanonical bool) {
	t := hash
	b.number = nil
	b.hash = &t
	b.requireCanonical = requireCanonical
}

// SetNumber sets the number and nullify the hash
func (b *BlockNumberOrHash) SetNumber(number BlockNumber) {
	t := number
	b.number = &t
	b.hash = nil
	b.requireCanonical = false
}

// Hash returns the hash
func (b *BlockNumberOrHash) Hash() *ArgHash {
	return b.hash
}

// Number returns the number
func (b *BlockNumberOrHash) Number() *BlockNumber {
	return b.number
}

// UnmarshalJSON automatically decodes the user input for the block number, when a JSON RPC method is called
func (b *BlockNumberOrHash) UnmarshalJSON(buffer []byte) error {
	var number BlockNumber
	err := json.Unmarshal(buffer, &number)
	if err == nil {
		b.SetNumber(number)
		return nil
	}

	var hash ArgHash
	err = json.Unmarshal(buffer, &hash)
	if err == nil {
		b.SetHash(hash, false)
		return nil
	}

	var m map[string]interface{}
	err = json.Unmarshal(buffer, &m)
	if err == nil {
		if v, ok := m[BlockNumberKey]; ok {
			input, _ := json.Marshal(v.(string))
			err := json.Unmarshal(input, &number)
			if err == nil {
				b.SetNumber(number)
				return nil
			}
		} else if v, ok := m[BlockHashKey]; ok {
			input, _ := json.Marshal(v.(string))
			err := json.Unmarshal(input, &hash)
			if err == nil {
				requireCanonical, ok := m[RequireCanonicalKey]
				if ok {
					switch v := requireCanonical.(type) {
					case bool:
						b.SetHash(hash, v)
					default:
						return fmt.Errorf("invalid requiredCanonical")
					}
				} else {
					b.SetHash(hash, false)
				}
				return nil
			}
		} else {
			return fmt.Errorf("invalid block or hash")
		}
	}

	return err
}

// Index of a item
type Index int64

// UnmarshalJSON automatically decodes the user input for the block number, when a JSON RPC method is called
func (i *Index) UnmarshalJSON(buffer []byte) error {
	str := strings.Trim(string(buffer), "\"")
	n, err := encoding.DecodeUint64orHex(&str)
	if err != nil {
		return err
	}
	*i = Index(n)
	return nil
}

// BatchNumber is the number of a ethereum block
type BatchNumber int64

// UnmarshalJSON automatically decodes the user input for the block number, when a JSON RPC method is called
func (b *BatchNumber) UnmarshalJSON(buffer []byte) error {
	num, err := stringToBatchNumber(string(buffer))
	if err != nil {
		return err
	}
	*b = num
	return nil
}

// GetNumericBatchNumber returns a numeric batch number based on the BatchNumber instance
func (b *BatchNumber) GetNumericBatchNumber(ctx context.Context, s StateInterface, dbTx pgx.Tx) (uint64, Error) {
	bValue := LatestBatchNumber
	if b != nil {
		bValue = *b
	}

	switch bValue {
	case LatestBatchNumber:
		lastBatchNumber, err := s.GetLastBatchNumber(ctx, dbTx)
		if err != nil {
			return 0, NewRPCError(DefaultErrorCode, "failed to get the last batch number from state")
		}

		return lastBatchNumber, nil

	case EarliestBatchNumber:
		return 0, nil

	default:
		if bValue < 0 {
			return 0, NewRPCError(InvalidParamsErrorCode, "invalid batch number: %v", bValue)
		}
		return uint64(bValue), nil
	}
}

func stringToBatchNumber(str string) (BatchNumber, error) {
	str = strings.Trim(str, "\"")
	switch str {
	case Earliest:
		return EarliestBatchNumber, nil
	case Latest, "":
		return LatestBatchNumber, nil
	}

	n, err := encoding.DecodeUint64orHex(&str)
	if err != nil {
		return 0, err
	}
	return BatchNumber(n), nil
}
