// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package filters

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/rpc"
)

// FilterCriteria represents a request to create a new filter.
// Same as bind.FilterQuery but with UnmarshalJSON() method.
type FilterCriteria bind.FilterQuery

type LogFilterOptions struct {
	LogCount          uint64 `json:"logCount,omitempty"`
	BlockCount        uint64 `json:"blockCount,omitempty"`
	IgnoreTopicsOrder bool   `json:"ignoreTopicsOrder,omitempty"`
}

func DefaultLogFilterOptions() LogFilterOptions {
	return LogFilterOptions{
		BlockCount: 1,
	}
}

// ReceiptsFilterCriteria defines criteria for transaction receipts subscription.
// If TransactionHashes is nil or empty, receipts for all transactions included in new blocks will be delivered.
// Otherwise, only receipts for the specified transactions will be delivered.
type ReceiptsFilterCriteria struct {
	TransactionHashes []common.Hash `json:"transactionHashes"`
}

// UnmarshalJSON sets *args fields with given data.
func (args *FilterCriteria) UnmarshalJSON(data []byte) error {
	type input struct {
		BlockHash *common.Hash     `json:"blockHash"`
		FromBlock *rpc.BlockNumber `json:"fromBlock"`
		ToBlock   *rpc.BlockNumber `json:"toBlock"`
		Addresses any              `json:"address"`
		Topics    []any            `json:"topics"`
	}

	var raw input
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.BlockHash != nil {
		if raw.FromBlock != nil || raw.ToBlock != nil {
			// BlockHash is mutually exclusive with FromBlock/ToBlock criteria
			return errors.New("cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other")
		}
		args.BlockHash = raw.BlockHash
	} else {
		if raw.FromBlock != nil {
			args.FromBlock = big.NewInt(raw.FromBlock.Int64())
		}

		if raw.ToBlock != nil {
			args.ToBlock = big.NewInt(raw.ToBlock.Int64())
		}
	}

	args.Addresses = []common.Address{}

	if raw.Addresses != nil {
		// raw.Address can contain a single address or an array of addresses
		switch rawAddr := raw.Addresses.(type) {
		case []any:
			for i, addr := range rawAddr {
				if strAddr, ok := addr.(string); ok {
					addr, err := decodeAddress(strAddr)
					if err != nil {
						return fmt.Errorf("invalid address at index %d: %w", i, err)
					}
					args.Addresses = append(args.Addresses, addr)
				} else {
					return fmt.Errorf("non-string address at index %d", i)
				}
			}
		case string:
			addr, err := decodeAddress(rawAddr)
			if err != nil {
				return fmt.Errorf("invalid address: %w", err)
			}
			args.Addresses = []common.Address{addr}
		default:
			return errors.New("invalid addresses in query")
		}
	}

	// topics is an array consisting of strings and/or arrays of strings.
	// JSON null values are converted to common.Hash{} and ignored by the filter manager.
	if len(raw.Topics) > 0 {
		args.Topics = make([][]common.Hash, len(raw.Topics))
		for i, t := range raw.Topics {
			switch topic := t.(type) {
			case nil:
				// ignore topic when matching logs

			case string:
				// match specific topic
				top, err := decodeTopic(topic)
				if err != nil {
					return err
				}
				args.Topics[i] = []common.Hash{top}

			case []any:
				// or case e.g. [null, "topic0", "topic1"]
				for _, rawTopic := range topic {
					if rawTopic == nil {
						// null component, match all
						args.Topics[i] = nil
						break
					}
					if topic, ok := rawTopic.(string); ok {
						parsed, err := decodeTopic(topic)
						if err != nil {
							return err
						}
						args.Topics[i] = append(args.Topics[i], parsed)
					} else {
						return errors.New("invalid topic(s)")
					}
				}
			default:
				return errors.New("invalid topic(s)")
			}
		}
	}

	return nil
}

func decodeAddress(s string) (common.Address, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != length.Addr {
		err = fmt.Errorf("hex has invalid length %d after decoding; expected %d for address", len(b), length.Addr)
	}
	return common.BytesToAddress(b), err
}

func decodeTopic(s string) (common.Hash, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != length.Hash {
		err = fmt.Errorf("hex has invalid length %d after decoding; expected %d for topic", len(b), length.Hash)
	}
	return common.BytesToHash(b), err
}
