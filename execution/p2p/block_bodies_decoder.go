// Copyright 2026 The Erigon Authors
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

package p2p

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func decodeBlockBodiesResponse(encodedBodies []byte, headers []*types.Header) ([]*types.Body, error) {
	bodies := make([]*types.Body, 0, len(headers))
	for bodyIndex := 0; len(encodedBodies) > 0; bodyIndex++ {
		if bodyIndex == len(headers) {
			return nil, &ErrTooManyBodies{requested: len(headers), received: bodyIndex + 1}
		}

		bodyPayload, rest, err := rlp.SplitList(encodedBodies)
		if err != nil {
			return nil, fmt.Errorf("split block body %d: %w", bodyIndex, err)
		}
		encodedBody := encodedBodies[:len(encodedBodies)-len(rest)]
		encodedBodies = rest

		if err := validateRawBlockBody(bodyPayload, headers[bodyIndex], bodyIndex); err != nil {
			return nil, err
		}

		body := new(types.Body)
		if err := rlp.DecodeBytes(encodedBody, body); err != nil {
			return nil, fmt.Errorf("decode block body %d: %w", bodyIndex, err)
		}
		bodies = append(bodies, body)
	}
	return bodies, nil
}

func validateRawBlockBody(payload []byte, header *types.Header, bodyIndex int) error {
	transactionsPayload, remaining, err := rlp.SplitList(payload)
	if err != nil {
		return fmt.Errorf("split block body %d: %w", bodyIndex, err)
	}
	encodedUncles := remaining
	_, remaining, err = rlp.SplitList(remaining)
	if err != nil {
		return fmt.Errorf("split block body %d: %w", bodyIndex, err)
	}
	encodedUncles = encodedUncles[:len(encodedUncles)-len(remaining)]

	hasWithdrawals := len(remaining) != 0
	var withdrawalsPayload []byte
	if hasWithdrawals {
		withdrawalsPayload, remaining, err = rlp.SplitList(remaining)
		if err != nil {
			return fmt.Errorf("split block body %d: %w", bodyIndex, err)
		}
		if len(remaining) != 0 {
			return fmt.Errorf("split block body %d: %w", bodyIndex, rlp.ErrMoreThanOneValue)
		}
	}

	transactionHash, err := types.DeriveShaRawTransactions(transactionsPayload)
	if err != nil {
		return fmt.Errorf("hash block body %d: %w", bodyIndex, err)
	}
	uncleHash := crypto.Keccak256Hash(encodedUncles)
	var withdrawalsHash common.Hash
	if hasWithdrawals {
		withdrawalsHash, err = types.DeriveShaRawValues(withdrawalsPayload)
		if err != nil {
			return fmt.Errorf("hash block body %d: %w", bodyIndex, err)
		}
	}

	switch {
	case transactionHash != header.TxHash:
		return newBodyHeaderMismatch(header, fmt.Errorf("body has invalid transaction hash: have %x, exp: %x", transactionHash, header.TxHash))
	case uncleHash != header.UncleHash:
		return newBodyHeaderMismatch(header, fmt.Errorf("body has invalid uncle hash: have %x, exp: %x", uncleHash, header.UncleHash))
	case header.WithdrawalsHash == nil && hasWithdrawals:
		return newBodyHeaderMismatch(header, errors.New("body has unexpected withdrawals"))
	case header.WithdrawalsHash != nil && !hasWithdrawals:
		return newBodyHeaderMismatch(header, errors.New("body is missing withdrawals"))
	case header.WithdrawalsHash != nil && withdrawalsHash != *header.WithdrawalsHash:
		return newBodyHeaderMismatch(header, fmt.Errorf("body has invalid withdrawals hash: have %x, exp: %x", withdrawalsHash, *header.WithdrawalsHash))
	}
	return nil
}

func newBodyHeaderMismatch(header *types.Header, err error) *ErrBodyDoesNotMatchHeader {
	return &ErrBodyDoesNotMatchHeader{
		blockNum:  header.Number.Uint64(),
		blockHash: header.Hash(),
		err:       err,
	}
}
