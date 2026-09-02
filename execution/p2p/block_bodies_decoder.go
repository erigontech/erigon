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

type rawBlockBody struct {
	transactions   []byte
	uncles         []byte
	withdrawals    []byte
	hasWithdrawals bool
}

type blockBodyCommitments struct {
	transactionHash common.Hash
	uncleHash       common.Hash
	withdrawalsHash common.Hash
	hasWithdrawals  bool
}

func decodeBlockBodiesResponse(encodedBodies []byte, headers []*types.Header) ([]*types.Body, error) {
	bodyCount, err := rlp.CountValues(encodedBodies)
	if err != nil {
		return nil, fmt.Errorf("count block bodies: %w", err)
	}
	if bodyCount > len(headers) {
		return nil, &ErrTooManyBodies{requested: len(headers), received: bodyCount}
	}

	bodies := make([]*types.Body, len(headers))
	nextHeaderIndex := 0
	for bodyIndex := range bodyCount {
		bodyPayload, rest, err := rlp.SplitList(encodedBodies)
		if err != nil {
			return nil, fmt.Errorf("split block body %d: %w", bodyIndex, err)
		}
		encodedBody := encodedBodies[:len(encodedBodies)-len(rest)]
		encodedBodies = rest

		rawBody, err := splitRawBlockBody(bodyPayload)
		if err != nil {
			return nil, fmt.Errorf("split block body %d: %w", bodyIndex, err)
		}
		commitments, err := rawBody.commitments()
		if err != nil {
			return nil, fmt.Errorf("hash block body %d: %w", bodyIndex, err)
		}

		lastHeaderIndex := len(headers) - (bodyCount - bodyIndex)
		firstHeaderIndex := nextHeaderIndex
		var firstMismatch error
		for nextHeaderIndex <= lastHeaderIndex {
			mismatch := commitments.matchesHeader(headers[nextHeaderIndex])
			if mismatch == nil {
				break
			}
			if firstMismatch == nil {
				firstMismatch = mismatch
			}
			nextHeaderIndex++
		}
		if nextHeaderIndex > lastHeaderIndex {
			firstMismatch = fmt.Errorf("body matches no remaining requested header: %w", firstMismatch)
			return nil, newBodyHeaderMismatch(headers[firstHeaderIndex], firstMismatch)
		}

		body := new(types.Body)
		if err := rlp.DecodeBytes(encodedBody, body); err != nil {
			return nil, fmt.Errorf("decode block body %d: %w", bodyIndex, err)
		}
		bodies[nextHeaderIndex] = body
		nextHeaderIndex++
	}
	return bodies, nil
}

func splitRawBlockBody(body []byte) (rawBlockBody, error) {
	transactions, _, body, err := splitRawList(body)
	if err != nil {
		return rawBlockBody{}, err
	}
	_, uncles, body, err := splitRawList(body)
	if err != nil {
		return rawBlockBody{}, err
	}

	rawBody := rawBlockBody{transactions: transactions, uncles: uncles}
	if len(body) == 0 {
		return rawBody, nil
	}
	rawBody.withdrawals, _, body, err = splitRawList(body)
	if err != nil {
		return rawBlockBody{}, err
	}
	if len(body) != 0 {
		return rawBlockBody{}, rlp.ErrMoreThanOneValue
	}
	rawBody.hasWithdrawals = true
	return rawBody, nil
}

func splitRawList(encoded []byte) (content, raw, rest []byte, err error) {
	content, rest, err = rlp.SplitList(encoded)
	if err != nil {
		return nil, nil, encoded, err
	}
	return content, encoded[:len(encoded)-len(rest)], rest, nil
}

func (b rawBlockBody) commitments() (blockBodyCommitments, error) {
	transactionHash, err := types.DeriveShaRawTransactions(b.transactions)
	if err != nil {
		return blockBodyCommitments{}, err
	}
	uncleHash := crypto.Keccak256Hash(b.uncles)
	commitments := blockBodyCommitments{
		transactionHash: transactionHash,
		uncleHash:       uncleHash,
		hasWithdrawals:  b.hasWithdrawals,
	}
	if !b.hasWithdrawals {
		return commitments, nil
	}

	commitments.withdrawalsHash, err = types.DeriveShaRawValues(b.withdrawals)
	if err != nil {
		return blockBodyCommitments{}, err
	}
	return commitments, nil
}

func (b blockBodyCommitments) matchesHeader(header *types.Header) error {
	if b.transactionHash != header.TxHash {
		return fmt.Errorf("body has invalid transaction hash: have %x, exp: %x", b.transactionHash, header.TxHash)
	}
	if b.uncleHash != header.UncleHash {
		return fmt.Errorf("body has invalid uncle hash: have %x, exp: %x", b.uncleHash, header.UncleHash)
	}
	switch {
	case header.WithdrawalsHash == nil && b.hasWithdrawals:
		return errors.New("body has unexpected withdrawals")
	case header.WithdrawalsHash != nil && !b.hasWithdrawals:
		return errors.New("body is missing withdrawals")
	case header.WithdrawalsHash != nil && b.withdrawalsHash != *header.WithdrawalsHash:
		return fmt.Errorf("body has invalid withdrawals hash: have %x, exp: %x", b.withdrawalsHash, *header.WithdrawalsHash)
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
