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
	transactionsPayload []byte
	encodedUncles       []byte
	withdrawalsPayload  []byte
	hasWithdrawals      bool
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
	candidateIndex := 0
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

		// Stop early enough to leave one header for every later body.
		lastCandidateIndex := len(headers) - (bodyCount - bodyIndex)
		firstCandidateIndex := candidateIndex
		var firstMismatch error
		for candidateIndex <= lastCandidateIndex {
			mismatch := commitments.matchesHeader(headers[candidateIndex])
			if mismatch == nil {
				break
			}
			if firstMismatch == nil {
				firstMismatch = mismatch
			}
			candidateIndex++
		}
		if candidateIndex > lastCandidateIndex {
			firstMismatch = fmt.Errorf("body matches no remaining requested header: %w", firstMismatch)
			return nil, newBodyHeaderMismatch(headers[firstCandidateIndex], firstMismatch)
		}

		body := new(types.Body)
		if err := rlp.DecodeBytes(encodedBody, body); err != nil {
			return nil, fmt.Errorf("decode block body %d: %w", bodyIndex, err)
		}
		bodies[candidateIndex] = body
		candidateIndex++
	}
	return bodies, nil
}

func splitRawBlockBody(payload []byte) (rawBlockBody, error) {
	transactionsPayload, _, remaining, err := splitRawList(payload)
	if err != nil {
		return rawBlockBody{}, err
	}
	_, encodedUncles, remaining, err := splitRawList(remaining)
	if err != nil {
		return rawBlockBody{}, err
	}

	rawBody := rawBlockBody{
		transactionsPayload: transactionsPayload,
		encodedUncles:       encodedUncles,
	}
	if len(remaining) == 0 {
		return rawBody, nil
	}
	rawBody.withdrawalsPayload, _, remaining, err = splitRawList(remaining)
	if err != nil {
		return rawBlockBody{}, err
	}
	if len(remaining) != 0 {
		return rawBlockBody{}, rlp.ErrMoreThanOneValue
	}
	rawBody.hasWithdrawals = true
	return rawBody, nil
}

func splitRawList(encoded []byte) (payload, encodedList, rest []byte, err error) {
	payload, rest, err = rlp.SplitList(encoded)
	if err != nil {
		return nil, nil, encoded, err
	}
	return payload, encoded[:len(encoded)-len(rest)], rest, nil
}

func (b rawBlockBody) commitments() (blockBodyCommitments, error) {
	transactionHash, err := types.DeriveShaRawTransactions(b.transactionsPayload)
	if err != nil {
		return blockBodyCommitments{}, err
	}
	uncleHash := crypto.Keccak256Hash(b.encodedUncles)
	commitments := blockBodyCommitments{
		transactionHash: transactionHash,
		uncleHash:       uncleHash,
		hasWithdrawals:  b.hasWithdrawals,
	}
	if !b.hasWithdrawals {
		return commitments, nil
	}

	commitments.withdrawalsHash, err = types.DeriveShaRawValues(b.withdrawalsPayload)
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
