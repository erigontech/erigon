// Copyright 2024 The Erigon Authors
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
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
)

var ErrInvalidFetchBlocksAmount = errors.New("invalid fetch blocks amount")

type ErrInvalidFetchHeadersRange struct {
	start uint64
	end   uint64
}

func (e ErrInvalidFetchHeadersRange) Error() string {
	return fmt.Sprintf("invalid fetch headers range: start=%d, end=%d", e.start, e.end)
}

func (e ErrInvalidFetchHeadersRange) Is(err error) bool {
	var errInvalidFetchHeadersRange *ErrInvalidFetchHeadersRange
	return errors.As(err, &errInvalidFetchHeadersRange)
}

type ErrIncompleteHeaders struct {
	start     uint64
	requested uint64
	received  uint64
}

func (e ErrIncompleteHeaders) Error() string {
	return fmt.Sprintf(
		"incomplete fetch headers response: start=%d, requested=%d, received=%d",
		e.start, e.requested, e.received,
	)
}

func (e ErrIncompleteHeaders) Is(err error) bool {
	var errIncompleteHeaders *ErrIncompleteHeaders
	return errors.As(err, &errIncompleteHeaders)
}

func (e ErrIncompleteHeaders) LowestMissingBlockNum() uint64 {
	return e.start + e.received
}

type ErrMissingHeaderHash struct {
	requested common.Hash
}

func (e ErrMissingHeaderHash) Error() string {
	return fmt.Sprintf("missing header hash: requested=%s", e.requested)
}

func (e ErrMissingHeaderHash) Is(err error) bool {
	var errMissingHeaderHash *ErrMissingHeaderHash
	return errors.As(err, &errMissingHeaderHash)
}

type ErrUnexpectedHeaderHash struct {
	requested common.Hash
	received  common.Hash
}

func (e ErrUnexpectedHeaderHash) Error() string {
	return fmt.Sprintf("unexpected headers hash: requested=%s, received=%s", e.requested, e.received)
}

func (e ErrUnexpectedHeaderHash) Is(err error) bool {
	var errUnexpectedHeaderHash *ErrUnexpectedHeaderHash
	return errors.As(err, &errUnexpectedHeaderHash)
}

type ErrTooManyHeaders struct {
	requested int
	received  int
}

func (e ErrTooManyHeaders) Error() string {
	return fmt.Sprintf("too many headers in fetch headers response: requested=%d, received=%d", e.requested, e.received)
}

func (e ErrTooManyHeaders) Is(err error) bool {
	var errTooManyHeaders *ErrTooManyHeaders
	return errors.As(err, &errTooManyHeaders)
}

type ErrNonSequentialHeaderNumbers struct {
	current  uint64
	expected uint64
}

func (e ErrNonSequentialHeaderNumbers) Error() string {
	return fmt.Sprintf(
		"non sequential header numbers in fetch headers response: current=%d, expected=%d",
		e.current, e.expected,
	)
}

func (e ErrNonSequentialHeaderNumbers) Is(err error) bool {
	var errDisconnectedHeaders *ErrNonSequentialHeaderNumbers
	return errors.As(err, &errDisconnectedHeaders)
}

type ErrNonSequentialHeaderHashes struct {
	hash       common.Hash
	parentHash common.Hash
	prevHash   common.Hash
}

func (e ErrNonSequentialHeaderHashes) Error() string {
	return fmt.Sprintf(
		"non sequential header hashes in fetch headers response: hash=%s parentHash=%s, prevHash=%s",
		e.hash, e.parentHash, e.prevHash,
	)
}

func (e ErrNonSequentialHeaderHashes) Is(err error) bool {
	var errNonSequentialHeaderHashes *ErrNonSequentialHeaderHashes
	return errors.As(err, &errNonSequentialHeaderHashes)
}

type ErrTooManyBodies struct {
	requested int
	received  int
}

func (e ErrTooManyBodies) Error() string {
	return fmt.Sprintf("too many bodies in fetch bodies response: requested=%d, received=%d", e.requested, e.received)
}

func (e ErrTooManyBodies) Is(err error) bool {
	var errTooManyBodies *ErrTooManyBodies
	return errors.As(err, &errTooManyBodies)
}

func NewErrMissingBodies(headers []*types.Header) *ErrMissingBodies {
	return &ErrMissingBodies{
		headers: headers,
	}
}

type ErrMissingBodies struct {
	headers []*types.Header
}

func (e ErrMissingBodies) Error() string {
	lowest, exists := e.LowestMissingBlockNum()
	return fmt.Sprintf("missing bodies: lowest=%d, exists=%v, total=%d", lowest, exists, len(e.headers))
}

func (e ErrMissingBodies) LowestMissingBlockNum() (uint64, bool) {
	return lowestHeadersNum(e.headers)
}

func (e ErrMissingBodies) Is(err error) bool {
	var errMissingBodies *ErrMissingBodies
	return errors.As(err, &errMissingBodies)
}

func lowestHeadersNum(headers []*types.Header) (uint64, bool) {
	if len(headers) == 0 {
		return 0, false
	}

	sort.Slice(headers, func(i, j int) bool {
		return headers[i].Number.Uint64() < headers[j].Number.Uint64()
	})

	return headers[0].Number.Uint64(), true
}
