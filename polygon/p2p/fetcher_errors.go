package p2p

import (
	"errors"
	"fmt"
	"sort"

	"github.com/ledgerwatch/erigon/core/types"
)

var ErrEmptyBody = errors.New("empty body")

type ErrInvalidFetchHeadersRange struct {
	start uint64
	end   uint64
}

func (e ErrInvalidFetchHeadersRange) Error() string {
	return fmt.Sprintf("invalid fetch headers range: start=%d, end=%d", e.start, e.end)
}

func (e ErrInvalidFetchHeadersRange) Is(err error) bool {
	var errInvalidFetchHeadersRange *ErrInvalidFetchHeadersRange
	switch {
	case errors.As(err, &errInvalidFetchHeadersRange):
		return true
	default:
		return false
	}
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
	switch {
	case errors.As(err, &errIncompleteHeaders):
		return true
	default:
		return false
	}
}

func (e ErrIncompleteHeaders) LowestMissingBlockNum() uint64 {
	return e.start + e.received
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
	switch {
	case errors.As(err, &errTooManyHeaders):
		return true
	default:
		return false
	}
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
	switch {
	case errors.As(err, &errDisconnectedHeaders):
		return true
	default:
		return false
	}
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
	switch {
	case errors.As(err, &errTooManyBodies):
		return true
	default:
		return false
	}
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
	switch {
	case errors.As(err, &errMissingBodies):
		return true
	default:
		return false
	}
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
