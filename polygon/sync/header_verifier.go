package sync

import "github.com/ledgerwatch/erigon/core/types"

type HeaderVerifier interface {
	Verify(statePoint *statePoint, headers []*types.Header) error
}
