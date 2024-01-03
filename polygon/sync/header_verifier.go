package sync

import "github.com/ledgerwatch/erigon/core/types"

type HeaderVerifier func(statePoint *statePoint, headers []*types.Header) error
