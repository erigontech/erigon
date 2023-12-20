package sync

import "github.com/ledgerwatch/erigon/core/types"

type DB interface {
	WriteHeaders([]*types.Header) error
}
