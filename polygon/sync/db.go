package sync

import "github.com/ledgerwatch/erigon/core/types"

//go:generate mockgen -destination=./db_mock.go -package=sync . DB
type DB interface {
	WriteHeaders(headers []*types.Header) error
}
