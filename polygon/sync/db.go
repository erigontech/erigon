package sync

import "github.com/ledgerwatch/erigon/core/types"

//go:generate mockgen -destination=./mock/db_mock.go -package=mock . DB
type DB interface {
	WriteHeaders(headers []*types.Header) error
}
