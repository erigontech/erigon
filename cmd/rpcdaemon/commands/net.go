package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type NetAPI interface {
	Version(ctx context.Context) uint64
}

type NetAPIImpl struct {
	ethBackend ethdb.Backend
}

// NwtNetAPIImpl returns NetAPIImplImpl instance
func NewNetAPIImpl(eth ethdb.Backend) *NetAPIImpl {
	return &NetAPIImpl{
		ethBackend: eth,
	}
}
