package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type NetAPI interface {
	Version(ctx context.Context) (string, error)
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
