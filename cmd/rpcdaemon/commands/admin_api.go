package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/p2p"
)

// AdminAPI the interface for the admin_* RPC commands.
type AdminAPI interface {
	// NodeInfo returns a collection of metadata known about the host.
	NodeInfo(ctx context.Context) (*p2p.NodeInfo, error)

	// Peers returns information about the connected remote nodes.
	// https://geth.ethereum.org/docs/rpc/ns-admin#admin_peers
	Peers(ctx context.Context) ([]*p2p.PeerInfo, error)
}

// AdminAPIImpl data structure to store things needed for admin_* commands.
type AdminAPIImpl struct {
	ethBackend services.ApiBackend
}

// NewAdminAPI returns AdminAPIImpl instance.
func NewAdminAPI(eth services.ApiBackend) *AdminAPIImpl {
	return &AdminAPIImpl{
		ethBackend: eth,
	}
}

func (api *AdminAPIImpl) NodeInfo(ctx context.Context) (*p2p.NodeInfo, error) {
	nodes, err := api.ethBackend.NodeInfo(ctx, 1)
	if err != nil {
		return nil, fmt.Errorf("node info request error: %w", err)
	}

	if len(nodes) == 0 {
		return nil, errors.New("empty nodesInfo response")
	}

	return &nodes[0], nil
}

func (api *AdminAPIImpl) Peers(ctx context.Context) ([]*p2p.PeerInfo, error) {
	return api.ethBackend.Peers(ctx)
}
