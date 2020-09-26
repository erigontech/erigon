package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// NetAPI the interface for the net_ RPC commands
type NetAPI interface {
	Listening(_ context.Context) (bool, error)
	Version(_ context.Context) (string, error)
	PeerCount(_ context.Context) (hexutil.Uint, error)
}

// NetAPIImpl data structure to store things needed for net_ commands
type NetAPIImpl struct {
	ethBackend ethdb.Backend
}

// NewNetAPIImpl returns NetAPIImplImpl instance
func NewNetAPIImpl(eth ethdb.Backend) *NetAPIImpl {
	return &NetAPIImpl{
		ethBackend: eth,
	}
}

// Listening implements RPC call for net_listening
// TODO(tjayrush) remove hard coded value
func (api *NetAPIImpl) Listening(_ context.Context) (bool, error) {
	return true, nil
}

// Version implements RPC call for net_version
func (api *NetAPIImpl) Version(_ context.Context) (string, error) {
	if api.ethBackend == nil {
		// We're running in --chaindata mode or otherwise cannot get the backend
		return "", fmt.Errorf("net_version function is not available")
	}

	res, err := api.ethBackend.NetVersion()
	if err != nil {
		return "", err
	}

	return strconv.FormatUint(res, 10), nil
}

// PeerCount implements RPC call for net_peerCount
// TODO(tjayrush) remove hard coded value
func (api *NetAPIImpl) PeerCount(_ context.Context) (hexutil.Uint, error) {
	return hexutil.Uint(25), nil
}
