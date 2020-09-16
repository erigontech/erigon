package commands

import (
	"context"
	"strconv"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type NetAPI interface {
	Listening(_ context.Context) (bool, error)
	Version(_ context.Context) (string, error)
	PeerCount(_ context.Context) (hexutil.Uint, error)
}

type NetAPIImpl struct {
	ethBackend ethdb.Backend
}

// NewtNetAPIImpl returns NetAPIImplImpl instance
func NewNetAPIImpl(eth ethdb.Backend) *NetAPIImpl {
	return &NetAPIImpl{
		ethBackend: eth,
	}
}

// Listen implements RPC call for net_listening
// TODO(tjayrush) remove hard coded value
func (api *NetAPIImpl) Listening(_ context.Context) (bool, error) {
	return true, nil
}

// Version implements RPC call for net_version
func (api *NetAPIImpl) Version(_ context.Context) (string, error) {
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
