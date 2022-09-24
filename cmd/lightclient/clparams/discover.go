package clparams

import (
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/erigon/p2p/enode"
)

func GetDefaultDiscoveryConfig(net NetworkType) (*discover.Config, error) {
	networkConfig := NetworkConfigFromNetworkType(net)
	bootnodes := networkConfig.BootNodes()
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	enodes := []*enode.Node{}
	for _, addr := range bootnodes {
		enode, err := enode.Parse(enode.ValidSchemes, addr)
		if err != nil {
			return nil, err
		}
		enodes = append(enodes, enode)
	}
	return &discover.Config{
		PrivateKey: privateKey,
		Bootnodes:  enodes,
	}, nil
}
