package p2p

import (
	"github.com/erigontech/erigon/p2p/discover"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
)

type P2PManager interface {
	Pubsub() *pubsub.PubSub
	Host() host.Host
	BandwidthCounter() *metrics.BandwidthCounter
	UDPv5Listener() *discover.UDPv5
	UpdateENRAttSubnets(subnetIndex int, on bool)
	UpdateENRSyncNets(subnetIndex int, on bool)
}
