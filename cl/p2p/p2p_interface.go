package p2p

import (
	"time"

	"github.com/erigontech/erigon/p2p/discover"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
)

//go:generate mockgen -destination=./mock_services/p2p_manager_mock.go -package=mock_services . P2PManager
type P2PManager interface {
	Pubsub() *pubsub.PubSub
	Host() host.Host
	SetDialPolicy(func(peer.ID) bool)
	BandwidthCounter() *metrics.BandwidthCounter
	UDPv5Listener() *discover.UDPv5
	SetENRAttSubnets(subnets []uint64)
	UpdateENRSyncNets(subnetIndex int, on bool)
	// MarkPeerUseful records that pid just gave us something: a first copy of a valid gossip message
	// or a non-empty answer to a data request.
	MarkPeerUseful(pid peer.ID)
	// PeerLastUseful returns when pid was last useful, counting its connection time as useful.
	PeerLastUseful(pid peer.ID) (time.Time, bool)
}
