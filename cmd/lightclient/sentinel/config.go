package sentinel

import (
	"github.com/ledgerwatch/erigon/p2p/discover"
)

type SentinelConfig struct {
	DiscoverConfig discover.Config
	IpAddr         string
	Port           int
}
