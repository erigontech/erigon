package downloadernat

import (
	"github.com/anacrolix/torrent"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/log/v3"
)

// DoNat can mutate `cfg` parameter
func DoNat(natif nat.Interface, cfg *torrent.ClientConfig, logger log.Logger) {
	switch natif.(type) {
	case nil:
		// No NAT interface, do nothing.
	case nat.ExtIP:
		// ExtIP doesn't block, set the IP right away.
		ip, _ := natif.ExternalIP()
		if ip != nil {
			if ip.To4() != nil {
				cfg.PublicIp4 = ip
			} else {
				cfg.PublicIp6 = ip
			}
		}
		logger.Info("[torrent] Public IP", "ip", ip)

	default:
		// Ask the router about the IP. This takes a while and blocks startup,
		// do it in the background.
		if ip, err := natif.ExternalIP(); err == nil {
			if ip != nil {
				if ip.To4() != nil {
					cfg.PublicIp4 = ip
				} else {
					cfg.PublicIp6 = ip
				}
			}
			logger.Info("[torrent] Public IP", "ip", ip)
		}
	}
}
