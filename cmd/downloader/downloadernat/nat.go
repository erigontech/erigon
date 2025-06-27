// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package downloadernat

import (
	"github.com/anacrolix/torrent"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/nat"
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
