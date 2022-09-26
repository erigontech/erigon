/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sentinel

import (
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	ssz "github.com/prysmaticlabs/fastssz"
)

var handlers map[protocol.ID]network.StreamHandler = map[protocol.ID]network.StreamHandler{
	protocol.ID("/eth2/beacon_chain/req/ping/1/ssz_snappy"): pingHandler,
}

func pingHandler(stream network.Stream) {
	pingBytes := make([]byte, 8)
	n, err := stream.Read(pingBytes)
	if err != nil {
		log.Warn("handler crashed", "err", err)
		return
	}
	if n != 8 {
		log.Warn("Invalid ping received")
		return
	}
	log.Info("[Lightclient] Received", "ping", ssz.UnmarshallUint64(pingBytes))
	// Send it back
	n, err = stream.Write(pingBytes)
	if err != nil {
		log.Warn("handler crashed", "err", err)
		return
	}
	if n != 8 {
		log.Warn("Could not send Ping")
	}
}

func (s *Sentinel) setupHandlers() {
	for id, handler := range handlers {
		s.host.SetStreamHandler(id, handler)
	}
}
