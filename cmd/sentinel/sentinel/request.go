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
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (s *Sentinel) SendRequestRaw(data []byte, topic string) ([]byte, bool, error) {
	var (
		peerInfo *peer.AddrInfo
		err      error
	)
	if strings.Contains(topic, "light_client") && !strings.Contains(topic, "bootstrap") {
		peerInfo, err = connectToRandomPeer(s, string(LightClientFinalityUpdateTopic))
	} else {
		peerInfo, err = connectToRandomPeer(s, string(BeaconBlockTopic))
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to connect to a random peer err=%s", err)
	}
	s.peers.PeerDoRequest(peerInfo.ID)
	defer s.peers.PeerFinishRequest(peerInfo.ID)
	data, isError, err := communication.SendRequestRawToPeer(s.ctx, s.host, data, topic, peerInfo.ID)
	if err != nil || isError {
		s.peers.Penalize(peerInfo.ID)
	} else {
		s.peers.Forgive(peerInfo.ID)
	}
	return data, isError, err
}
