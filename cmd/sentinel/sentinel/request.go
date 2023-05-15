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

	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/peers"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (s *Sentinel) SendRequestRaw(input []byte, topic string) (data []byte, isError bool, err error) {
	pid, err := s.RandomPeer(topic)
	if err != nil {
		return nil, false, err
	}
	s.peers.WithPeer(pid, func(peer *peers.Peer) {
		data, isError, err = communication.SendRequestRawToPeer(s.ctx, s.host, data, topic, pid)
		if err != nil {
			peer.Penalize()
		}
	})
	return data, isError, err
}

func (s *Sentinel) RandomPeer(topic string) (peer.ID, error) {
	var (
		pid peer.ID
		err error
	)
	pid, err = connectToRandomPeer(s, string(BeaconBlockTopic))
	if err != nil {
		return peer.ID(""), fmt.Errorf("failed to connect to a random peer err=%s", err)
	}
	return pid, nil
}
