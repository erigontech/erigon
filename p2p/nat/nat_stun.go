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

package nat

import (
	"fmt"
	"net"
	"time"

	"github.com/pion/stun"
)

const STUNDefaultServerAddr = "stun.l.google.com:19302"

type STUN struct {
	serverAddr string
}

func NewSTUN(serverAddr string) STUN {
	if serverAddr == "" {
		serverAddr = STUNDefaultServerAddr
	}
	return STUN{serverAddr: serverAddr}
}

func (s STUN) String() string {
	return fmt.Sprintf("STUN(%s)", s.serverAddr)
}

func (STUN) SupportsMapping() bool {
	return false
}

func (STUN) AddMapping(string, int, int, string, time.Duration) error {
	return nil
}

func (STUN) DeleteMapping(string, int, int) error {
	return nil
}

func (s STUN) ExternalIP() (net.IP, error) {
	conn, err := stun.Dial("udp4", s.serverAddr)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	message := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	var response *stun.Event
	err = conn.Do(message, func(event stun.Event) {
		response = &event
	})
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, response.Error
	}

	var mappedAddr stun.XORMappedAddress
	if err := mappedAddr.GetFrom(response.Message); err != nil {
		return nil, err
	}

	return mappedAddr.IP, nil
}
