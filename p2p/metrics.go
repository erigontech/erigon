// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Contains the meters and timers used by the networking layer.

package p2p

import (
	"github.com/ledgerwatch/erigon-lib/metrics"
	"net"
)

const (
	ingressMeterName = "p2p_ingress"
	egressMeterName  = "p2p_egress"
)

var (
	ingressConnectMeter = metrics.GetOrCreateCounter("p2p_serves")
	ingressTrafficMeter = metrics.GetOrCreateCounter(ingressMeterName)
	egressConnectMeter  = metrics.GetOrCreateCounter("p2p_dials")
	egressTrafficMeter  = metrics.GetOrCreateCounter(egressMeterName)
	activePeerGauge     = metrics.GetOrCreateCounter("p2p_peers", true)
)

// meteredConn is a wrapper around a net.Conn that meters both the
// inbound and outbound network traffic.
type meteredConn struct {
	net.Conn
}

// newMeteredConn creates a new metered connection, bumps the ingress or egress
// connection meter and also increases the metered peer count. If the metrics
// system is disabled, function returns the original connection.
func newMeteredConn(conn net.Conn, ingress bool, addr *net.TCPAddr) net.Conn {
	// Bump the connection counters and wrap the connection
	if ingress {
		ingressConnectMeter.Inc()
	} else {
		egressConnectMeter.Inc()
	}
	activePeerGauge.Inc()
	return &meteredConn{Conn: conn}
}

// Read delegates a network read to the underlying connection, bumping the common
// and the peer ingress traffic meters along the way.
func (c *meteredConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	ingressTrafficMeter.Add(n)
	return n, err
}

// Write delegates a network write to the underlying connection, bumping the common
// and the peer egress traffic meters along the way.
func (c *meteredConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	egressTrafficMeter.Add(n)
	return n, err
}

// Close delegates a close operation to the underlying connection, unregisters
// the peer from the traffic registries and emits close event.
func (c *meteredConn) Close() error {
	err := c.Conn.Close()
	if err == nil {
		activePeerGauge.Dec()
	}
	return err
}
