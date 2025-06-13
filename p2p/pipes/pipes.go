// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package pipes

import (
	"net"
)

// NetPipe wraps net.Pipe in a signature returning an error
func NetPipe() (net.Conn, net.Conn, error) {
	p1, p2 := net.Pipe()
	return p1, p2, nil
}

// TCPPipe creates an in process full duplex pipe based on a localhost TCP socket
func TCPPipe() (net.Conn, net.Conn, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	defer l.Close()

	var aconn net.Conn
	aerr := make(chan error, 1)
	go func() {
		var err error
		aconn, err = l.Accept()
		aerr <- err
	}()

	dconn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		<-aerr
		return nil, nil, err
	}
	if err := <-aerr; err != nil {
		dconn.Close()
		return nil, nil, err
	}
	return aconn, dconn, nil
}
