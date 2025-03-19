// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"fmt"
	"net"
	"sync"
)

var consumedPorts = sync.Map{}

// ConsumeFreeTcpPort can be used by many goroutines at the same time.
// It uses port 0 and the OS to find a random free port. Note it opens the port
// so we have to close it. But closing a port makes it eligible for selection
// by the OS again. So we need to remember which ones have been already touched.
func ConsumeFreeTcpPort() (int, func(), error) {
	var port int
	var done bool
	var iterations int
	for !done {
		err := func() (err error) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return err
			}
			defer func() {
				err = listener.Close()
			}()

			port = listener.Addr().(*net.TCPAddr).Port
			_, ok := consumedPorts.Swap(port, struct{}{})
			done = !ok
			return nil
		}()
		if err != nil {
			return 0, nil, err
		}
		iterations++
		if iterations > 1024 {
			return 0, nil, fmt.Errorf("failed to find a free port after %d iterations", iterations)
		}
	}
	return port, func() { consumedPorts.Delete(port) }, nil
}
