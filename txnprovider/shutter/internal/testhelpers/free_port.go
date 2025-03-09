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
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var consumedPorts = sync.Map{}

// ConsumeFreeTcpPort can be used by many goroutines at the same time.
// It uses port 0 and the OS to find a random free port. Note it opens the port
// so we have to close it. But closing a port makes it eligible for selection
// by the OS again. So we need to remember which ones have been already touched.
func ConsumeFreeTcpPort(t *testing.T) (int, func()) {
	var port int
	var done bool
	var iterations int
	for !done {
		func() {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() {
				err := listener.Close()
				require.NoError(t, err)
			}()

			port = listener.Addr().(*net.TCPAddr).Port
			_, ok := consumedPorts.Swap(port, struct{}{})
			done = !ok
		}()
		iterations++
		if iterations > 1024 {
			require.FailNow(t, "failed to find a free port", "after %d iterations", iterations)
		}
	}
	return port, func() { consumedPorts.Delete(port) }
}
