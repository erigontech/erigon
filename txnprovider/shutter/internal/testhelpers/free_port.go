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
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	maxPort = 65535
	minPort = 1024
)

var (
	portMu  sync.Mutex
	portNum int64
)

// NextFreePort uses a global circular port counter to find the next free port.
// Note, it can be used by many goroutines at the same time.
func NextFreePort() (int, error) {
	portMu.Lock()
	defer portMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	attempts := -1
	for {
		attempts++

		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("could not find next free port: attempts %d: %w", attempts, ctx.Err())
		default: // continue
		}

		portNum = nextPortNum(portNum)
		listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.FormatInt(portNum, 10))
		if err != nil {
			continue
		}

		err = listener.Close()
		if err != nil {
			return 0, err
		}

		return int(portNum), nil
	}
}

func nextPortNum(port int64) int64 {
	if port == 0 { // init case
		// generate a random starting point to avoid clashes
		// if more than 1 "go test ./erigon " processes are run on the same machine at the same time
		// for simplicity, assume there are 60,000 ports and each go test process needs 2500 ports
		// 60,000/2500=24 buckets - randomly pick 1 of these buckets
		// note: this randomness is not needed for a single run of a "go test ./erigon" process
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		n := rnd.Intn(24)
		return minPort + int64(n)*2500
	} else if port == maxPort {
		return minPort
	} else {
		return port + 1
	}
}
