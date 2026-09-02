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

package node

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGzipHandlerLatency(t *testing.T) {
	for _, blk := range historicalBlocks {
		t.Run(blk.desc, func(t *testing.T) {
			payload := fetchPayload(t, blk.tag)
			if payload == nil {
				return
			}
			kp := measureHandlerLatency(t, payload, newGzipHandler)
			std := measureHandlerLatency(t, payload, newStdlibGzipHandler)
			t.Logf("klauspost  %s", kp)
			t.Logf("stdlib     %s", std)
			t.Logf("speedup p50=%.2fx  p99=%.2fx", float64(std.p50)/float64(kp.p50), float64(std.p99)/float64(kp.p99))
		})
	}
}

func TestRPCDaemonLatency(t *testing.T) {
	var sb strings.Builder

	for _, blk := range historicalBlocks {
		t.Run(blk.desc, func(t *testing.T) {
			stat := measureRPCLatency(t, rpcEndpoint, blk.tag)
			line := fmt.Sprintf("%-52s  %s\n", blk.desc, stat)
			t.Log(line)
			sb.WriteString(line)
		})
	}

	// Append results to file with a header so we can diff two runs.
	f, err := os.OpenFile(resultsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("warning: could not write results file: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "\n=== %s ===\n", time.Now().Format("2006-01-02 15:04:05"))
	f.WriteString(sb.String())
	t.Logf("results appended to %s", resultsFile)
}
