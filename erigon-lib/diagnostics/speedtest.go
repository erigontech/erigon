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

package diagnostics

import (
	"context"
	"time"

	"github.com/showwin/speedtest-go/speedtest"
	"github.com/showwin/speedtest-go/speedtest/transport"
)

func (d *DiagnosticClient) setupSpeedtestDiagnostics(rootCtx context.Context) {
	go func() {
		if d.speedTest {
			d.networkSpeedMutex.Lock()
			d.networkSpeed = d.runSpeedTest(rootCtx)
			d.networkSpeedMutex.Unlock()
		}
	}()
}

var cacheServerList speedtest.Servers

func (d *DiagnosticClient) runSpeedTest(rootCtx context.Context) NetworkSpeedTestResult {
	var speedtestClient = speedtest.New()

	serverList, err := speedtestClient.FetchServers()
	// Ensure that the server list can rolled back to the previous cache.
	if err == nil {
		cacheServerList = serverList
	}
	targets, _ := cacheServerList.FindServer([]int{})

	latency := time.Duration(0)
	downloadSpeed := float64(0)
	uploadSpeed := float64(0)
	packetLoss := float64(-1)

	analyzer := speedtest.NewPacketLossAnalyzer(nil)

	if len(targets) > 0 {
		s := targets[0]
		err = s.PingTestContext(rootCtx, nil)
		if err == nil {
			latency = s.Latency
		}

		err = s.DownloadTestContext(rootCtx)
		if err == nil {
			downloadSpeed = s.DLSpeed.Mbps()
		}

		err = s.UploadTestContext(rootCtx)
		if err == nil {
			uploadSpeed = s.ULSpeed.Mbps()
		}

		ctx, cancel := context.WithTimeout(rootCtx, time.Second*15)

		defer cancel()
		_ = analyzer.RunWithContext(ctx, s.Host, func(pl *transport.PLoss) {
			packetLoss = pl.Loss()
		})
	}

	return NetworkSpeedTestResult{
		Latency:       latency,
		DownloadSpeed: downloadSpeed,
		UploadSpeed:   uploadSpeed,
		PacketLoss:    packetLoss,
	}
}

func (d *DiagnosticClient) GetNetworkSpeed() NetworkSpeedTestResult {
	return d.networkSpeed
}
