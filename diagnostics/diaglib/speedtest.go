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

package diaglib

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/speedtest/speedtest"
)

var cloudflareHeaders = http.Header{
	"lsjdjwcush6jbnjj3jnjscoscisoc5s": []string{"I%OSJDNFKE783DDHHJD873EFSIVNI7384R78SSJBJBCCJBC32JABBJCBJK45"},
}

func (d *DiagnosticClient) setupSpeedtestDiagnostics(rootCtx context.Context) {
	go func() {
		if d.speedTest {
			d.networkSpeedMutex.Lock()
			defer d.networkSpeedMutex.Unlock()
			d.networkSpeed = d.runSpeedTest(rootCtx)
		}
	}()
}

func (d *DiagnosticClient) runSpeedTest(rootCtx context.Context) NetworkSpeedTestResult {
	result := NetworkSpeedTestResult{
		Latency:       time.Duration(0),
		DownloadSpeed: float64(0),
		UploadSpeed:   float64(0),
		PacketLoss:    float64(-1),
	}

	urlstr, err := speedtest.SelectSegmentFromWebseeds(d.webseedsList, cloudflareHeaders)
	if err != nil {
		log.Debug("[diagnostics] runSpeedTest", "err", err)
		return result
	}

	s, err := speedtest.CustomServer(urlstr)
	if err != nil {
		log.Debug("[diagnostics] runSpeedTest", "err", err)
		return result
	}

	err = s.PingTestContext(rootCtx, nil)
	if err == nil {
		result.Latency = s.Latency
	}

	err = s.DownloadTestContext(rootCtx)
	if err == nil {
		result.DownloadSpeed = s.DLSpeed.Mbps()
	}

	return result
}

func (d *DiagnosticClient) NetworkSpeedJson(w io.Writer) {
	d.networkSpeedMutex.Lock()
	defer d.networkSpeedMutex.Unlock()
	if err := json.NewEncoder(w).Encode(d.networkSpeed); err != nil {
		log.Debug("[diagnostics] ResourcesUsageJson", "err", err)
	}
}
