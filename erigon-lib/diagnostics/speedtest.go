package diagnostics

import (
	"context"
	"time"

	"github.com/showwin/speedtest-go/speedtest"
	"github.com/showwin/speedtest-go/speedtest/transport"
)

func (d *DiagnosticClient) setupSpeedtestDiagnostics(rootCtx context.Context) {
	ticker := time.NewTicker(30 * time.Minute)
	go func() {
		d.networkSpeedMutex.Lock()
		d.networkSpeed = d.runSpeedTest(rootCtx)
		d.networkSpeedMutex.Unlock()

		for {
			select {
			case <-ticker.C:
				d.networkSpeedMutex.Lock()
				d.networkSpeed = d.runSpeedTest(rootCtx)
				d.networkSpeedMutex.Unlock()
			case <-rootCtx.Done():
				ticker.Stop()
				return
			}
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

	analyzer, _ := speedtest.NewPacketLossAnalyzer(nil)

	if len(targets) > 0 {
		s := targets[0]
		err = s.PingTestContext(rootCtx, nil)
		if err == nil {
			latency = s.Latency
		}

		err = s.DownloadTestContext(rootCtx)
		if err == nil {
			downloadSpeed = float64(s.DLSpeed) / 125000
		}

		err = s.UploadTestContext(rootCtx)
		if err == nil {
			uploadSpeed = float64(s.ULSpeed) / 125000
		}

		if analyzer != nil {
			ctx, cancel := context.WithTimeout(rootCtx, time.Second*15)
			defer cancel()
			_ = analyzer.RunWithContext(ctx, s.Host, func(pl *transport.PLoss) {
				if pl.Sent != 0 {
					packetLoss = pl.Loss()
				}
			})
		}
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
