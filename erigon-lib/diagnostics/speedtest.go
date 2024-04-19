package diagnostics

import (
	"context"
	"time"

	"github.com/showwin/speedtest-go/speedtest"
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

func (d *DiagnosticClient) runSpeedTest(rootCtx context.Context) NetworkSpeedTestResult {
	var speedtestClient = speedtest.New()
	serverList, _ := speedtestClient.FetchServers()
	targets, _ := serverList.FindServer([]int{})

	latency := time.Duration(0)
	downloadSpeed := float64(0)
	uploadSpeed := float64(0)

	if len(targets) > 0 {
		s := targets[0]
		err := s.PingTestContext(rootCtx, nil)
		if err == nil {
			latency = s.Latency
		}

		err = s.DownloadTestContext(rootCtx)
		if err == nil {
			downloadSpeed = s.DLSpeed
		}

		err = s.UploadTestContext(rootCtx)
		if err == nil {
			uploadSpeed = s.ULSpeed
		}
	}

	return NetworkSpeedTestResult{
		Latency:       latency,
		DownloadSpeed: downloadSpeed,
		UploadSpeed:   uploadSpeed,
	}
}

func (d *DiagnosticClient) GetNetworkSpeed() NetworkSpeedTestResult {
	return d.networkSpeed
}
