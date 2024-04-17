package diagnostics

import (
	"context"
	"time"

	"github.com/showwin/speedtest-go/speedtest"
)

func (d *DiagnosticClient) setupSpeedtestDiagnostics(rootCtx context.Context) {
	ticker := time.NewTicker(180 * time.Second)
	go func() {
		d.networkSpeedMutex.Lock()
		d.networkSpeed = d.runSpeedTest()
		d.networkSpeedMutex.Unlock()

		for {
			select {
			case <-ticker.C:
				d.networkSpeedMutex.Lock()
				d.networkSpeed = d.runSpeedTest()
				d.networkSpeedMutex.Unlock()
			case <-rootCtx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (d *DiagnosticClient) runSpeedTest() NetworkSpeedTestResult {
	var speedtestClient = speedtest.New()
	serverList, _ := speedtestClient.FetchServers()
	targets, _ := serverList.FindServer([]int{})

	latency := time.Duration(0)
	downloadSpeed := float64(0)
	uploadSpeed := float64(0)

	if len(targets) > 0 {
		s := targets[0]
		err := s.PingTest(nil)
		if err == nil {
			latency = s.Latency
		}

		err = s.DownloadTest()
		if err == nil {
			downloadSpeed = s.DLSpeed
		}

		err = s.UploadTest()
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
