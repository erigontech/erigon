package main

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/log/v3"
)

var (
	defaultIpAddr  = "127.0.0.1" // Localhost
	defaultPort    = 8080
	defaultTcpPort = uint(9000)
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	discCfg, err := clparams.GetDefaultDiscoveryConfig(clparams.MainnetNetwork)
	if err != nil {
		log.Error("error", "err", err)
		return
	}
	sent, err := sentinel.New(context.Background(), sentinel.SentinelConfig{
		IpAddr:         defaultIpAddr,
		Port:           defaultPort,
		TCPPort:        defaultTcpPort,
		DiscoverConfig: *discCfg,
	})
	if err != nil {
		log.Error("error", "err", err)
		return
	}
	if err := sent.Start(); err != nil {
		log.Error("failed to start sentinel", "err", err)
		return
	}
	log.Info("Sentinel started", "enr", sent.String())
	logInterval := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-logInterval.C:
			log.Info("[Lighclient] Networking Report", "peers", sent.GetPeersCount())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
