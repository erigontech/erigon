package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/log/v3"
)

func generateKey() (*ecdsa.PrivateKey, error) {
	key, err := crypto.GenerateKey()
	if err != nil {
		err = fmt.Errorf("failed to generate node key: %w", err)
	}
	return key, err
}

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	discCfg, err := clparams.GetDefaultDiscoveryConfig(clparams.Mainnet)
	if err != nil {
		log.Error("error", "err", err)
		return
	}
	sent, err := sentinel.New(context.Background(), sentinel.SentinelConfig{
		IpAddr:         "127.0.0.1",
		Port:           8080,
		TCPPort:        9000,
		DiscoverConfig: *discCfg,
	})
	if err != nil {
		log.Error("error", "err", err)
		return
	}
	if err := sent.Start(); err != nil {
		log.Error("error", "err", err)
		return
	}
	log.Info("Sentinel started", "enr", sent.String())
	logInterval := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-logInterval.C:
			log.Info("[Lighclient] Networking Report", "peers", sent.PeersCount())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
