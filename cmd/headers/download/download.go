package download

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/dnsdisc"
	"github.com/ledgerwatch/turbo-geth/params"
)

func makeP2PServer(ctx context.Context, protocols []string) (*p2p.Server, error) {
	client := dnsdisc.NewClient(dnsdisc.Config{})

	dns := params.KnownDNSNetwork(params.MainnetGenesisHash, "all")
	dialCandidates, err := client.NewIterator(dns)
	if err != nil {
		return nil, fmt.Errorf("create discovery candidates: %v", err)
	}

	serverKey, err1 := crypto.GenerateKey()
	if err1 != nil {
		return nil, fmt.Errorf("Failed to generate server key: %v", err1)
	}

	p2pConfig := p2p.Config{}
	p2pConfig.PrivateKey = serverKey
	p2pConfig.Name = "header downloader"
	p2pConfig.Logger = log.New()
	p2pConfig.MaxPeers = 100
	p2pConfig.Protocols = []p2p.Protocol{}
	p2pConfig.NodeDatabase = "downloader_nodes"
	pMap := map[string]p2p.Protocol{
		eth.ProtocolName: {
			Name:           eth.ProtocolName,
			Version:        eth.ProtocolVersions[0],
			Length:         eth.ProtocolLengths[eth.ProtocolVersions[0]],
			DialCandidates: dialCandidates,
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
				fmt.Printf("Run protocol on peer %s\n", peer.ID())
				return nil
			},
		},
	}

	for _, protocolName := range protocols {
		p2pConfig.Protocols = append(p2pConfig.Protocols, pMap[protocolName])
	}
	return &p2p.Server{Config: p2pConfig}, nil
}

func rootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx
}

func Download() error {
	ctx := rootContext()
	server, err := makeP2PServer(ctx, []string{eth.ProtocolName})
	if err != nil {
		return err
	}
	// Add protocol
	if err = server.Start(); err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}
	<-ctx.Done()
	return nil
}
