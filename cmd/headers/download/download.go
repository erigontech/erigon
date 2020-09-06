package download

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/dnsdisc"
	"github.com/ledgerwatch/turbo-geth/params"
)

func makeP2PServer(protocols []string) (*p2p.Server, error) {
	client := dnsdisc.NewClient(dnsdisc.Config{})

	dns := params.KnownDNSNetwork(params.MainnetGenesisHash, "all")
	dialCandidates, err := client.NewIterator(dns)
	if err != nil {
		return nil, fmt.Errorf("create discovery candidates: %v", err)
	}

	serverKey, err1 := crypto.GenerateKey()
	if err1 != nil {
		return nil, fmt.Errorf("generate server key: %v", err1)
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
				genesis := core.DefaultGenesisBlock()
				if err := runPeer(peer, rw, eth.ProtocolVersions[0], eth.DefaultConfig.NetworkID, genesis.Difficulty, params.MainnetGenesisHash, params.MainnetChainConfig, 0 /* head */); err != nil {
					fmt.Printf("Error while running peer %s: %v\n", peer.ID(), err)
				}
				return nil
			},
		},
	}

	for _, protocolName := range protocols {
		p2pConfig.Protocols = append(p2pConfig.Protocols, pMap[protocolName])
	}
	return &p2p.Server{Config: p2pConfig}, nil
}

func errResp(code int, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

func runPeer(peer *p2p.Peer, rw p2p.MsgReadWriter, version uint, networkID uint64, td *big.Int, genesisHash common.Hash, chainConfig *params.ChainConfig, head uint64) error {
	forkId := forkid.NewID(chainConfig, genesisHash, head)
	// Send handshake message
	if err := p2p.Send(rw, eth.StatusMsg, &eth.StatusData{
		ProtocolVersion: uint32(version),
		NetworkID:       networkID,
		TD:              td,
		Head:            genesisHash, // For now we always start unsyched
		Genesis:         genesisHash,
		ForkID:          forkId,
	}); err != nil {
		return fmt.Errorf("handshake to peer %d: %v", peer.ID(), err)
	}
	// Read handshake message
	msg, err := rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Code != eth.StatusMsg {
		return errResp(eth.ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		return errResp(eth.ErrMsgTooLarge, "message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	var status eth.StatusData
	if err := msg.Decode(&status); err != nil {
		return errResp(eth.ErrDecode, "decode message %v: %v", msg, err)
	}
	if status.NetworkID != networkID {
		return errResp(eth.ErrNetworkIDMismatch, "network id does not match: theirs %d, ours %d", status.NetworkID, networkID)
	}
	if uint(status.ProtocolVersion) != version {
		return errResp(eth.ErrProtocolVersionMismatch, "version does not match: theirs %d, ours %d", status.ProtocolVersion, version)
	}
	if status.Genesis != genesisHash {
		return errResp(eth.ErrGenesisMismatch, "genesis hash does not match: theirs %x, ours %x", status.Genesis, genesisHash)
	}
	forkFilter := forkid.NewFilter(chainConfig, genesisHash, head)
	if err := forkFilter(status.ForkID); err != nil {
		return errResp(eth.ErrForkIDRejected, "%v", err)
	}
	fmt.Printf("Received status mesage OK from %s\n", peer.ID())
	return nil
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
	server, err := makeP2PServer([]string{eth.ProtocolName})
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
