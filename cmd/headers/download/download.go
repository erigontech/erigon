package download

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/dnsdisc"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func nodeKey() *ecdsa.PrivateKey {
	keyfile := "nodekey"
	if key, err := crypto.LoadECDSA(keyfile); err == nil {
		return key
	}
	// No persistent key found, generate and store a new one.
	key, err := crypto.GenerateKey()
	if err != nil {
		log.Crit(fmt.Sprintf("Failed to generate node key: %v", err))
	}
	if err := crypto.SaveECDSA(keyfile, key); err != nil {
		log.Error(fmt.Sprintf("Failed to persist node key: %v", err))
	}
	return key
}

func makeP2PServer(protocols []string) (*p2p.Server, error) {
	client := dnsdisc.NewClient(dnsdisc.Config{})

	dns := params.KnownDNSNetwork(params.MainnetGenesisHash, "all")
	dialCandidates, err := client.NewIterator(dns)
	if err != nil {
		return nil, fmt.Errorf("create discovery candidates: %v", err)
	}

	serverKey := nodeKey()

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
				log.Info(fmt.Sprintf("[%s] Start with peer", peer.ID()))
				genesis := core.DefaultGenesisBlock()
				if err := runPeer(peer, rw, eth.ProtocolVersions[0], eth.DefaultConfig.NetworkID, genesis.Difficulty, params.MainnetGenesisHash, params.MainnetChainConfig, 0 /* head */); err != nil {
					log.Info(fmt.Sprintf("[%s] Error while running peer: %v", peer.ID(), err))
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
		msg.Discard()
		return errResp(eth.ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		msg.Discard()
		return errResp(eth.ErrMsgTooLarge, "message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	var status eth.StatusData
	if err = msg.Decode(&status); err != nil {
		msg.Discard()
		return errResp(eth.ErrDecode, "decode message %v: %v", msg, err)
	}
	msg.Discard()
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
	if err = forkFilter(status.ForkID); err != nil {
		return errResp(eth.ErrForkIDRejected, "%v", err)
	}
	log.Info(fmt.Sprintf("[%s] Received status message OK", peer.ID()))

	for {
		msg, err = rw.ReadMsg()
		if err != nil {
			return fmt.Errorf("reading message: %v", err)
		}
		if msg.Size > eth.ProtocolMaxMsgSize {
			msg.Discard()
			return errResp(eth.ErrMsgTooLarge, "message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
		}
		switch msg.Code {
		case eth.StatusMsg:
			msg.Discard()
			// Status messages should never arrive after the handshake
			return errResp(eth.ErrExtraStatusMsg, "uncontrolled status message")
		case eth.GetBlockHeadersMsg:
			var query eth.GetBlockHeadersData
			if err = msg.Decode(&query); err != nil {
				return errResp(eth.ErrDecode, "decoding %v: %v", msg, err)
			}
			log.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t}", peer.ID(), query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse))
			var headers []*types.Header
			if err = p2p.Send(rw, eth.BlockHeadersMsg, headers); err != nil {
				return fmt.Errorf("send empty headers reply: %v", err)
			}
		case eth.BlockHeadersMsg:
			log.Info(fmt.Sprintf("[%s] BlockHeadersMsg", peer.ID()))
		case eth.GetBlockBodiesMsg:
			// Decode the retrieval message
			msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
			if _, err = msgStream.List(); err != nil {
				return fmt.Errorf("getting list from RLP stream for GetBlockBodiesMsg: %v", err)
			}
			// Gather blocks until the fetch or network limits is reached
			var hash common.Hash
			var hashesStr strings.Builder
			for {
				// Retrieve the hash of the next block
				if err = msgStream.Decode(&hash); errors.Is(err, rlp.EOL) {
					break
				} else if err != nil {
					return errResp(eth.ErrDecode, "decode hash for GetBlockBodiesMsg %v: %v", msg, err)
				}
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hashesStr.WriteString(fmt.Sprintf("%x", hash))
			}
			log.Info(fmt.Sprintf("[%s] GetBlockBodiesMsg {%s}", peer.ID(), hashesStr.String()))
		case eth.BlockBodiesMsg:
			log.Info(fmt.Sprintf("[%s] BlockBodiesMsg", peer.ID()))
		case eth.GetNodeDataMsg:
			log.Info(fmt.Sprintf("[%s] GetNodeData", peer.ID()))
		case eth.GetReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] GetReceiptsMsg", peer.ID()))
		case eth.ReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] ReceiptsMsg", peer.ID()))
		case eth.NewBlockHashesMsg:
			var announces eth.NewBlockHashesData
			if err := msg.Decode(&announces); err != nil {
				return errResp(eth.ErrDecode, "decode NewBlockHashesData %v: %v", msg, err)
			}
			var hashesStr strings.Builder
			for _, announce := range announces {
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hashesStr.WriteString(fmt.Sprintf("%d", announce.Number))
			}
			log.Info(fmt.Sprintf("[%s] NewBlockHashesMsg {%s}", peer.ID(), hashesStr.String()))
		case eth.NewBlockMsg:
			var request eth.NewBlockData
			if err = msg.Decode(&request); err != nil {
				return errResp(eth.ErrDecode, "decode NewBlockMsg %v: %v", msg, err)
			}
			log.Info(fmt.Sprintf("[%s] NewBlockMsg{blockNumber: %d}", peer.ID(), request.Block.NumberU64()))
		case eth.NewPooledTransactionHashesMsg:
			log.Info(fmt.Sprintf("[%s] NewPooledTransactionHashesMsg", peer.ID()))
		case eth.GetPooledTransactionsMsg:
			log.Info(fmt.Sprintf("[%s] GetPooledTransactionsMsg", peer.ID()))
		case eth.TransactionMsg:
			log.Info(fmt.Sprintf("[%s] TransactionMsg", peer.ID()))
		case eth.PooledTransactionsMsg:
			log.Info(fmt.Sprintf("[%s] PooledTransactionsMsg", peer.ID()))
		default:
			log.Error(fmt.Sprintf("[%s] Unknown message code: %d", peer.ID(), msg.Code))
		}
		msg.Discard()
	}
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
