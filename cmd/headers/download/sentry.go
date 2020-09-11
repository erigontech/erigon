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
	"sync"
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
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
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

// SentryMsg declares ID fields necessary for communicating with the sentry
type SentryMsg struct {
	sentryId  int
	requestId int
}

// NewBlockFromSentry is a type of message sent from sentry to the downloader as a result of NewBlockMsg
type NewBlockFromSentry struct {
	SentryMsg
	eth.NewBlockData
}

type PenaltyMsg struct {
	SentryMsg
	penalty headerdownload.Penalty
}

func makeP2PServer(
	peerHeightMap *sync.Map,
	peerRwMap *sync.Map,
	protocols []string,
	newBlockCh chan NewBlockFromSentry,
	penaltyCh chan PenaltyMsg,
) (*p2p.Server, error) {
	client := dnsdisc.NewClient(dnsdisc.Config{})

	dns := params.KnownDNSNetwork(params.MainnetGenesisHash, "all")
	dialCandidates, err := client.NewIterator(dns)
	if err != nil {
		return nil, fmt.Errorf("create discovery candidates: %v", err)
	}

	genesis := core.DefaultGenesisBlock()
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
				peerRwMap.Store(peer.ID().String(), rw)
				if err := runPeer(
					peerHeightMap,
					peer,
					rw,
					eth.ProtocolVersions[0],
					eth.DefaultConfig.NetworkID,
					genesis.Difficulty,
					params.MainnetGenesisHash,
					params.MainnetChainConfig,
					0, /* head */
					newBlockCh,
					penaltyCh,
				); err != nil {
					log.Info(fmt.Sprintf("[%s] Error while running peer: %v", peer.ID(), err))
				}
				peerHeightMap.Delete(peer.ID().String())
				peerRwMap.Delete(peer.ID().String())
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

func runPeer(
	peerMap *sync.Map,
	peer *p2p.Peer,
	rw p2p.MsgReadWriter,
	version uint,
	networkID uint64,
	td *big.Int,
	genesisHash common.Hash,
	chainConfig *params.ChainConfig,
	head uint64,
	newBlockCh chan NewBlockFromSentry,
	_ chan PenaltyMsg,
) error {
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
		return fmt.Errorf("handshake to peer %s: %v", peer.ID().String(), err)
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
				return errResp(eth.ErrDecode, "decoding GetBlockHeadersMsg %v: %v", msg, err)
			}
			log.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t}", peer.ID(), query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse))
			var headers []*types.Header
			if err = p2p.Send(rw, eth.BlockHeadersMsg, headers); err != nil {
				return fmt.Errorf("send empty headers reply: %v", err)
			}
		case eth.BlockHeadersMsg:
			var headers []*types.Header
			if err = msg.Decode(&headers); err != nil {
				return errResp(eth.ErrDecode, "decoding BlockHeadersMsg %v: %v", msg, err)
			}
			var hashesStr strings.Builder
			for _, header := range headers {
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hash := header.Hash()
				hashesStr.WriteString(fmt.Sprintf("%x-%x(%d)", hash[:4], hash[28:], header.Number.Uint64()))
			}
			log.Info(fmt.Sprintf("[%s] BlockHeadersMsg{%s}", peer.ID().String(), hashesStr.String()))
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
				hashesStr.WriteString(fmt.Sprintf("%x-%x", hash[:4], hash[28:]))
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
			if err = msg.Decode(&announces); err != nil {
				return errResp(eth.ErrDecode, "decode NewBlockHashesData %v: %v", msg, err)
			}
			x, _ := peerMap.Load(peer.ID().String())
			highestBlock, _ := x.(uint64)
			var numStr strings.Builder
			for _, announce := range announces {
				if numStr.Len() > 0 {
					numStr.WriteString(",")
				}
				numStr.WriteString(fmt.Sprintf("%d", announce.Number))
				if announce.Number > highestBlock {
					highestBlock = announce.Number
				}
			}
			peerMap.Store(peer.ID().String(), highestBlock)
			log.Info(fmt.Sprintf("[%s] NewBlockHashesMsg {%s}", peer.ID(), numStr.String()))
		case eth.NewBlockMsg:
			var request eth.NewBlockData
			if err = msg.Decode(&request); err != nil {
				return errResp(eth.ErrDecode, "decode NewBlockMsg %v: %v", msg, err)
			}
			log.Info(fmt.Sprintf("[%s] NewBlockMsg{blockNumber: %d}", peer.ID(), request.Block.NumberU64()))
			newBlockCh <- NewBlockFromSentry{SentryMsg: SentryMsg{sentryId: 0, requestId: 0}, NewBlockData: request}
		case eth.NewPooledTransactionHashesMsg:
			var hashes []common.Hash
			if err := msg.Decode(&hashes); err != nil {
				return errResp(eth.ErrDecode, "decode NewPooledTransactionHashesMsg %v: %v", msg, err)
			}
			var hashesStr strings.Builder
			for _, hash := range hashes {
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hashesStr.WriteString(fmt.Sprintf("%x-%x", hash[:4], hash[28:]))
			}
			log.Info(fmt.Sprintf("[%s] NewPooledTransactionHashesMsg {%s}", peer.ID(), hashesStr.String()))
		case eth.GetPooledTransactionsMsg:
			log.Info(fmt.Sprintf("[%s] GetPooledTransactionsMsg", peer.ID()))
		case eth.TransactionMsg:
			var txs []*types.Transaction
			if err := msg.Decode(&txs); err != nil {
				return errResp(eth.ErrDecode, "decode TransactionMsg %v: %v", msg, err)
			}
			var hashesStr strings.Builder
			for _, tx := range txs {
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hash := tx.Hash()
				hashesStr.WriteString(fmt.Sprintf("%x-%x", hash[:4], hash[28:]))
			}
			log.Info(fmt.Sprintf("[%s] TransactionMsg {%s}", peer.ID(), hashesStr.String()))
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

func Download(filesDir string) error {
	ctx := rootContext()
	newBlockCh := make(chan NewBlockFromSentry)
	penaltyCh := make(chan PenaltyMsg)
	reqHeadersCh := make(chan headerdownload.HeaderRequest)
	var peerHeightMap, peerRwMap sync.Map
	server, err := makeP2PServer(&peerHeightMap, &peerRwMap, []string{eth.ProtocolName}, newBlockCh, penaltyCh)
	if err != nil {
		return err
	}
	// Add protocol
	if err = server.Start(); err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}
	go Downloader(ctx, filesDir, newBlockCh, penaltyCh, reqHeadersCh)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-penaltyCh:
				log.Warn(fmt.Sprintf("Received penalty %s for peer %d req %d", req.penalty, req.SentryMsg.sentryId, req.SentryMsg.requestId))
			case req := <-reqHeadersCh:
				// Choose a peer that we can send this request to
				var peerID string
				var found bool
				peerHeightMap.Range(func(key, value interface{}) bool {
					valUint, _ := value.(uint64)
					if valUint >= req.Number {
						peerID = key.(string)
						found = true
						return false
					}
					return true
				})
				if !found {
					log.Warn(fmt.Sprintf("Could not find suitable peer to send GetBlockHeadersData request for block %d", req.Number))
				} else {
					log.Info(fmt.Sprintf("Sending req for hash %x, blocknumber %d, length %d to peer %s\n", req.Hash, req.Number, req.Length, peerID))
					rwRaw, _ := peerRwMap.Load(peerID)
					rw, _ := rwRaw.(p2p.MsgReadWriter)
					if rw == nil {
						log.Error(fmt.Sprintf("Could not find rw for peer %s", peerID))
					} else {
						if err := p2p.Send(rw, eth.GetBlockHeadersMsg, &eth.GetBlockHeadersData{
							Amount:  uint64(req.Length),
							Reverse: true,
							Skip:    0,
							Origin:  eth.HashOrNumber{Hash: req.Hash},
						}); err != nil {
							log.Error(fmt.Sprintf("Failed to send to peer %s: %v", peerID, err))
						}
					}
				}
			}
		}
	}()

	<-ctx.Done()
	return nil
}
