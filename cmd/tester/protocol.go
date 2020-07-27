package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type statusData struct {
	ProtocolVersion uint32
	NetworkID       uint64
	TD              *big.Int
	CurrentBlock    common.Hash
	GenesisBlock    common.Hash
	ForkID          forkid.ID
}

type TesterProtocol struct {
	name             string
	protocolVersion  uint32
	networkId        uint64
	mainnetGenesis   *types.Block
	genesisBlockHash common.Hash
	blockFeeder      BlockFeeder
	forkFeeder       BlockFeeder
	blockMarkers     []uint64 // Bitmap to remember which blocks (or just header if the blocks are empty) have been sent already
	// This is to prevent double counting them
	forkBase   uint64
	forkHeight uint64
	fork       bool
	debug      bool
	debugCh    chan struct{}
	forkCh     chan struct{}
}

func NewTesterProtocol(name string, fork bool, debug bool) *TesterProtocol {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	mainnetGenesis := core.DefaultGenesisBlock().MustCommit(db)
	return &TesterProtocol{
		name: name,
		mainnetGenesis: mainnetGenesis,
		fork: fork,
		debug: debug,
		debugCh: make(chan struct{}, 1),
		forkCh: make(chan struct{}, 1),
	}
}

// Return true if the block has already been marked. If the block has not been marked, returns false and marks it
func (tp *TesterProtocol) markBlockSent(blockNumber uint) bool {
	lengthNeeded := (blockNumber+63)/64 + 1
	if lengthNeeded > uint(len(tp.blockMarkers)) {
		tp.blockMarkers = append(tp.blockMarkers, make([]uint64, lengthNeeded-uint(len(tp.blockMarkers)))...)
	}
	bitMask := (uint64(1) << (blockNumber & 63))
	result := (tp.blockMarkers[blockNumber/64] & bitMask) != 0
	tp.blockMarkers[blockNumber/64] |= bitMask
	return result
}

func (tp *TesterProtocol) debugProtocolRun(ctx context.Context, peer *p2p.Peer, rw p2p.MsgReadWriter) error {
	v, err := json.Marshal(genesis())
	if err != nil {
		return err
	}
	err = p2p.Send(rw, eth.DebugSetGenesisMsg, v)
	if err != nil {
		return fmt.Errorf("[%s] failed to send DebugSetGenesisMsg message to peer: %w", tp.name, err)
	}

	tp.debugCh <- struct{}{}

	msg, err := rw.ReadMsg()
	if err != nil {
		fmt.Printf("[%s] Failed to recevied DebugSetGenesisMsg message from peer: %v\n", tp.name, err)
		return err
	}
	if msg.Code != eth.DebugSetGenesisMsg {
		return fmt.Errorf("[%s] first msg has code %x (!= %x)", tp.name, msg.Code, eth.DebugSetGenesisMsg)
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		return fmt.Errorf("[%s] message too large %v > %v", tp.name, msg.Size, eth.ProtocolMaxMsgSize)
	}

	log.Info("eth set custom genesis.config", "name", tp.name)
	<- ctx.Done() // Wait until the protocol is closed
	return nil
}

func (tp *TesterProtocol) WaitForFork(ctx context.Context) {
	fmt.Printf("[%s] Waiting for fork...\n", tp.name)
	select {
	case <- ctx.Done():
	case <- tp.forkCh:
	}
	fmt.Printf("[%s] Cleared the fork\n", tp.name)
}

func (tp *TesterProtocol) protocolRun(ctx context.Context, peer *p2p.Peer, rw p2p.MsgReadWriter) error {
	log.Info("Ethereum peer connected", "name", tp.name, "peer", peer.Name())
	log.Debug("Protocol version", "name", tp.name, "version", tp.protocolVersion)
	time.Sleep(3*time.Second)
	if tp.debug {
		// Wait for the debug protocol to finish its message exchange
		select {
		case <- ctx.Done():
		case <- tp.debugCh:
		}
	}

	// Synchronous "eth" handshake
	err := p2p.Send(rw, eth.StatusMsg, &statusData{
		ProtocolVersion: tp.protocolVersion,
		NetworkID:       tp.networkId,
		TD:              tp.blockFeeder.TotalDifficulty(),
		CurrentBlock:    tp.blockFeeder.LastBlock().Hash(),
		GenesisBlock:    tp.mainnetGenesis.Hash(),
		ForkID:          forkid.NewID(params.MainnetChainConfig, tp.mainnetGenesis.Hash(), 0),
	})
	log.Info("Sent status message", "name", tp.name)
	if err != nil {
		return fmt.Errorf("[%s] failed to send status message to peer: %w", tp.name, err)
	}
	msg, err := rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("[%s] failed to recevied state message from peer: %w", tp.name, err)
	}
	fmt.Printf("[%s] Received response from status message\n", tp.name)
	if msg.Code != eth.StatusMsg {
		return fmt.Errorf("[%s] first msg has code %x (!= %x)", tp.name, msg.Code, eth.StatusMsg)
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		return fmt.Errorf("[%s] message too large %v > %v", tp.name, msg.Size, eth.ProtocolMaxMsgSize)
	}
	var statusResp statusData
	if err := msg.Decode(&statusResp); err != nil {
		return fmt.Errorf("[%s] failed to decode msg %v: %v", tp.name, msg, err)
	}
	if statusResp.GenesisBlock != tp.genesisBlockHash {
		return fmt.Errorf("[%s] mismatched genesis block hash %x (!= %x)", tp.name, statusResp.GenesisBlock[:8], tp.genesisBlockHash[:8])
	}
	if statusResp.NetworkID != tp.networkId {
		return fmt.Errorf("[%s] mismatched network id %d (!= %d)", tp.name, statusResp.NetworkID, tp.networkId)
	}
	if statusResp.ProtocolVersion != tp.protocolVersion {
		return fmt.Errorf("[%s] mismatched protocol version %d (!= %d)", tp.name, statusResp.ProtocolVersion, tp.protocolVersion)
	}
	log.Info(fmt.Sprintf("[%s] eth handshake complete, block hash: %x, block difficulty: %s", tp.name, statusResp.CurrentBlock, statusResp.TD))

	sentBlocks := 0
	emptyBlocks := 0
	signaledHead := false
	lastBlockNumber := int(tp.blockFeeder.LastBlock().NumberU64())
	fmt.Printf("[%s] lastBlockNumber: %d\n", tp.name, lastBlockNumber)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read the next message
		msg, err = rw.ReadMsg()
		if err != nil {
			return fmt.Errorf("[%s] failed to receive message from peer: %w", tp.name, err)
		}
		switch {
		case msg.Code == eth.GetBlockHeadersMsg:
			if emptyBlocks, err = tp.handleGetBlockHeaderMsg(msg, rw, tp.blockFeeder, emptyBlocks); err != nil {
				return err
			}
		case msg.Code == eth.GetBlockBodiesMsg:
			if sentBlocks, err = tp.handleGetBlockBodiesMsg(msg, rw, tp.blockFeeder, sentBlocks); err != nil {
				return err
			}
		case msg.Code == eth.NewBlockHashesMsg:
			if signaledHead, err = tp.handleNewBlockHashesMsg(msg, rw); err != nil {
				return err
			}
		default:
			log.Info("Next message", "name", tp.name, "msg", msg)
		}
		if tp.fork && signaledHead {
			break
		}
		if tp.fork && emptyBlocks+sentBlocks >= lastBlockNumber {
			break
		}
	}
	if tp.fork {
		tp.forkCh <- struct{}{}
		log.Info("Peer downloaded all our blocks, entering next phase", "name", tp.name)
		tp.announceForkHeaders(rw)
		log.Info("Announced fork blocks", "name", tp.name)
		for i := 0; i < 10000; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Read the next message
			msg, err = rw.ReadMsg()
			if err != nil {
				return fmt.Errorf("[%s] failed to receive state message from peer: %w", tp.name, err)
			}
			switch {
			case msg.Code == eth.GetBlockHeadersMsg:
				if emptyBlocks, err = tp.handleGetBlockHeaderMsg(msg, rw, tp.forkFeeder, emptyBlocks); err != nil {
					return err
				}
			case msg.Code == eth.GetBlockBodiesMsg:
				if sentBlocks, err = tp.handleGetBlockBodiesMsg(msg, rw, tp.forkFeeder, sentBlocks); err != nil {
					return err
				}
			case msg.Code == eth.NewBlockHashesMsg:
				if _, err = tp.handleNewBlockHashesMsg(msg, rw); err != nil {
					return err
				}
			default:
				log.Info("Next message", "name", tp.name, "msg", msg)
			}
		}
	}
	return nil
}

// hashOrNumber is a combined field for specifying an origin block.
type hashOrNumber struct {
	Hash   common.Hash // Block hash from which to retrieve headers (excludes Number)
	Number uint64      // Block hash from which to retrieve headers (excludes Hash)
}

// getBlockHeadersData represents a block header query.
type getBlockHeadersData struct {
	Origin  hashOrNumber // Block from which to retrieve headers
	Amount  uint64       // Maximum number of headers to retrieve
	Skip    uint64       // Blocks to skip between consecutive headers
	Reverse bool         // Query direction (false = rising towards latest, true = falling towards genesis)
}

// newBlockHashesData is the network packet for the block announcements.
type newBlockHashesData []struct {
	Hash   common.Hash // Hash of one particular block being announced
	Number uint64      // Number of one particular block being announced
}

// EncodeRLP is a specialized encoder for hashOrNumber to encode only one of the
// two contained union fields.
func (hn *hashOrNumber) EncodeRLP(w io.Writer) error {
	if hn.Hash == (common.Hash{}) {
		return rlp.Encode(w, hn.Number)
	}
	if hn.Number != 0 {
		return fmt.Errorf("both origin hash (%x) and number (%d) provided", hn.Hash, hn.Number)
	}
	return rlp.Encode(w, hn.Hash)
}

// DecodeRLP is a specialized decoder for hashOrNumber to decode the contents
// into either a block hash or a block number.
func (hn *hashOrNumber) DecodeRLP(s *rlp.Stream) error {
	_, size, _ := s.Kind()
	origin, err := s.Raw()
	if err == nil {
		switch {
		case size == 32:
			err = rlp.DecodeBytes(origin, &hn.Hash)
		case size <= 8:
			err = rlp.DecodeBytes(origin, &hn.Number)
		default:
			err = fmt.Errorf("invalid input size %d for origin", size)
		}
	}
	return err
}

func (tp *TesterProtocol) handleGetBlockHeaderMsg(msg p2p.Msg, rw p2p.MsgReadWriter, blockFeeder BlockFeeder, emptyBlocks int) (int, error) {
	newEmptyBlocks := emptyBlocks
	var query getBlockHeadersData
	if err := msg.Decode(&query); err != nil {
		return newEmptyBlocks, fmt.Errorf("[%s] failed to decode msg %v: %w", tp.name, msg, err)
	}
	log.Trace("GetBlockHeadersMsg", "name", tp.name, "query", query)
	headers := []*types.Header{}
	if query.Origin.Hash == (common.Hash{}) && !query.Reverse {
		number := query.Origin.Number
		for i := 0; i < int(query.Amount); i++ {
			if header := blockFeeder.GetHeaderByNumber(number); header != nil {
				//fmt.Printf("Going to send block %d\n", header.Number.Uint64())
				headers = append(headers, header)
				if header.TxHash == types.EmptyRootHash {
					if !tp.markBlockSent(uint(number)) {
						newEmptyBlocks++
					}
				}
			} else {
				//fmt.Printf("Could not find header with number %d\n", number)
			}
			number += query.Skip + 1
		}
	}
	if query.Origin.Hash != (common.Hash{}) && query.Amount == 1 && query.Skip == 0 && !query.Reverse {
		if header := blockFeeder.GetHeaderByHash(query.Origin.Hash); header != nil {
			log.Trace("Going to send header", "name", tp.name, "number", header.Number.Uint64())
			headers = append(headers, header)
		}
	}
	if err := p2p.Send(rw, eth.BlockHeadersMsg, headers); err != nil {
		return newEmptyBlocks, fmt.Errorf("[%s] failed to send headers: %w", tp.name, err)
	}
	log.Info(fmt.Sprintf("[%s] Sent %d headers, empty blocks so far %d", tp.name, len(headers), newEmptyBlocks))
	return newEmptyBlocks, nil
}

func (tp *TesterProtocol) handleGetBlockBodiesMsg(msg p2p.Msg, rw p2p.MsgReadWriter, blockFeeder BlockFeeder, sentBlocks int) (int, error) {
	newSentBlocks := sentBlocks
	msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
	log.Trace("GetBlockBodiesMsg with size", "name", tp.name, "size", msg.Size)
	if _, err := msgStream.List(); err != nil {
		return newSentBlocks, err
	}
	// Gather blocks until the fetch or network limits is reached
	var (
		hash   common.Hash
		bodies []rlp.RawValue
	)
	for {
		// Retrieve the hash of the next block
		if err := msgStream.Decode(&hash); err == rlp.EOL {
			break
		} else if err != nil {
			return newSentBlocks, fmt.Errorf("[%s] failed to decode msg %v: %w", tp.name, msg, err)
		}
		// Retrieve the requested block body, stopping if enough was found
		if block, err := blockFeeder.GetBlockByHash(hash); err != nil {
			return newSentBlocks, fmt.Errorf("[%s] failed to read block %w", tp.name, err)
		} else if block != nil {
			if !tp.markBlockSent(uint(block.NumberU64())) {
				newSentBlocks++
			}
			body := block.Body()
			data, err := rlp.EncodeToBytes(body)
			if err != nil {
				return newSentBlocks, fmt.Errorf("[%s] failed to encode body: %w", tp.name, err)
			}
			bodies = append(bodies, data)
		}
	}
	if err := p2p.Send(rw, eth.BlockBodiesMsg, bodies); err != nil {
		return newSentBlocks, err
	}
	log.Info("Sending bodies", "name", tp.name, "number", len(bodies), "progress", newSentBlocks)

	return newSentBlocks, nil
}

func (tp *TesterProtocol) announceForkHeaders(rw p2p.MsgWriter) {
	var request = make(newBlockHashesData, int(tp.forkHeight))
	for fb := 0; fb < int(tp.forkHeight); fb++ {
		blockNumber := tp.forkBase + uint64(fb)
		block, err := tp.forkFeeder.GetBlockByNumber(blockNumber)
		if err != nil {
			panic(err)
		}
		request[fb].Hash = block.Hash()
		request[fb].Number = blockNumber
	}
	if err := p2p.Send(rw, eth.NewBlockHashesMsg, request); err != nil {
		panic(err)
	}
}

func (tp *TesterProtocol) SendTransaction(tx *types.Transaction) {
	
}

func (tp *TesterProtocol) sendLastBlock(rw p2p.MsgReadWriter, blockFeeder BlockFeeder) error {
	return p2p.Send(rw, eth.NewBlockMsg, []interface{}{blockFeeder.LastBlock(), blockFeeder.TotalDifficulty()})
}

func (tp *TesterProtocol) handleNewBlockHashesMsg(msg p2p.Msg, rw p2p.MsgReadWriter) (bool, error) {
	var blockHashMsg newBlockHashesData
	if err := msg.Decode(&blockHashMsg); err != nil {
		return false, fmt.Errorf("[%s] failed to decode msg %v: %w", tp.name, msg, err)
	}
	log.Trace("NewBlockHashesMsg", "name", tp.name, "query", blockHashMsg)
	signaledHead := false
	for _, bh := range blockHashMsg {
		if bh.Number == tp.blockFeeder.LastBlock().NumberU64() {
			signaledHead = true
			break
		}
	}
	return signaledHead, nil
}
