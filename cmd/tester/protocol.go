package main

import (
	"fmt"
	"io"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type statusData struct {
	ProtocolVersion uint32
	NetworkID       uint64
	TD              *big.Int
	CurrentBlock    common.Hash
	GenesisBlock    common.Hash
}

type TesterProtocol struct {
	protocolVersion  uint32
	networkId        uint64
	genesisBlockHash common.Hash
	blockFeeder      BlockFeeder
	forkFeeder       BlockFeeder
	blockMarkers     []uint64 // Bitmap to remember which blocks (or just header if the blocks are empty) have been sent already
	// This is to prevent double counting them
}

func NewTesterProtocol() *TesterProtocol {
	return &TesterProtocol{}
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

func (tp *TesterProtocol) protocolRun(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
	fmt.Printf("Ethereum peer connected: %s\n", peer.Name())
	// Synchronous "eth" handshake
	err := p2p.Send(rw, eth.StatusMsg, &statusData{
		ProtocolVersion: tp.protocolVersion,
		NetworkID:       tp.networkId,
		TD:              tp.blockFeeder.TotalDifficulty(),
		CurrentBlock:    tp.blockFeeder.LastBlock().Hash(),
		GenesisBlock:    tp.genesisBlockHash,
	})
	if err != nil {
		fmt.Printf("Failed to send status message to peer: %v\n", err)
		return err
	}
	msg, err := rw.ReadMsg()
	if err != nil {
		fmt.Printf("Failed to recevied state message from peer: %v\n", err)
		return err
	}
	if msg.Code != eth.StatusMsg {
		fmt.Printf("first msg has code %x (!= %x)\n", msg.Code, eth.StatusMsg)
		return fmt.Errorf("first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		fmt.Printf("message too large %v > %v", msg.Size, eth.ProtocolMaxMsgSize)
		return fmt.Errorf("message too large %v > %v", msg.Size, eth.ProtocolMaxMsgSize)
	}
	var statusResp statusData
	if err := msg.Decode(&statusResp); err != nil {
		fmt.Printf("Failed to decode msg %v: %v\n", msg, err)
		return fmt.Errorf("failed to decode msg %v: %v", msg, err)
	}
	if statusResp.GenesisBlock != tp.genesisBlockHash {
		fmt.Printf("Mismatched genesis block hash %x (!= %x)", statusResp.GenesisBlock[:8], tp.genesisBlockHash[:8])
		return fmt.Errorf("mismatched genesis block hash %x (!= %x)", statusResp.GenesisBlock[:8], tp.genesisBlockHash[:8])
	}
	if statusResp.NetworkID != tp.networkId {
		fmt.Printf("Mismatched network id %d (!= %d)", statusResp.NetworkID, tp.networkId)
		return fmt.Errorf("mismatched network id %d (!= %d)", statusResp.NetworkID, tp.networkId)
	}
	if statusResp.ProtocolVersion != tp.protocolVersion {
		fmt.Printf("Mismatched protocol version %d (!= %d)", statusResp.ProtocolVersion, tp.protocolVersion)
		return fmt.Errorf("mismatched protocol version %d (!= %d)", statusResp.ProtocolVersion, tp.protocolVersion)
	}
	fmt.Printf("eth handshake complete, block hash: %x, block difficulty: %s\n", statusResp.CurrentBlock, statusResp.TD)
	//lastBlockNumber := int(tp.blockFeeder.LastBlock().NumberU64())
	sentBlocks := 0
	emptyBlocks := 0
	signaledHead := false
	for {
		// Read the next message
		msg, err = rw.ReadMsg()
		if err != nil {
			fmt.Printf("Failed to receive state message from peer: %v\n", err)
			return err
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
			fmt.Printf("Next message: %v\n", msg)
		}
		if signaledHead {
			break
		}
		//if emptyBlocks + sentBlocks >= lastBlockNumber {
		//	break
		//}
	}
	fmt.Printf("Peer downloaded all our blocks, entering next phase\n")
	tp.sendLastBlock(rw, tp.forkFeeder)
	fmt.Printf("Announced fork block\n")
	for i := 0; i < 10000; i++ {
		fmt.Printf("Message loop i %d\n", i)
		// Read the next message
		msg, err = rw.ReadMsg()
		if err != nil {
			fmt.Printf("Failed to receive state message from peer: %v\n", err)
			return err
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
			if signaledHead, err = tp.handleNewBlockHashesMsg(msg, rw); err != nil {
				return err
			}
		default:
			fmt.Printf("Next message: %v\n", msg)
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
		fmt.Printf("Failed to decode msg %v: %v\n", msg, err)
		return newEmptyBlocks, fmt.Errorf("Failed to decode msg %v: %v\n", msg, err)
	}
	fmt.Printf("GetBlockHeadersMsg: %v\n", query)
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
			fmt.Printf("Going to send block %d\n", header.Number.Uint64())
			headers = append(headers, header)
		}
	}
	if err := p2p.Send(rw, eth.BlockHeadersMsg, headers); err != nil {
		fmt.Printf("Failed to send headers: %v\n", err)
		return newEmptyBlocks, err
	}
	fmt.Printf("Sent %d headers, empty blocks so far %d\n", len(headers), newEmptyBlocks)
	return newEmptyBlocks, nil
}

func (tp *TesterProtocol) handleGetBlockBodiesMsg(msg p2p.Msg, rw p2p.MsgReadWriter, blockFeeder BlockFeeder, sentBlocks int) (int, error) {
	newSentBlocks := sentBlocks
	msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
	fmt.Printf("GetBlockBodiesMsg with size %d\n", msg.Size)
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
			fmt.Printf("Failed to decode msg %v: %v", msg, err)
			return newSentBlocks, fmt.Errorf("Failed to decode msg %v: %v", msg, err)
		}
		// Retrieve the requested block body, stopping if enough was found
		if block, err := blockFeeder.GetBlockByHash(hash); err != nil {
			fmt.Printf("Failed to read block %v", err)
			return newSentBlocks, fmt.Errorf("Failed to read block %v", err)
		} else if block != nil {
			if !tp.markBlockSent(uint(block.NumberU64())) {
				newSentBlocks++
			}
			data, err := rlp.EncodeToBytes(block.Body())
			if err != nil {
				fmt.Printf("Failed to encode body: %v", err)
				return newSentBlocks, fmt.Errorf("Failed to encode body: %v", err)
			}
			bodies = append(bodies, data)
		}
	}
	p2p.Send(rw, eth.BlockBodiesMsg, bodies)
	fmt.Printf("Sent %d bodies, total so far %d\n", len(bodies), newSentBlocks)
	return newSentBlocks, nil
}

func (tp *TesterProtocol) announceForkBlock(rw p2p.MsgReadWriter) error {
	request := make(newBlockHashesData, 1)
	request[0].Hash = tp.forkFeeder.LastBlock().Hash()
	request[0].Number = tp.forkFeeder.LastBlock().NumberU64()
	return p2p.Send(rw, eth.NewBlockHashesMsg, request)
}

func (tp *TesterProtocol) sendLastBlock(rw p2p.MsgReadWriter, blockFeeder BlockFeeder) error {
	return p2p.Send(rw, eth.NewBlockMsg, []interface{}{blockFeeder.LastBlock(), blockFeeder.TotalDifficulty()})
}

func (tp *TesterProtocol) handleNewBlockHashesMsg(msg p2p.Msg, rw p2p.MsgReadWriter) (bool, error) {
	var blockHashMsg newBlockHashesData
	if err := msg.Decode(&blockHashMsg); err != nil {
		fmt.Printf("Failed to decode msg %v: %v\n", msg, err)
		return false, fmt.Errorf("Failed to decode msg %v: %v\n", msg, err)
	}
	fmt.Printf("NewBlockHashesMsg: %v\n", blockHashMsg)
	signaledHead := false
	for _, bh := range blockHashMsg {
		if bh.Number == tp.blockFeeder.LastBlock().NumberU64() {
			signaledHead = true
			break
		}
	}
	return signaledHead, nil
}
