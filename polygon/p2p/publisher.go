package p2p

import (
	"context"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/protocols/eth"
)

type Publisher interface {
	PublishNewBlockHashes(ctx context.Context, block *types.Block) error
	PublishNewBlock(ctx context.Context, block *types.Block) error
}

func newPublisher(messageSender MessageSender) Publisher {
	return &publisher{
		messageSender: messageSender,
	}
}

// https://github.com/ethereum/devp2p/blob/master/caps/eth.md#block-propagation
//
// Rules:
//
// 1. When a NewBlock announcement message is received from a peer, the client first verifies the basic header validity
//    of the block, checking whether the proof-of-work value is valid. It then sends the block to a small fraction of
//    connected peers (usually the square root of the total number of peers) using the NewBlock message.
//
// 2. After the header validity check, the client imports the block into its local chain by executing all transactions
//    contained in the block, computing the block's 'post state'. The block's state-root hash must match the computed
//    post state root. Once the block is fully processed, and considered valid, the client sends a NewBlockHashes
//    message about the block to all peers which it didn't notify earlier. Those peers may request the full block later
//    if they fail to receive it via NewBlock from anyone else.
//
// NewBlockHashes (0x01)
// [[blockhash₁: B_32, number₁: P], [blockhash₂: B_32, number₂: P], ...]
//
// Specify one or more new blocks which have appeared on the network. To be maximally helpful, nodes should inform
// peers of all blocks that they may not be aware of. Including hashes that the sending peer could reasonably be
// considered to know (due to the fact they were previously informed or because that node has itself advertised
// knowledge of the hashes through NewBlockHashes) is considered bad form, and may reduce the reputation of the
// sending node. Including hashes that the sending node later refuses to honour with a proceeding GetBlockHeaders
// message is considered bad form, and may reduce the reputation of the sending node.
//
//
// NewBlock (0x07)
// [block, td: P]
//
// Specify a single complete block that the peer should know about. td is the total difficulty of the block, i.e. the
// sum of all block difficulties up to and including this block.

type publisher struct {
	messageSender MessageSender
	peerTracker   PeerTracker
}

func (p publisher) PublishNewBlockHashes(ctx context.Context, block *types.Block) error {
	hash := block.Hash()
	peers := p.peerTracker.ListPeersMayMissBlockHash(hash)
	blockHashesPacket := eth.NewBlockHashesPacket{
		{
			Hash:   block.Hash(),
			Number: block.NumberU64(),
		},
	}

	for _, peerId := range peers {
		p.messageSender.SendNewBlockHashes(ctx, peerId, blockHashesPacket)
	}

	return p.messageSender.SendNewBlockHashes(ctx, eth.NewBlockHashesPacket{})
}

func (p publisher) PublishNewBlock(ctx context.Context, block *types.Block) error {
	//TODO implement me
	panic("implement me")
}
