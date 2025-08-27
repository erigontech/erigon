// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"fmt"
	"io"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p/forkid"
)

var ProtocolToString = map[uint]string{
	direct.ETH67: "eth67",
	direct.ETH68: "eth68",
}

// ProtocolName is the official short name of the `eth` protocol used during
// devp2p capability negotiation.
const ProtocolName = "eth"

// maxMessageSize is the maximum cap on the size of a protocol message.
const maxMessageSize = 10 * 1024 * 1024
const ProtocolMaxMsgSize = maxMessageSize

const (
	// Protocol messages in eth/64
	StatusMsg          = 0x00
	NewBlockHashesMsg  = 0x01
	TransactionsMsg    = 0x02
	GetBlockHeadersMsg = 0x03
	BlockHeadersMsg    = 0x04
	GetBlockBodiesMsg  = 0x05
	BlockBodiesMsg     = 0x06
	NewBlockMsg        = 0x07
	GetReceiptsMsg     = 0x0f
	ReceiptsMsg        = 0x10

	// Protocol messages overloaded in eth/65
	NewPooledTransactionHashesMsg = 0x08
	GetPooledTransactionsMsg      = 0x09
	PooledTransactionsMsg         = 0x0a
)

var ToProto = map[uint]map[uint64]proto_sentry.MessageId{
	direct.ETH67: {
		GetBlockHeadersMsg:            proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
		BlockHeadersMsg:               proto_sentry.MessageId_BLOCK_HEADERS_66,
		GetBlockBodiesMsg:             proto_sentry.MessageId_GET_BLOCK_BODIES_66,
		BlockBodiesMsg:                proto_sentry.MessageId_BLOCK_BODIES_66,
		GetReceiptsMsg:                proto_sentry.MessageId_GET_RECEIPTS_66,
		ReceiptsMsg:                   proto_sentry.MessageId_RECEIPTS_66,
		NewBlockHashesMsg:             proto_sentry.MessageId_NEW_BLOCK_HASHES_66,
		NewBlockMsg:                   proto_sentry.MessageId_NEW_BLOCK_66,
		TransactionsMsg:               proto_sentry.MessageId_TRANSACTIONS_66,
		NewPooledTransactionHashesMsg: proto_sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		GetPooledTransactionsMsg:      proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_66,
		PooledTransactionsMsg:         proto_sentry.MessageId_POOLED_TRANSACTIONS_66,
	},
	direct.ETH68: {
		GetBlockHeadersMsg:            proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
		BlockHeadersMsg:               proto_sentry.MessageId_BLOCK_HEADERS_66,
		GetBlockBodiesMsg:             proto_sentry.MessageId_GET_BLOCK_BODIES_66,
		BlockBodiesMsg:                proto_sentry.MessageId_BLOCK_BODIES_66,
		GetReceiptsMsg:                proto_sentry.MessageId_GET_RECEIPTS_66,
		ReceiptsMsg:                   proto_sentry.MessageId_RECEIPTS_66,
		NewBlockHashesMsg:             proto_sentry.MessageId_NEW_BLOCK_HASHES_66,
		NewBlockMsg:                   proto_sentry.MessageId_NEW_BLOCK_66,
		TransactionsMsg:               proto_sentry.MessageId_TRANSACTIONS_66,
		NewPooledTransactionHashesMsg: proto_sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68, // Modified in eth/68
		GetPooledTransactionsMsg:      proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_66,
		PooledTransactionsMsg:         proto_sentry.MessageId_POOLED_TRANSACTIONS_66,
	},
}

var FromProto = map[uint]map[proto_sentry.MessageId]uint64{
	direct.ETH67: {
		proto_sentry.MessageId_GET_BLOCK_HEADERS_66:             GetBlockHeadersMsg,
		proto_sentry.MessageId_BLOCK_HEADERS_66:                 BlockHeadersMsg,
		proto_sentry.MessageId_GET_BLOCK_BODIES_66:              GetBlockBodiesMsg,
		proto_sentry.MessageId_BLOCK_BODIES_66:                  BlockBodiesMsg,
		proto_sentry.MessageId_GET_RECEIPTS_66:                  GetReceiptsMsg,
		proto_sentry.MessageId_RECEIPTS_66:                      ReceiptsMsg,
		proto_sentry.MessageId_NEW_BLOCK_HASHES_66:              NewBlockHashesMsg,
		proto_sentry.MessageId_NEW_BLOCK_66:                     NewBlockMsg,
		proto_sentry.MessageId_TRANSACTIONS_66:                  TransactionsMsg,
		proto_sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66: NewPooledTransactionHashesMsg,
		proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_66:       GetPooledTransactionsMsg,
		proto_sentry.MessageId_POOLED_TRANSACTIONS_66:           PooledTransactionsMsg,
	},
	direct.ETH68: {
		proto_sentry.MessageId_GET_BLOCK_HEADERS_66:             GetBlockHeadersMsg,
		proto_sentry.MessageId_BLOCK_HEADERS_66:                 BlockHeadersMsg,
		proto_sentry.MessageId_GET_BLOCK_BODIES_66:              GetBlockBodiesMsg,
		proto_sentry.MessageId_BLOCK_BODIES_66:                  BlockBodiesMsg,
		proto_sentry.MessageId_GET_RECEIPTS_66:                  GetReceiptsMsg,
		proto_sentry.MessageId_RECEIPTS_66:                      ReceiptsMsg,
		proto_sentry.MessageId_NEW_BLOCK_HASHES_66:              NewBlockHashesMsg,
		proto_sentry.MessageId_NEW_BLOCK_66:                     NewBlockMsg,
		proto_sentry.MessageId_TRANSACTIONS_66:                  TransactionsMsg,
		proto_sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68: NewPooledTransactionHashesMsg,
		proto_sentry.MessageId_GET_POOLED_TRANSACTIONS_66:       GetPooledTransactionsMsg,
		proto_sentry.MessageId_POOLED_TRANSACTIONS_66:           PooledTransactionsMsg,
	},
}

// Packet represents a p2p message in the `eth` protocol.
type Packet interface {
	Name() string // Name returns a string corresponding to the message type.
	Kind() byte   // Kind returns the message type.
}

// StatusPacket is the network packet for the status message for eth/64 and later.
type StatusPacket struct {
	ProtocolVersion uint32
	NetworkID       uint64
	TD              *big.Int
	Head            common.Hash
	Genesis         common.Hash
	ForkID          forkid.ID
}

// NewBlockHashesPacket is the network packet for the block announcements.
type NewBlockHashesPacket []struct {
	Hash   common.Hash // Hash of one particular block being announced
	Number uint64      // Number of one particular block being announced
}

// GetBlockHeadersPacket represents a block header query.
type GetBlockHeadersPacket struct {
	Origin  HashOrNumber // Block from which to retrieve headers
	Amount  uint64       // Maximum number of headers to retrieve
	Skip    uint64       // Blocks to skip between consecutive headers
	Reverse bool         // Query direction (false = rising towards latest, true = falling towards genesis)
}

// GetBlockHeadersPacket66 represents a block header query over eth/66
type GetBlockHeadersPacket66 struct {
	RequestId uint64
	*GetBlockHeadersPacket
}

// HashOrNumber is a combined field for specifying an origin block.
type HashOrNumber struct {
	Hash   common.Hash // Block hash from which to retrieve headers (excludes Number)
	Number uint64      // Block hash from which to retrieve headers (excludes Hash)
}

// EncodeRLP is a specialized encoder for HashOrNumber to encode only one of the
// two contained union fields.
func (hn *HashOrNumber) EncodeRLP(w io.Writer) error {
	if hn.Hash == (common.Hash{}) {
		return rlp.Encode(w, hn.Number)
	}
	if hn.Number != 0 {
		return fmt.Errorf("both origin hash (%x) and number (%d) provided", hn.Hash, hn.Number)
	}
	return rlp.Encode(w, hn.Hash)
}

// DecodeRLP is a specialized decoder for HashOrNumber to decode the contents
// into either a block hash or a block number.
func (hn *HashOrNumber) DecodeRLP(s *rlp.Stream) error {
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

// BlockHeadersPacket represents a block header response.
type BlockHeadersPacket []*types.Header

// BlockHeadersPacket66 represents a block header response over eth/66.
type BlockHeadersPacket66 struct {
	RequestId uint64
	BlockHeadersPacket
}

// NewBlockPacket is the network packet for the block propagation message.
type NewBlockPacket struct {
	Block *types.Block
	TD    *big.Int
}

func (nbp NewBlockPacket) EncodeRLP(w io.Writer) error {
	encodingSize := 0
	// size of Block
	blockLen := nbp.Block.EncodingSize()
	encodingSize += rlp.ListPrefixLen(blockLen) + blockLen
	// size of TD
	encodingSize++
	var tdBitLen, tdLen int
	if nbp.TD != nil {
		tdBitLen = nbp.TD.BitLen()
		if tdBitLen >= 8 {
			tdLen = common.BitLenToByteLen(tdBitLen)
		}
	}
	encodingSize += tdLen
	var b [33]byte
	// prefix
	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	// encode Block
	if err := nbp.Block.EncodeRLP(w); err != nil {
		return err
	}
	// encode TD
	if tdBitLen < 8 {
		if tdBitLen > 0 {
			b[0] = byte(nbp.TD.Uint64())
		} else {
			b[0] = 128
		}
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + byte(tdLen)
		nbp.TD.FillBytes(b[1 : 1+tdLen])
		if _, err := w.Write(b[:1+tdLen]); err != nil {
			return err
		}
	}
	return nil
}

func (nbp *NewBlockPacket) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	// decode Block
	nbp.Block = &types.Block{}
	if err = nbp.Block.DecodeRLP(s); err != nil {
		return err
	}
	// decode TD
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read TD: %w", err)
	}
	nbp.TD = new(big.Int).SetBytes(b)
	if err = s.ListEnd(); err != nil {
		return err
	}
	return nil
}

// SanityCheck verifies that the values are reasonable, as a DoS protection
func (request *NewBlockPacket) SanityCheck() error {
	return request.Block.SanityCheck()
}

// GetBlockBodiesPacket represents a block body query.
type GetBlockBodiesPacket []common.Hash

// GetBlockBodiesPacket represents a block body query over eth/66.
type GetBlockBodiesPacket66 struct {
	RequestId uint64
	GetBlockBodiesPacket
}

// BlockBodiesPacket is the network packet for block content distribution.
type BlockBodiesPacket []*types.Body

// BlockRawBodiesPacket is the network packet for block content distribution.
type BlockRawBodiesPacket []*types.RawBody

// BlockBodiesPacket66 is the network packet for block content distribution over eth/66.
type BlockBodiesPacket66 struct {
	RequestId uint64
	BlockBodiesPacket
}

// BlockRawBodiesPacket66 is the network packet for block content distribution over eth/66.
type BlockRawBodiesPacket66 struct {
	RequestId uint64
	BlockRawBodiesPacket
}

// BlockBodiesRLPPacket is used for replying to block body requests, in cases
// where we already have them RLP-encoded, and thus can avoid the decode-encode
// roundtrip.
type BlockBodiesRLPPacket []rlp.RawValue

// BlockBodiesRLPPacket66 is the BlockBodiesRLPPacket over eth/66
type BlockBodiesRLPPacket66 struct {
	RequestId uint64
	BlockBodiesRLPPacket
}

// Unpack retrieves the transactions, uncles, withdrawals from the range packet and returns
// them in a split flat format that's more consistent with the internal data structures.
func (p *BlockRawBodiesPacket) Unpack() ([][][]byte, [][]*types.Header, []types.Withdrawals) {
	var (
		txSet         = make([][][]byte, len(*p))
		uncleSet      = make([][]*types.Header, len(*p))
		withdrawalSet = make([]types.Withdrawals, len(*p))
	)
	for i, body := range *p {
		txSet[i], uncleSet[i], withdrawalSet[i] = body.Transactions, body.Uncles, body.Withdrawals
	}
	return txSet, uncleSet, withdrawalSet
}

// GetReceiptsPacket represents a block receipts query.
type GetReceiptsPacket []common.Hash

// GetReceiptsPacket66 represents a block receipts query over eth/66.
type GetReceiptsPacket66 struct {
	RequestId uint64
	GetReceiptsPacket
}

// ReceiptsPacket is the network packet for block receipts distribution.
type ReceiptsPacket [][]*types.Receipt

// ReceiptsPacket66 is the network packet for block receipts distribution over eth/66.
type ReceiptsPacket66 struct {
	RequestId uint64
	ReceiptsPacket
}

// ReceiptsRLPPacket is used for receipts, when we already have it encoded
type ReceiptsRLPPacket []rlp.RawValue

// ReceiptsRLPPacket66 is the eth-66 version of ReceiptsRLPPacket
type ReceiptsRLPPacket66 struct {
	RequestId uint64
	ReceiptsRLPPacket
}

func (*StatusPacket) Name() string { return "Status" }
func (*StatusPacket) Kind() byte   { return StatusMsg }

func (*NewBlockHashesPacket) Name() string { return "NewBlockHashes" }
func (*NewBlockHashesPacket) Kind() byte   { return NewBlockHashesMsg }

func (*GetBlockHeadersPacket) Name() string { return "GetBlockHeaders" }
func (*GetBlockHeadersPacket) Kind() byte   { return GetBlockHeadersMsg }

func (*BlockHeadersPacket) Name() string { return "BlockHeaders" }
func (*BlockHeadersPacket) Kind() byte   { return BlockHeadersMsg }

func (*GetBlockBodiesPacket) Name() string { return "GetBlockBodies" }
func (*GetBlockBodiesPacket) Kind() byte   { return GetBlockBodiesMsg }

func (*BlockBodiesPacket) Name() string { return "BlockBodies" }
func (*BlockBodiesPacket) Kind() byte   { return BlockBodiesMsg }

func (*NewBlockPacket) Name() string { return "NewBlock" }
func (*NewBlockPacket) Kind() byte   { return NewBlockMsg }

func (*GetReceiptsPacket) Name() string { return "GetReceipts" }
func (*GetReceiptsPacket) Kind() byte   { return GetReceiptsMsg }

func (*ReceiptsPacket) Name() string { return "Receipts" }
func (*ReceiptsPacket) Kind() byte   { return ReceiptsMsg }
