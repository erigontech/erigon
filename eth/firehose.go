package eth

import (
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

// FirehoseName is the official short name of the protocol used during capability negotiation.
var FirehoseName = "frh" // Parity only supports 3 letter capabilities

// FirehoseVersions are the supported versions of the Firehose protocol.
var FirehoseVersions = []uint{1}

// FirehoseLengths are the number of implemented message corresponding to different protocol versions.
var FirehoseLengths = []uint64{12}

// FirehoseMaxMsgSize is the maximum cap on the size of a message.
const FirehoseMaxMsgSize = 10 * 1024 * 1024

// MaxLeavesPerPrefix is the maximum number of leaves allowed per prefix.
const MaxLeavesPerPrefix = 4096

// Firehose protocol message codes
const (
	GetStateRangesCode   = 0x00
	StateRangesCode      = 0x01
	GetStorageRangesCode = 0x02
	StorageRangesCode    = 0x03
	GetStateNodesCode    = 0x04
	StateNodesCode       = 0x05
	GetStorageNodesCode  = 0x06
	StorageNodesCode     = 0x07
	GetBytecodeCode      = 0x08
	BytecodeCode         = 0x09
	GetStorageSizesCode  = 0x0a
	StorageSizesCode     = 0x0b
)

// Status of Firehose results.
type Status uint

const (
	// OK means success.
	OK Status = 0
	// NoData for the requested root; available blocks should be returned.
	NoData Status = 1
	// TooManyLeaves means that there're more than 4096 leaves matching the prefix.
	TooManyLeaves Status = 2
)

type firehosePeer struct {
	*p2p.Peer
	rw p2p.MsgReadWriter
}

type accountLeaf struct {
	Key common.Hash
	Val *accounts.Account
}

type firehoseAccountRange struct {
	Status Status
	Leaves []accountLeaf
}

type getStateRangesOrNodes struct {
	ID       uint64
	Block    common.Hash
	Prefixes []trie.Keybytes
}

type stateRangesMsg struct {
	ID              uint64
	Entries         []firehoseAccountRange
	AvailableBlocks []common.Hash
}

type storageReqForOneAccount struct {
	Account  []byte // account address or hash thereof
	Prefixes []trie.Keybytes
}

type getStorageRangesOrNodes struct {
	ID       uint64
	Block    common.Hash
	Requests []storageReqForOneAccount
}

type storageLeaf struct {
	Key common.Hash
	Val big.Int
}

type storageRange struct {
	Status Status
	Leaves []storageLeaf
}

type storageRangesMsg struct {
	ID              uint64
	Entries         [][]storageRange
	AvailableBlocks []common.Hash
}

type stateNodesMsg struct {
	ID              uint64
	Nodes           [][]byte
	AvailableBlocks []common.Hash
}

type storageNodesMsg struct {
	ID              uint64
	Nodes           [][][]byte // indexing matches getStorageRangesOrNodes request: [#account/contract][#prefix][RLP]
	AvailableBlocks []common.Hash
}

type bytecodeRef struct {
	Account  []byte // account address or hash thereof
	CodeHash common.Hash
}

type getBytecodeMsg struct {
	ID  uint64
	Ref []bytecodeRef
}

type bytecodeMsg struct {
	ID   uint64
	Code [][]byte
}

// SendByteCode sends a BytecodeCode message.
func (p *firehosePeer) SendByteCode(id uint64, data [][]byte) error {
	msg := bytecodeMsg{ID: id, Code: data}
	return p2p.Send(p.rw, BytecodeCode, msg)
}
