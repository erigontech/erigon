package eth

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/trie"
)

// FirehoseName is the official short name of the protocol used during capability negotiation.
var FirehoseName = "firehose"

// FirehoseVersions are the supported versions of the Firehose protocol.
var FirehoseVersions = []uint{1}

// FirehoseLengths are the number of implemented message corresponding to different protocol versions.
var FirehoseLengths = []uint64{12}

// FirehoseMaxMsgSize is the maximum cap on the size of a message.
const FirehoseMaxMsgSize = 10 * 1024 * 1024

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

type accountAndHash struct {
	Account []byte      // account address or hash thereof
	Hash    common.Hash // TODO [yperbasis] potentially allow nil Hash in getBytecodeMsg
}

type keyValue struct {
	Key []byte
	Val []byte
}

type rangeEntry struct {
	Status Status
	Leaves []keyValue
}

type getStateRangesMsg struct {
	ID       uint64
	Block    common.Hash // TODO [yperbasis] change to Root Hash or change the doc
	Prefixes []trie.Keybytes
}

type stateRangesMsg struct {
	ID              uint64
	Entries         []rangeEntry
	AvailableBlocks []common.Hash
}

type getBytecodeMsg struct {
	ID  uint64
	Ref []accountAndHash
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
