package wit

import (
	"errors"

	"github.com/erigontech/erigon-lib/common"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/core/stateless"
	"github.com/erigontech/erigon/node/direct"
)

var ProtocolToString = map[uint]string{
	direct.WIT0: "wit0",
}

// Constants to match up protocol versions and messages
const (
	WIT1 = 1
)

// ProtocolName is the official short name of the `wit` protocol used during
// devp2p capability negotiation.
const ProtocolName = "wit"

// ProtocolVersions are the supported versions of the `wit` protocol (first
// is primary).
var ProtocolVersions = []uint{WIT1}

// ProtocolLengths are the number of implemented message corresponding to
// different protocol versions.
var ProtocolLengths = map[uint]uint64{WIT1: 4}

// MaxMessageSize is the maximum cap on the size of a protocol message.
const MaxMessageSize = 16 * 1024 * 1024

// Witness Response constants
const (
	PageSize                       = 15 * 1024 * 1024  // 15 MB
	MaximumCachedWitnessOnARequest = 200 * 1024 * 1024 // 200 MB, the maximum amount of memory a request can demand while getting witness
	MaximumResponseSize            = 16 * 1024 * 1024  // 16 MB, helps to fast fail check

	// RetentionBlocks defines how many recent blocks to keep witness data for
	RetentionBlocks = 10_000
)

const (
	NewWitnessMsg       = 0x00 // sends witness hash
	NewWitnessHashesMsg = 0x01 // announces witness availability
	GetWitnessMsg       = 0x02 // witness request
	WitnessMsg          = 0x03 // witness response
)

var (
	errNoStatusMsg             = errors.New("no status message")
	errMsgTooLarge             = errors.New("message too long")
	errDecode                  = errors.New("invalid message")
	errInvalidMsgCode          = errors.New("invalid message code")
	errProtocolVersionMismatch = errors.New("protocol version mismatch")
	errNetworkIDMismatch       = errors.New("network ID mismatch")
	errGenesisMismatch         = errors.New("genesis mismatch")
	errForkIDRejected          = errors.New("fork ID rejected")
)

// Packet represents a p2p message in the `wit` protocol.
type Packet interface {
	Name() string // Name returns a string corresponding to the message type.
	Kind() byte   // Kind returns the message type.
}

// GetWitnessRequest represents a list of witnesses query by witness pages.
type GetWitnessRequest struct {
	WitnessPages []WitnessPageRequest // Request by list of witness pages
}

type WitnessPageRequest struct {
	Hash common.Hash // BlockHash
	Page uint64      // Starts on 0
}

// GetWitnessPacket represents a witness query with request ID wrapping.
type GetWitnessPacket struct {
	RequestId uint64
	*GetWitnessRequest
}

// WitnessPacketRLPPacket represents a witness response with request ID wrapping.
type WitnessPacketRLPPacket struct {
	RequestId uint64
	WitnessPacketResponse
}

// WitnessPacketResponse represents a witness response, to use when we already
// have the witness rlp encoded.
type WitnessPacketResponse []WitnessPageResponse

type WitnessPageResponse struct {
	Data       []byte
	Hash       common.Hash
	Page       uint64 // Starts on 0; If Page >= TotalPages means the request was invalid and the response is an empty data array
	TotalPages uint64 // Length of pages
}

type NewWitnessPacket struct {
	Witness *stateless.Witness
}

type NewWitnessHashesPacket struct {
	Hashes  []common.Hash
	Numbers []uint64
}

func (w *GetWitnessRequest) Name() string { return "GetWitness" }
func (w *GetWitnessRequest) Kind() byte   { return GetWitnessMsg }

func (*WitnessPacketRLPPacket) Name() string { return "Witness" }
func (*WitnessPacketRLPPacket) Kind() byte   { return WitnessMsg }

func (w *NewWitnessPacket) Name() string { return "NewWitness" }
func (w *NewWitnessPacket) Kind() byte   { return NewWitnessMsg }

func (w *NewWitnessHashesPacket) Name() string { return "NewWitnessHashes" }
func (w *NewWitnessHashesPacket) Kind() byte   { return NewWitnessHashesMsg }

var ToProto = map[uint]map[uint64]proto_sentry.MessageId{
	direct.WIT0: {
		NewWitnessMsg:       proto_sentry.MessageId_NEW_WITNESS_W0,
		NewWitnessHashesMsg: proto_sentry.MessageId_NEW_WITNESS_HASHES_W0,
		GetWitnessMsg:       proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
		WitnessMsg:          proto_sentry.MessageId_BLOCK_WITNESS_W0,
	},
}

var FromProto = map[uint]map[proto_sentry.MessageId]uint64{
	direct.WIT0: {
		proto_sentry.MessageId_NEW_WITNESS_W0:        NewWitnessMsg,
		proto_sentry.MessageId_NEW_WITNESS_HASHES_W0: NewWitnessHashesMsg,
		proto_sentry.MessageId_GET_BLOCK_WITNESS_W0:  GetWitnessMsg,
		proto_sentry.MessageId_BLOCK_WITNESS_W0:      WitnessMsg,
	},
}
