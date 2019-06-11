package eth

// FirehoseName is the official short name of the protocol used during capability negotiation.
var FirehoseName = "firehose"

// FirehoseVersions are the supported versions of the Firehose protocol.
var FirehoseVersions = []uint{1}

// FirehoseLengths are the number of implemented message corresponding to different protocol versions.
var FirehoseLengths = []uint64{12}

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
