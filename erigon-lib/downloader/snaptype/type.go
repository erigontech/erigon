package snaptype

import (
	"fmt"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
)

type Version uint8

func (v Version) String() string {
	return "v" + strconv.Itoa(int(v))
}

type SupportedVersions struct {
	Min Version
	Max Version
}

type Versions struct {
	Current   Version
	Supported SupportedVersions
}

type Index int

var Indexes = struct {
	Unknown,
	HeaderHash,
	TxnHash,
	TxnHash2BlockNum Index
}{
	Unknown:          -1,
	HeaderHash:       0,
	TxnHash:          1,
	TxnHash2BlockNum: 2,
}

func (i Index) Offset() int {
	switch i {
	case Indexes.TxnHash2BlockNum:
		return 1
	default:
		return 0
	}
}

func (i Index) String() string {
	switch i {
	case Indexes.HeaderHash:
		return "headers"

	default:
		panic(fmt.Sprintf("unknown index: %d", i))
	}
}

type Type interface {
	Enum() Enum
	Versions() Versions
	String() string
	FileName(version Version, from uint64, to uint64) string
	IdxFileName(version Version, from uint64, to uint64, indexNumber ...int) string
	KnownCfg(networkName string, version uint8) *snapcfg.Cfg
	IndexCount() int
}

type snapType struct {
	enum     Enum
	versions Versions
	indexes  []Index
}

func (s snapType) Enum() Enum {
	return s.enum
}

func (s snapType) IndexCount() int {
	return len(s.indexes)
}

func (s snapType) Versions() Versions {
	return s.versions
}

func (s snapType) String() string {
	return s.enum.String()
}

func (s snapType) FileName(version Version, from uint64, to uint64) string {
	if version == 0 {
		version = s.versions.Current
	}

	return SegmentFileName(version, from, to, s.enum)
}

func (s snapType) IdxFileName(version Version, from uint64, to uint64, indexNumber ...int) string {

	if len(indexNumber) == 0 {
		indexNumber = []int{0}
	}

	if indexNumber[0] > len(s.indexes) {
		return ""
	}

	return IdxFileName(version, from, to, s.indexes[indexNumber[0]].String())
}

func (s snapType) KnownCfg(networkName string, version uint8) *snapcfg.Cfg {
	return nil
}

func ParseFileType(s string) (Type, bool) {
	enum, ok := ParseEnum(s)

	if !ok {
		return nil, false
	}

	return enum.Type(), true
}

type Enum int

var Enums = struct {
	Unknown,
	Headers,
	Bodies,
	Transactions,
	BorEvents,
	BorSpans,
	BeaconBlocks Enum
}{
	Unknown:      -1,
	Headers:      0,
	Bodies:       1,
	Transactions: 2,
	BorEvents:    3,
	BorSpans:     4,
	BeaconBlocks: 5,
}

func (ft Enum) String() string {
	switch ft {
	case Enums.Headers:
		return "headers"
	case Enums.Bodies:
		return "bodies"
	case Enums.Transactions:
		return "transactions"
	case Enums.BorEvents:
		return "borevents"
	case Enums.BorSpans:
		return "borspans"
	case Enums.BeaconBlocks:
		return "beaconblocks"
	default:
		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func (ft Enum) Type() Type {
	switch ft {
	case Enums.Headers:
		return Headers
	case Enums.Bodies:
		return Bodies
	case Enums.Transactions:
		return Transactions
	case Enums.BorEvents:
		return BorEvents
	case Enums.BorSpans:
		return BorSpans
	case Enums.BeaconBlocks:
		return BeaconBlocks
	default:
		return nil
	}
}

func ParseEnum(s string) (Enum, bool) {
	switch s {
	case "headers":
		return Enums.Headers, true
	case "bodies":
		return Enums.Bodies, true
	case "transactions":
		return Enums.Transactions, true
	case "borevents":
		return Enums.BorEvents, true
	case "borspans":
		return Enums.BorSpans, true
	case "beaconblocks":
		return Enums.BeaconBlocks, true
	default:
		return Enums.Unknown, false
	}
}

var (
	Headers = snapType{
		enum: Enums.Headers,
		versions: Versions{
			Current: 2,
			Supported: SupportedVersions{
				Max: 2,
				Min: 1,
			},
		},
	}

	Bodies = snapType{
		enum: Enums.Bodies,
		versions: Versions{
			Current: 2,
			Supported: SupportedVersions{
				Max: 2,
				Min: 1,
			},
		},
	}

	Transactions = snapType{
		enum: Enums.Transactions,
		versions: Versions{
			Current: 2,
			Supported: SupportedVersions{
				Max: 2,
				Min: 1,
			},
		},
	}

	BorEvents = snapType{
		enum: Enums.BorEvents,
		versions: Versions{
			Current: 2,
			Supported: SupportedVersions{
				Max: 2,
				Min: 1,
			},
		},
	}

	BorSpans = snapType{
		enum: Enums.BorSpans,
		versions: Versions{
			Current: 2,
			Supported: SupportedVersions{
				Max: 2,
				Min: 1,
			},
		},
	}

	BeaconBlocks = snapType{
		enum: Enums.BeaconBlocks,
		versions: Versions{
			Current: 1,
			Supported: SupportedVersions{
				Max: 1,
				Min: 1,
			},
		},
	}

	BlockSnapshotTypes = []Type{Headers, Bodies, Transactions}

	BorSnapshotTypes = []Type{BorEvents, BorSpans}
)
