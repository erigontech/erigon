package snaptype

import (
	"fmt"
	"strconv"
	"strings"
)

type Version uint8

func ParseVersion(v string) (Version, error) {
	if strings.HasPrefix(v, "v") {
		v, err := strconv.ParseUint(v[1:], 10, 8)

		if err != nil {
			return 0, fmt.Errorf("invalid version: %w", err)
		}

		return Version(v), nil
	}

	if len(v) == 0 {
		return 0, fmt.Errorf("invalid version: no prefix")
	}

	return 0, fmt.Errorf("invalid version prefix: %s", v[0:1])
}

func (v Version) String() string {
	return "v" + strconv.Itoa(int(v))
}

type Versions struct {
	Current      Version
	MinSupported Version
}

type Index int

var Indexes = struct {
	Unknown,
	HeaderHash,
	BodyHash,
	TxnHash,
	TxnHash2BlockNum,
	BorTxnHash,
	BorSpanId,
	BeaconBlockSlot Index
}{
	Unknown:          -1,
	HeaderHash:       0,
	BodyHash:         1,
	TxnHash:          2,
	TxnHash2BlockNum: 3,
	BorTxnHash:       4,
	BorSpanId:        5,
	BeaconBlockSlot:  6,
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
		return Enums.Headers.String()
	case Indexes.BodyHash:
		return Enums.Bodies.String()
	case Indexes.TxnHash:
		return Enums.Transactions.String()
	case Indexes.TxnHash2BlockNum:
		return "transactions-to-block"
	case Indexes.BorTxnHash:
		return Enums.BorEvents.String()
	case Indexes.BorSpanId:
		return Enums.BorSpans.String()
	case Indexes.BeaconBlockSlot:
		return Enums.BeaconBlocks.String()
	default:
		panic(fmt.Sprintf("unknown index: %d", i))
	}
}

type Type interface {
	Enum() Enum
	Versions() Versions
	String() string
	FileName(version Version, from uint64, to uint64) string
	FileInfo(dir string, from uint64, to uint64) FileInfo
	IdxFileName(version Version, from uint64, to uint64, index ...Index) string
	IdxFileNames(version Version, from uint64, to uint64) []string
	Indexes() []Index
}

type snapType struct {
	enum     Enum
	versions Versions
	indexes  []Index
}

func (s snapType) Enum() Enum {
	return s.enum
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

func (s snapType) FileInfo(dir string, from uint64, to uint64) FileInfo {
	f, _ := ParseFileName(dir, s.FileName(s.versions.Current, from, to))
	return f
}

func (s snapType) Indexes() []Index {
	return s.indexes
}

func (s snapType) IdxFileNames(version Version, from uint64, to uint64) []string {
	fileNames := make([]string, len(s.indexes))
	for i, index := range s.indexes {
		fileNames[i] = IdxFileName(version, from, to, index.String())
	}

	return fileNames
}

func (s snapType) IdxFileName(version Version, from uint64, to uint64, index ...Index) string {

	if len(index) == 0 {
		if len(s.indexes) == 0 {
			return ""
		}

		index = []Index{s.indexes[0]}
	} else {
		i := index[0]
		found := false

		for _, index := range s.indexes {
			if i == index {
				found = true
				break
			}
		}

		if !found {
			return ""
		}
	}

	return IdxFileName(version, from, to, index[0].String())
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

func (e Enum) FileName(from uint64, to uint64) string {
	return SegmentFileName(e.Type().Versions().Current, from, to, e)
}

func (e Enum) FileInfo(dir string, from uint64, to uint64) FileInfo {
	f, _ := ParseFileName(dir, e.FileName(from, to))
	return f
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
			Current:      1, //2,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.HeaderHash},
	}

	Bodies = snapType{
		enum: Enums.Bodies,
		versions: Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.BodyHash},
	}

	Transactions = snapType{
		enum: Enums.Transactions,
		versions: Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.TxnHash, Indexes.TxnHash2BlockNum},
	}

	BorEvents = snapType{
		enum: Enums.BorEvents,
		versions: Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.BorTxnHash},
	}

	BorSpans = snapType{
		enum: Enums.BorSpans,
		versions: Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.BorSpanId},
	}

	BeaconBlocks = snapType{
		enum: Enums.BeaconBlocks,
		versions: Versions{
			Current:      1,
			MinSupported: 1,
		},
		indexes: []Index{Indexes.BeaconBlockSlot},
	}

	BlockSnapshotTypes = []Type{Headers, Bodies, Transactions}

	BorSnapshotTypes = []Type{BorEvents, BorSpans}

	CaplinSnapshotTypes = []Type{BeaconBlocks}

	AllTypes = []Type{
		Headers,
		Bodies,
		Transactions,
		BorEvents,
		BorSpans,
		BeaconBlocks,
	}
)
