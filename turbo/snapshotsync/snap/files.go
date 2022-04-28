package snap

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type Type int

const (
	Headers Type = iota
	Bodies
	Transactions
	NumberOfTypes
)

func (ft Type) String() string {
	switch ft {
	case Headers:
		return "headers"
	case Bodies:
		return "bodies"
	case Transactions:
		return "transactions"
	default:
		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func ParseFileType(s string) (Type, bool) {
	switch s {
	case "headers":
		return Headers, true
	case "bodies":
		return Bodies, true
	case "transactions":
		return Transactions, true
	default:
		return NumberOfTypes, false
	}
}

type IdxType string

const (
	Transactions2Block IdxType = "transactions-to-block"
)

func (it IdxType) String() string { return string(it) }

var AllSnapshotTypes = []Type{Headers, Bodies, Transactions}

var (
	ErrInvalidFileName = fmt.Errorf("invalid compressed file name")
)

func FileName(from, to uint64, fileType string) string {
	return fmt.Sprintf("v1-%06d-%06d-%s", from/1_000, to/1_000, fileType)
}
func SegmentFileName(from, to uint64, t Type) string   { return FileName(from, to, t.String()) + ".seg" }
func DatFileName(from, to uint64, fType string) string { return FileName(from, to, fType) + ".dat" }
func IdxFileName(from, to uint64, fType string) string { return FileName(from, to, fType) + ".idx" }

func FilterExt(in []snapshotsync.FileInfo, expectExt string) (out []snapshotsync.FileInfo) {
	for _, f := range in {
		if f.Ext != expectExt { // filter out only compressed files
			continue
		}
		out = append(out, f)
	}
	return out
}
func FilesWithExt(dir, expectExt string) ([]snapshotsync.FileInfo, error) {
	files, err := snapshotsync.ParseDir(dir)
	if err != nil {
		return nil, err
	}
	return FilterExt(files, expectExt), nil
}

func IsCorrectFileName(name string) bool {
	parts := strings.Split(name, "-")
	return len(parts) == 4 && parts[3] != "v1"
}

func ParseFileName(dir, fileName string) (res snapshotsync.FileInfo, err error) {
	ext := filepath.Ext(fileName)
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.Split(onlyName, "-")
	if len(parts) < 4 {
		return res, fmt.Errorf("expected format: v1-001500-002000-bodies.seg got: %s. %w", fileName, ErrInvalidFileName)
	}
	if parts[0] != "v1" {
		return res, fmt.Errorf("version: %s. %w", parts[0], ErrInvalidFileName)
	}
	from, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return
	}
	to, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return
	}
	var snapshotType Type
	ft, ok := ParseFileType(parts[3])
	if !ok {
		return res, fmt.Errorf("unexpected snapshot suffix: %s,%w", parts[2], ErrInvalidFileName)
	}
	switch ft {
	case Headers:
		snapshotType = Headers
	case Bodies:
		snapshotType = Bodies
	case Transactions:
		snapshotType = Transactions
	default:
		return res, fmt.Errorf("unexpected snapshot suffix: %s,%w", parts[2], ErrInvalidFileName)
	}
	return snapshotsync.FileInfo{From: from * 1_000, To: to * 1_000, Path: filepath.Join(dir, fileName), T: snapshotType, Ext: ext}, nil
}

const MERGE_THRESHOLD = 2 // don't trigger merge if have too small amount of partial segments
const DEFAULT_SEGMENT_SIZE = 500_000
const MIN_SEGMENT_SIZE = 1_000
