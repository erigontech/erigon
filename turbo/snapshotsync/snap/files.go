package snap

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"golang.org/x/exp/slices"
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

func FilterExt(in []FileInfo, expectExt string) (out []FileInfo) {
	for _, f := range in {
		if f.Ext != expectExt { // filter out only compressed files
			continue
		}
		out = append(out, f)
	}
	return out
}
func FilesWithExt(dir, expectExt string) ([]FileInfo, error) {
	files, err := ParseDir(dir)
	if err != nil {
		return nil, err
	}
	return FilterExt(files, expectExt), nil
}

func IsCorrectFileName(name string) bool {
	parts := strings.Split(name, "-")
	return len(parts) == 4 && parts[3] != "v1"
}

func ParseFileName(dir, fileName string) (res FileInfo, err error) {
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
	return FileInfo{From: from * 1_000, To: to * 1_000, Path: filepath.Join(dir, fileName), T: snapshotType, Ext: ext}, nil
}

const MERGE_THRESHOLD = 2 // don't trigger merge if have too small amount of partial segments
const DEFAULT_SEGMENT_SIZE = 500_000
const MIN_SEGMENT_SIZE = 1_000

// FileInfo - parsed file metadata
type FileInfo struct {
	_         fs.FileInfo
	Version   uint8
	From, To  uint64
	Path, Ext string
	T         Type
}

func (f FileInfo) TorrentFileExists() bool { return common.FileExist(f.Path + ".torrent") }
func (f FileInfo) Seedable() bool          { return f.To-f.From == DEFAULT_SEGMENT_SIZE }
func (f FileInfo) NeedTorrentFile() bool   { return f.Seedable() && !f.TorrentFileExists() }

func IdxFiles(dir string) (res []FileInfo, err error) { return FilesWithExt(dir, ".idx") }
func Segments(dir string) (res []FileInfo, err error) { return FilesWithExt(dir, ".seg") }
func TmpFiles(dir string) (res []string, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || len(f.Name()) < 3 {
			continue
		}
		if filepath.Ext(f.Name()) != ".tmp" {
			continue
		}
		res = append(res, filepath.Join(dir, f.Name()))
	}
	return res, nil
}

// ParseDir - reading dir (
func ParseDir(dir string) (res []FileInfo, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []FileInfo{}, nil
		}
		return nil, err
	}
	for _, f := range files {
		fileInfo, err := f.Info()
		if err != nil {
			return nil, err
		}
		if f.IsDir() || fileInfo.Size() == 0 || len(f.Name()) < 3 {
			continue
		}

		meta, err := ParseFileName(dir, f.Name())
		if err != nil {
			if errors.Is(err, ErrInvalidFileName) {
				continue
			}
			return nil, err
		}
		res = append(res, meta)
	}
	slices.SortFunc(res, func(i, j FileInfo) bool {
		if i.Version != j.Version {
			return i.Version < j.Version
		}
		if i.From != j.From {
			return i.From < j.From
		}
		if i.To != j.To {
			return i.To < j.To
		}
		if i.T != j.T {
			return i.T < j.T
		}
		return i.Ext < j.Ext
	})

	return res, nil
}

func RemoveNonPreverifiedFiles(chainName, snapDir string) error {
	preverified := snapcfg.KnownCfg(chainName, nil).Preverified
	keep := map[string]struct{}{}
	for _, p := range preverified {
		ext := filepath.Ext(p.Name)
		withoutExt := p.Name[0 : len(p.Name)-len(ext)]
		keep[withoutExt] = struct{}{}
	}
	list, err := Segments(snapDir)
	if err != nil {
		return err
	}
	for _, f := range list {
		_, fname := filepath.Split(f.Path)
		ext := filepath.Ext(fname)
		withoutExt := fname[0 : len(fname)-len(ext)]
		if _, ok := keep[withoutExt]; !ok {
			_ = os.Remove(f.Path)
			_ = os.Remove(f.Path + ".torrent")
		} else {
			if f.T == Transactions {
				idxPath := IdxFileName(f.From, f.To, Transactions2Block.String())
				idxExt := filepath.Ext(idxPath)
				keep[idxPath[0:len(idxPath)-len(idxExt)]] = struct{}{}
			}
		}
	}
	list, err = IdxFiles(snapDir)
	if err != nil {
		return err
	}
	for _, f := range list {
		_, fname := filepath.Split(f.Path)
		ext := filepath.Ext(fname)
		withoutExt := fname[0 : len(fname)-len(ext)]
		if _, ok := keep[withoutExt]; !ok {
			_ = os.Remove(f.Path)
		}
	}
	return nil
}
