package snap

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"golang.org/x/exp/slices"
)

var Erigon21Extensions = []string{".seg", ".idx", ".dat"}
var Erigon22Extensions = []string{".ef", ".efi", ".v", ".vi"}

type Type string

const (
	Headers      Type = "headers"
	Bodies       Type = "bodies"
	Transactions Type = "transactions"
)

type IdxType string

const (
	Transactions2Block IdxType = "transactions-to-block"
)

func (it IdxType) String() string { return string(it) }

var AllErigon21SnapshotTypes = []Type{Headers, Bodies, Transactions}

var (
	ErrInvalidFileName = fmt.Errorf("invalid compressed file name")
)

func FileName(from, to uint64, fileType string) string {
	return fmt.Sprintf("v1-%06d-%06d-%s", from/1_000, to/1_000, fileType)
}
func SegmentFileName(from, to uint64, t Type) string { return FileName(from, to, string(t)) + ".seg" }
func DatFileName(from, to uint64, t Type) string     { return FileName(from, to, string(t)) + ".dat" }
func IdxFileName(from, to uint64, t string) string   { return FileName(from, to, t) + ".idx" }

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

func parseFileName22(dir, fileName string) (res FileInfo, err error) {
	re := regexp.MustCompile("([[:lower:]]).([0-9]+)-([0-9]+).(v|ef)")
	subs := re.FindStringSubmatch(fileName)
	if len(subs) != 4 {
		return res, fmt.Errorf("expected format: 'storage.0-1.v' got: %s. %w", fileName, ErrInvalidFileName)
	}
	from, err := strconv.ParseUint(subs[1], 10, 64)
	if err != nil {
		return res, fmt.Errorf("parsing startTxNum: %s. %w: %s", fileName, ErrInvalidFileName, err)
	}
	to, err := strconv.ParseUint(subs[2], 10, 64)
	if err != nil {
		return res, fmt.Errorf("parsing endTxNum: %s. %w: %s", fileName, ErrInvalidFileName, err)
	}
	if from > to {
		return res, fmt.Errorf("from > to: %s. %w", fileName, ErrInvalidFileName)
	}
	ext := filepath.Ext(fileName)
	return FileInfo{From: from * 1_000, To: to * 1_000, Path: filepath.Join(dir, fileName), T: Type(subs[0]), Ext: ext}, nil
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
	switch parts[3] {
	case string(Headers):
	case string(Bodies):
	case string(Transactions):
	default:
		return res, fmt.Errorf("unexpected snapshot suffix: %s,%w", parts[2], ErrInvalidFileName)
	}
	return FileInfo{From: from * 1_000, To: to * 1_000, Path: filepath.Join(dir, fileName), T: Type(parts[3]), Ext: ext}, nil
}

const Erigon21SegmentSize = 500_000
const Erigon21MinSegmentSize = 1_000

// FileInfo - parsed file metadata
type FileInfo struct {
	_         fs.FileInfo
	Version   uint8
	From, To  uint64
	Path, Ext string
	T         Type
}

func (f FileInfo) TorrentFileExists() bool { return common.FileExist(f.Path + ".torrent") }
func (f FileInfo) Seedable() bool {
	return (f.Ext == ".seg" && f.To-f.From == Erigon21SegmentSize) ||
		(f.Ext == ".v" && f.To-f.From == 32) ||
		(f.Ext == ".ef" && f.To-f.From == 32)
}
func (f FileInfo) NeedTorrentFile() bool { return f.Seedable() && !f.TorrentFileExists() }

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
				idxPath := IdxFileName(f.From, f.To, string(Transactions2Block))
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
