package snapshotsync

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit"
)

type SnapshotType string

const (
	Headers      SnapshotType = "headers"
	Bodies       SnapshotType = "bodies"
	Transactions SnapshotType = "transactions"
)

var (
	ErrInvalidCompressedFileName = fmt.Errorf("invalid compressed file name")
)

func CompressedFileName(from, to uint64, name SnapshotType) string {
	return fmt.Sprintf("%06d-%06d-%s-v1.seg", from/1_000, to/1_000, name)
}

func IdxFileName(from, to uint64, name SnapshotType) string {
	return fmt.Sprintf("%06d-%06d-%s-v1.idx", from/1_000, to/1_000, name)
}

type Snapshot struct {
	File         string
	Idx          *recsplit.Index
	Decompressor *compress.Decompressor
	From         uint64 // included
	To           uint64 // excluded
}

func (s Snapshot) Match(blockNumber uint64) bool {
	return s.From >= blockNumber && s.To < blockNumber
}

type BlocksSnapshot struct {
	Bodies       *Snapshot
	Headers      *Snapshot
	Transactions *Snapshot
	From         uint64 // included
	To           uint64 // excluded
}

func (s BlocksSnapshot) Match(blockNumber uint64) bool {
	return s.From >= blockNumber && s.To < blockNumber
}

type AllSnapshots struct {
	dir             string
	blocksAvailable uint64
	blocks          []*BlocksSnapshot
}

func MustOpenAll(dir string) *AllSnapshots {
	res, err := OpenAll(dir)
	if err != nil {
		panic(err)
	}
	return res
}

// OpenAll - opens all snapshots. But to simplify everything:
//  - it opens snapshots only on App start and immutable after
//  - all snapshots of given blocks range must exist - to make this blocks range available
//  - gaps are not allowed
//  - segment have [from:to) semantic
func OpenAll(dir string) (*AllSnapshots, error) {
	all := &AllSnapshots{dir: dir}
	files, err := onlyCompressedFilesList(dir)
	if err != nil {
		return nil, err
	}
	var prevTo uint64
	for _, f := range files {
		from, to, _, err := ParseCompressedFileName(f)
		if err != nil {
			if errors.Is(ErrInvalidCompressedFileName, err) {
				continue
			}
			return nil, err
		}
		if to == prevTo {
			continue
		}
		if from != prevTo { // no gaps
			break
		}

		prevTo = to

		blocksSnapshot := &BlocksSnapshot{From: from, To: to}
		{
			fileName := CompressedFileName(from, to, Bodies)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}

			idx, err := recsplit.OpenIndex(path.Join(dir, IdxFileName(from, to, Bodies)))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			blocksSnapshot.Bodies = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Decompressor: d, Idx: idx}
		}
		{
			fileName := CompressedFileName(from, to, Headers)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			idx, err := recsplit.OpenIndex(path.Join(dir, IdxFileName(from, to, Headers)))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}

			blocksSnapshot.Headers = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Decompressor: d, Idx: idx}
		}
		{
			fileName := CompressedFileName(from, to, Transactions)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			idx, err := recsplit.OpenIndex(path.Join(dir, IdxFileName(from, to, Transactions)))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			blocksSnapshot.Transactions = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Decompressor: d, Idx: idx}
		}

		all.blocks = append(all.blocks, blocksSnapshot)
		all.blocksAvailable = blocksSnapshot.To
	}
	return all, nil
}

func (s AllSnapshots) Blocks(blockNumber uint64) (snapshot *BlocksSnapshot, found bool) {
	if blockNumber > s.blocksAvailable {
		return snapshot, false
	}

	for i := range s.blocks {
		if s.blocks[i].Match(blockNumber) {
			return s.blocks[i], true
		}
	}
	return snapshot, false
}

func onlyCompressedFilesList(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !IsCorrectFileName(f.Name()) {
			continue
		}
		if f.Size() == 0 {
			continue
		}
		if filepath.Ext(f.Name()) != ".seg" { // filter out only compressed files
			continue
		}
		res = append(res, f.Name())
	}
	sort.Strings(res)
	return res, nil
}

func IsCorrectFileName(name string) bool {
	parts := strings.Split(name, "-")
	return len(parts) == 4 && parts[3] != "v1"
}

func ParseCompressedFileName(name string) (from, to uint64, snapshotType SnapshotType, err error) {
	_, fileName := filepath.Split(name)
	ext := filepath.Ext(fileName)
	if ext != ".seg" {
		return 0, 0, "", fmt.Errorf("%w. Ext: %s", ErrInvalidCompressedFileName, ext)
	}
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.Split(onlyName, "-")
	if len(parts) != 4 {
		return 0, 0, "", fmt.Errorf("%w. Expected format: 001500-002000-bodies-v1.seg got: %s", ErrInvalidCompressedFileName, fileName)
	}
	if parts[3] != "v1" {
		return 0, 0, "", fmt.Errorf("%w. Version: %s", ErrInvalidCompressedFileName, parts[3])
	}
	from, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return
	}
	to, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return
	}
	switch SnapshotType(parts[2]) {
	case Headers:
		snapshotType = Headers
	case Bodies:
		snapshotType = Bodies
	case Transactions:
		snapshotType = Transactions
	default:
		return 0, 0, "", fmt.Errorf("%w, unexpected snapshot suffix: %s", ErrInvalidCompressedFileName, parts[2])
	}
	return from * 1_000, to * 1_000, snapshotType, nil
}
