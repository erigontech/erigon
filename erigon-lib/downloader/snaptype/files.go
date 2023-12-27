/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package snaptype

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"golang.org/x/exp/slices"
)

type Type int

const (
	Unknown Type = -1
	Headers Type = iota
	Bodies
	Transactions
	BorEvents
	BorSpans
	BeaconBlocks
)

func (ft Type) String() string {
	switch ft {
	case Headers:
		return "headers"
	case Bodies:
		return "bodies"
	case Transactions:
		return "transactions"
	case BorEvents:
		return "borevents"
	case BorSpans:
		return "borspans"
	case BeaconBlocks:
		return "beaconblocks"
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
	case "borevents":
		return BorEvents, true
	case "borspans":
		return BorSpans, true
	case "beaconblocks":
		return BeaconBlocks, true
	default:
		return Unknown, false
	}
}

type IdxType string

const (
	Transactions2Block IdxType = "transactions-to-block"
)

func (it IdxType) String() string { return string(it) }

var BlockSnapshotTypes = []Type{Headers, Bodies, Transactions}

var BorSnapshotTypes = []Type{BorEvents, BorSpans}

var (
	ErrInvalidFileName = fmt.Errorf("invalid compressed file name")
)

func FileName(version uint8, from, to uint64, fileType string) string {
	return fmt.Sprintf("v%d-%06d-%06d-%s", version, from/1_000, to/1_000, fileType)
}

func SegmentFileName(version uint8, from, to uint64, t Type) string {
	return FileName(version, from, to, t.String()) + ".seg"
}
func DatFileName(version uint8, from, to uint64, fType string) string {
	return FileName(version, from, to, fType) + ".dat"
}
func IdxFileName(version uint8, from, to uint64, fType string) string {
	return FileName(version, from, to, fType) + ".idx"
}

func FilterExt(in []FileInfo, expectExt string) (out []FileInfo) {
	for _, f := range in {
		if f.Ext != expectExt { // filter out only compressed files
			continue
		}
		out = append(out, f)
	}
	return out
}
func FilesWithExt(dir string, version uint8, expectExt string) ([]FileInfo, error) {
	files, err := ParseDir(dir, version)
	if err != nil {
		return nil, err
	}
	return FilterExt(files, expectExt), nil
}

func IsCorrectFileName(name string) bool {
	parts := strings.Split(name, "-")
	return len(parts) == 4
}

func IsCorrectHistoryFileName(name string) bool {
	parts := strings.Split(name, ".")
	return len(parts) == 3
}

func ParseFileName(dir, fileName string) (res FileInfo, ok bool) {
	ext := filepath.Ext(fileName)
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.Split(onlyName, "-")
	if len(parts) < 4 {
		return res, ok
	}

	var version uint8
	if len(parts[0]) > 1 && parts[0][0] == 'v' {
		v, err := strconv.ParseUint(parts[0][1:], 10, 64)
		if err != nil {
			return
		}
		version = uint8(v)
	}

	from, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return
	}
	to, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return
	}
	ft, ok := ParseFileType(parts[3])
	if !ok {
		return res, ok
	}

	return FileInfo{Version: version, From: from * 1_000, To: to * 1_000, Path: filepath.Join(dir, fileName), T: ft, Ext: ext}, ok
}

const Erigon3SeedableSteps = 32

// Use-cases:
//   - produce and seed snapshots earlier on chain tip. reduce depnedency on "good peers with history" at p2p-network.
//     Some networks have no much archive peers, also ConsensusLayer clients are not-good(not-incentivised) at serving history.
//   - avoiding having too much files:
//     more files(shards) - means "more metadata", "more lookups for non-indexed queries", "more dictionaries", "more bittorrent connections", ...
//     less files - means small files will be removed after merge (no peers for this files).
const Erigon2OldMergeLimit = 500_000
const Erigon2MergeLimit = 100_000
const Erigon2MinSegmentSize = 1_000

var MergeSteps = []uint64{100_000, 10_000}

// FileInfo - parsed file metadata
type FileInfo struct {
	Version   uint8
	From, To  uint64
	Path, Ext string
	T         Type
}

func (f FileInfo) TorrentFileExists() bool { return dir.FileExist(f.Path + ".torrent") }
func (f FileInfo) Seedable() bool {
	return f.To-f.From == Erigon2MergeLimit || f.To-f.From == Erigon2OldMergeLimit
}
func (f FileInfo) NeedTorrentFile() bool { return f.Seedable() && !f.TorrentFileExists() }
func (f FileInfo) Name() string          { return filepath.Base(f.Path) }

func IdxFiles(dir string, version uint8) (res []FileInfo, err error) {
	return FilesWithExt(dir, version, ".idx")
}
func Segments(dir string, version uint8) (res []FileInfo, err error) {
	return FilesWithExt(dir, version, ".seg")
}
func TmpFiles(dir string, version uint8) (res []string, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}
		return nil, err
	}

	v := fmt.Sprint("v", version)

	for _, f := range files {
		if f.IsDir() || len(f.Name()) < 3 || !strings.HasPrefix(f.Name(), v) {
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
func ParseDir(dir string, version uint8) (res []FileInfo, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []FileInfo{}, nil
		}
		return nil, err
	}

	v := fmt.Sprint("v", version)

	for _, f := range files {
		fileInfo, err := f.Info()
		if err != nil {
			return nil, err
		}
		if f.IsDir() || fileInfo.Size() == 0 || len(f.Name()) < 3 || !strings.HasPrefix(f.Name(), v) {
			continue
		}

		meta, ok := ParseFileName(dir, f.Name())
		if !ok {
			continue
		}
		res = append(res, meta)
	}
	slices.SortFunc(res, func(i, j FileInfo) int {
		if i.Version != j.Version {
			return cmp.Compare(i.Version, j.Version)
		}
		if i.From != j.From {
			return cmp.Compare(i.From, j.From)
		}
		if i.To != j.To {
			return cmp.Compare(i.To, j.To)
		}
		if i.T != j.T {
			return cmp.Compare(i.T, j.T)
		}
		return cmp.Compare(i.Ext, j.Ext)
	})

	return res, nil
}

func Hex2InfoHash(in string) (infoHash metainfo.Hash) {
	inHex, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	copy(infoHash[:], inHex)
	return infoHash
}
