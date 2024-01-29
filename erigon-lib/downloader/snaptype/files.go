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

var (
	ErrInvalidFileName = fmt.Errorf("invalid compressed file name")
)

func FileName(version Version, from, to uint64, fileType string) string {
	return fmt.Sprintf("v%d-%06d-%06d-%s", version, from/1_000, to/1_000, fileType)
}

func SegmentFileName(version Version, from, to uint64, t Enum) string {
	return FileName(version, from, to, t.String()) + ".seg"
}
func DatFileName(version Version, from, to uint64, fType string) string {
	return FileName(version, from, to, fType) + ".dat"
}
func IdxFileName(version Version, from, to uint64, fType string) string {
	return FileName(version, from, to, fType) + ".idx"
}

func FilterExt(in []FileInfo, expectExt string) (out []FileInfo) {
	for _, f := range in {
		if f.Ext != expectExt { // filter out only compressed files
			continue
		}
		out = append(out, f)
	}

	slices.SortFunc(out, func(a, b FileInfo) int {
		if cmp := strings.Compare(a.Type.String(), b.Type.String()); cmp != 0 {
			return cmp
		}

		switch {
		case a.From > b.From:
			return +1
		case b.From > a.From:
			return -1
		}

		switch {
		case a.To > b.To:
			return +1
		case b.To > a.To:
			return -1
		}

		return int(a.Version) - int(b.Version)
	})

	return out
}
func FilesWithExt(dir string, expectExt string) ([]FileInfo, error) {
	files, err := ParseDir(dir)
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

	version, err := ParseVersion(parts[0])
	if err != nil {
		return
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

	return FileInfo{Version: version, From: from * 1_000, To: to * 1_000, Path: filepath.Join(dir, fileName), Type: ft, Ext: ext}, ok
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
	Version   Version
	From, To  uint64
	Path, Ext string
	Type      Type
}

func (f FileInfo) TorrentFileExists() bool { return dir.FileExist(f.Path + ".torrent") }

func (f FileInfo) Name() string {
	return fmt.Sprintf("v%d-%06d-%06d-%s%s", f.Version, f.From/1_000, f.To/1_000, f.Type, f.Ext)
}
func (f FileInfo) Dir() string { return filepath.Dir(f.Path) }
func (f FileInfo) Len() uint64 { return f.To - f.From }

func (f FileInfo) As(t Type) FileInfo {
	as := FileInfo{
		Version: f.Version,
		From:    f.From,
		To:      f.To,
		Ext:     f.Ext,
		Type:    t,
	}

	as.Path = filepath.Join(f.Dir(), as.Name())

	return as
}

func IdxFiles(dir string) (res []FileInfo, err error) {
	return FilesWithExt(dir, ".idx")
}

func Segments(dir string) (res []FileInfo, err error) {
	return FilesWithExt(dir, ".seg")
}

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
		if i.Type.Enum() != j.Type.Enum() {
			return cmp.Compare(i.Type.Enum(), j.Type.Enum())
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
