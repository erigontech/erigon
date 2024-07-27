// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package snaptype

import (
	"cmp"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon-lib/common/dir"
)

var (
	ErrInvalidFileName = errors.New("invalid compressed file name")
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
		if cmp := strings.Compare(a.Type.Name(), b.Type.Name()); cmp != 0 {
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

func ParseFileName(dir, fileName string) (res FileInfo, isE3Seedable bool, ok bool) {
	res, ok = parseFileName(dir, fileName)
	if ok {
		return res, false, true
	}
	isStateFile := IsStateFile(fileName)
	res.name = fileName
	res.Path = filepath.Join(dir, fileName)

	if res.From == 0 && res.To == 0 {
		parts := strings.Split(fileName, ".")
		if len(parts) == 3 || len(parts) == 4 {
			fsteps := strings.Split(parts[1], "-")
			if len(fsteps) == 2 {
				if from, err := strconv.ParseUint(fsteps[0], 10, 64); err == nil {
					res.From = from
				}
				if to, err := strconv.ParseUint(fsteps[1], 10, 64); err == nil {
					res.To = to
				}
			}
		}
	}
	return res, isStateFile, isStateFile
}

func parseFileName(dir, fileName string) (res FileInfo, ok bool) {
	ext := filepath.Ext(fileName)
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.Split(onlyName, "-")
	res = FileInfo{Path: filepath.Join(dir, fileName), name: fileName, Ext: ext}
	if len(parts) < 4 {
		return res, ok
	}

	var err error
	res.Version, err = ParseVersion(parts[0])
	if err != nil {
		return
	}

	from, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return
	}
	res.From = from * 1_000
	to, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return
	}
	res.To = to * 1_000
	res.Type, ok = ParseFileType(parts[3])
	if !ok {
		return res, ok
	}
	return res, ok
}

var stateFileRegex = regexp.MustCompile("^v([0-9]+)-([[:lower:]]+).([0-9]+)-([0-9]+).(.*)$")

func E3Seedable(name string) bool {
	_, name = filepath.Split(name) // remove absolute path, or `history/` prefixes
	subs := stateFileRegex.FindStringSubmatch(name)
	if len(subs) != 6 {
		return false
	}
	// Check that it's seedable
	from, err := strconv.ParseUint(subs[3], 10, 64)
	if err != nil {
		return false
	}
	to, err := strconv.ParseUint(subs[4], 10, 64)
	if err != nil {
		return false
	}
	if (to-from)%Erigon3SeedableSteps != 0 {
		return false
	}
	return true
}
func IsStateFile(name string) (ok bool) {
	_, name = filepath.Split(name) // remove absolute path, or `history/` prefixes
	subs := stateFileRegex.FindStringSubmatch(name)
	if len(subs) != 6 {
		return false
	}
	// Check that it's seedable
	_, err := strconv.ParseUint(subs[3], 10, 64)
	if err != nil {
		return false
	}
	_, err = strconv.ParseUint(subs[4], 10, 64)

	return err == nil
}

func SeedableV2Extensions() []string {
	return []string{".seg"}
}

func SeedableV3Extensions() []string {
	return []string{".kv", ".v", ".ef"}
}

func IsSeedableExtension(name string) bool {
	for _, ext := range append(SeedableV2Extensions(), SeedableV3Extensions()...) {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}

const Erigon3SeedableSteps = 64

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
	Version         Version
	From, To        uint64
	name, Path, Ext string
	Type            Type
}

func (f FileInfo) TorrentFileExists() (bool, error) { return dir.FileExist(f.Path + ".torrent") }

func (f FileInfo) Name() string { return f.name }
func (f FileInfo) Dir() string  { return filepath.Dir(f.Path) }
func (f FileInfo) Len() uint64  { return f.To - f.From }

func (f FileInfo) CompareTo(o FileInfo) int {
	if res := cmp.Compare(f.From, o.From); res != 0 {
		return res
	}

	if res := cmp.Compare(f.To, o.To); res != 0 {
		return res
	}

	// this is a lexical comparison (don't use enum)
	return strings.Compare(f.Type.Name(), o.Type.Name())
}

func (f FileInfo) As(t Type) FileInfo {
	name := fmt.Sprintf("v%d-%06d-%06d-%s%s", f.Version, f.From/1_000, f.To/1_000, t, f.Ext)
	return FileInfo{
		Version: f.Version,
		From:    f.From,
		To:      f.To,
		Ext:     f.Ext,
		Type:    t,
		name:    name,
		Path:    filepath.Join(f.Dir(), name),
	}
}

func IdxFiles(dir string) (res []FileInfo, err error) {
	return FilesWithExt(dir, ".idx")
}

func Segments(dir string) (res []FileInfo, err error) {
	return FilesWithExt(dir, ".seg")
}

func TmpFiles(name string) (res []string, err error) {
	files, err := dir.ReadDir(name)
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

		res = append(res, filepath.Join(name, f.Name()))
	}
	return res, nil
}

// ParseDir - reading dir (
func ParseDir(name string) (res []FileInfo, err error) {
	files, err := dir.ReadDir(name)
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

		meta, _, ok := ParseFileName(name, f.Name())
		if !ok {
			continue
		}
		res = append(res, meta)
	}
	slices.SortFunc(res, func(i, j FileInfo) int {
		switch {
		case i.Version != j.Version:
			return cmp.Compare(i.Version, j.Version)

		case i.From != j.From:
			return cmp.Compare(i.From, j.From)

		case i.To != j.To:
			return cmp.Compare(i.To, j.To)

		case i.Type.Enum() != j.Type.Enum():
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
