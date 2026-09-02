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
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/version"
)

// EpochMarker flags an epoch (era1-aligned) block segment in its file name, as a suffix on the type:
// v1.0-000000-000008-headers-ep.seg. The marker makes the regime self-describing and independent of
// the (content) version; decimal files carry no marker. It goes last, not next to the version, so that
// a lookup mask — whose version is a wildcard, since an index's version may differ from its segment's
// — cannot match across regimes: the two regimes print the same digits for the same tier index
// (decimal [1000,2000) and epoch [1024,2048) are both "000001-000002"), and a wildcard sitting where
// the marker is would swallow it.
const EpochMarker = "ep"

// fileNameBase is the divisor a file name encodes block numbers with: epoch block segments print
// block/1024, decimal ones (Bor/Gnosis block segments, and every non-block type) block/1000.
func fileNameBase(epoch bool) uint64 {
	if epoch {
		return 1_024
	}
	return 1_000
}

func markedType(fileType string, epoch bool) string {
	if epoch {
		return fileType + "-" + EpochMarker
	}
	return fileType
}

func fileName(version Version, epoch bool, from, to uint64, fileType string) string {
	base, spellOut := nameEncoding(epoch, from, to)
	fileType = markedType(fileType, epoch)
	if spellOut {
		return fmt.Sprintf("%s-%09d-%09d-%s", version.String(), from, to, fileType)
	}
	return fmt.Sprintf("%s-%06d-%06d-%s", version.String(), from/base, to/base, fileType)
}

func fileMask(epoch bool, from, to uint64, fileType string) string {
	base, spellOut := nameEncoding(epoch, from, to)
	fileType = markedType(fileType, epoch)
	if spellOut {
		return fmt.Sprintf("*-%09d-%09d-%s", from, to, fileType)
	}
	return fmt.Sprintf("*-%06d-%06d-%s", from/base, to/base, fileType)
}

func nameEncoding(epoch bool, from, to uint64) (base uint64, spellOut bool) {
	if to < from {
		panic(fmt.Errorf("snap file name to < from: %d < %d", to, from))
	}
	base = fileNameBase(epoch)
	return base, to-from < base
}

func FileName(version Version, epoch bool, from, to uint64, fileType string) string {
	return fileName(version, epoch, from, to, fileType)
}

func FileMask(epoch bool, from, to uint64, fileType string) string {
	return fileMask(epoch, from, to, fileType)
}

func SegmentFileName(version Version, epoch bool, from, to uint64, t Enum) string {
	return fileName(version, epoch, from, to, t.String()) + ".seg"
}
func IdxFileName(version Version, epoch bool, from, to uint64, fType string) string {
	return fileName(version, epoch, from, to, fType) + ".idx"
}

func SegmentFileMask(epoch bool, from, to uint64, t Enum) string {
	return fileMask(epoch, from, to, t.String()) + ".seg"
}
func IdxFileMask(epoch bool, from, to uint64, fType string) string {
	return fileMask(epoch, from, to, fType) + ".idx"
}

func FilterExt(in []FileInfo, expectExt string) (out []FileInfo) {
	for i := range in {
		f := &in[i]
		if f.Ext != expectExt { // filter out only compressed files
			continue
		}
		out = append(out, *f)
	}

	slices.SortFunc(out, func(a, b FileInfo) int {
		if a.Type != nil && b.Type != nil {
			if cmp := strings.Compare(a.Type.Name(), b.Type.Name()); cmp != 0 {
				return cmp
			}
		}
		if cmp := strings.Compare(a.TypeString, b.TypeString); cmp != 0 {
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

		return a.Version.Cmp(b.Version)
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
	return len(parts) == 4 || len(parts) == 5
}

// check that filename w/o ext matches pattern: "<any>.<num>-<num>"
var StateFileRegex = regexp.MustCompile(`^[^.]+\.\d+-\d+$`)

func IsStateFileV2(name string) bool {
	return StateFileRegex.MatchString(name)
}

func IsCaplin(dir string, fileName string) bool {
	if strings.Contains(fileName, "caplin") || strings.Contains(dir, "caplin") {
		return true
	}
	return false
}

func ParseFileName(dir, fileName string) (res FileInfo, isE3Seedable bool, ok bool) {
	res.Path = filepath.Join(dir, fileName)
	res.Ext = filepath.Ext(fileName)
	res.name = fileName
	caplin := IsCaplin(dir, fileName)

	fileName = filepath.Base(fileName)

	if isSaltFile(fileName) {
		typeString := "salt"
		// format for salt files is different: salt-<type>.txt
		res.Type, _ = ParseFileType(typeString)
		res.CaplinTypeString = typeString
		res.TypeString = typeString
		return res, false, true
	}

	var err error
	res.Version, err = version.ParseVersion(fileName)
	if err != nil {
		return res, false, false
	}

	_, remainingPart, ok := strings.Cut(fileName, "-")
	if !ok {
		return res, false, false
	}
	croppedFileName, ok := strings.CutSuffix(remainingPart, res.Ext)
	if !ok {
		return res, false, false
	}

	for ext := filepath.Ext(croppedFileName); ext != "" && !strings.Contains(ext, "-"); ext = filepath.Ext(croppedFileName) {
		croppedFileName = strings.TrimSuffix(croppedFileName, ext)
	}

	isStateFile := IsStateFileV2(croppedFileName)

	if isStateFile { // accounts.24-28
		typeString, rest, ok := strings.Cut(croppedFileName, ".")
		if !ok || typeString == "" {
			return res, false, false
		}
		fromStr, toStr, ok := strings.Cut(rest, "-")
		if !ok || fromStr == "" || toStr == "" {
			return res, false, false
		}

		from, err := strconv.Atoi(fromStr)
		if err != nil {
			return res, false, false
		}
		to, err := strconv.Atoi(toStr)
		if err != nil {
			return res, false, false
		}

		res.From, res.To, res.TypeString = uint64(from), uint64(to), typeString
		res.Type, ok = ParseFileType(typeString)
		if ok {
			res.CaplinTypeString = res.Type.Name()
		}
	} else { // 1-2-bodies  (or  1-2-bodies-ep for epoch)
		// epoch (era1-aligned) block segments carry the "ep" marker as a type suffix; it makes the
		// regime self-describing, independent of the version.
		if trimmed, found := strings.CutSuffix(croppedFileName, "-"+EpochMarker); found {
			res.Epoch = true
			croppedFileName = trimmed
		}
		fromStr, rest, ok := strings.Cut(croppedFileName, "-")
		if !ok || fromStr == "" {
			return res, false, false
		}
		toStr, typeString, ok := strings.Cut(rest, "-")
		if !ok || toStr == "" || typeString == "" {
			return res, false, false
		}

		from, err := strconv.Atoi(fromStr)
		if err != nil {
			return res, false, false
		}
		to, err := strconv.Atoi(toStr)
		if err != nil {
			return res, false, false
		}
		var multiplier uint64
		switch {
		case len(fromStr) == 9: // range shorter than the regime's divisor, spelled out as it is
			multiplier = 1
		default:
			multiplier = fileNameBase(res.Epoch)
		}
		res.From, res.To, res.TypeString, res.CaplinTypeString = uint64(from)*multiplier, uint64(to)*multiplier, typeString, typeString
		res.Type, ok = ParseFileType(typeString)
		if ok {
			res.CaplinTypeString = res.Type.Name()
		} else if !caplin {
			return res, isStateFile, false
		}
	}
	if caplin {
		return res, isStateFile, true
	}
	return res, isStateFile, true
}

func isSaltFile(name string) bool {
	return strings.HasPrefix(name, "salt")
}

var stateFileRegex = regexp.MustCompile("^v([0-9]+)(?:.([0-9]+))?-([[:lower:]]+).([0-9]+)-([0-9]+).(.*)$")

func parseStateFile(name string) (from, to uint64, ok bool) {
	_, name = filepath.Split(name) // убираем путь
	subs := stateFileRegex.FindStringSubmatch(name)
	if len(subs) != 7 && len(subs) != 6 {
		return 0, 0, false
	}

	fromIdx := len(subs) - 3
	toIdx := len(subs) - 2

	from, err := strconv.ParseUint(subs[fromIdx], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	to, err = strconv.ParseUint(subs[toIdx], 10, 64)
	if err != nil {
		return 0, 0, false
	}

	return from, to, true
}

func IsStateFileSeedable(name string) bool {
	return IsStateFile(name) // all state files are seedable (in the past we seeded only big files)
}

func IsStateFile(name string) bool {
	_, _, ok := parseStateFile(name)
	return ok
}

func IsTorrentPartial(ext string) bool {
	return strings.HasPrefix(ext, ".torrent") && len(ext) > len(".torrent")
}

func SeedableV2Extensions() []string {
	return []string{".seg"}
}

func AllV2Extensions() []string {
	return []string{".seg", ".idx", ".txt", ".toml"}
}

func SeedableV3Extensions() []string {
	return []string{".kv", ".v", ".ef", ".ap"}
}

func AllV3Extensions() []string {
	return []string{".kv", ".v", ".ef", ".kvei", ".vi", ".efi", ".bt", ".kvi"}
}

func IsSeedableExtension(name string) bool {
	for _, ext := range append(AllV2Extensions(), AllV3Extensions()...) {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}

// Use-cases:
//   - produce and seed snapshots earlier on chain tip. reduce dependency on "good peers with history" at p2p-network.
//     Some networks don't have many archive peers, also ConsensusLayer clients are not-good(not-incentivised) at serving history.
//   - avoiding having too much files:
//     more files(shards) - means "more metadata", "more lookups for non-indexed queries", "more dictionaries", "more bittorrent connections", ...
//     less files - means small files will be removed after merge (no peers for this files).
const Erigon2OldMergeLimit = 500_000
const Erigon2MergeLimit = 100_000

// EpochMergeLimit is the frozen (top-tier) size of epoch-rounded block segments: 64 era1
// files of 8192 blocks each.
const EpochMergeLimit = 524_288
const CaplinMergeLimit = 10_000
const Erigon2MinSegmentSize = 1_000

// EpochMinSegmentSize is the produce/round-down granularity of epoch-rounded segments
// (the smallest tier, 2^10), analogous to Erigon2MinSegmentSize for the decimal scheme.
const EpochMinSegmentSize = 1_024

var MergeSteps = []uint64{100_000, 10_000}

// EpochMergeSteps are the merge targets (largest first) for epoch-rounded segments: the
// 1024-block produce tier merges up through 8192 (one era1 file), 65536, to 524288.
var EpochMergeSteps = []uint64{524_288, 65_536, 8_192}

// epochTypes is the set of segment types eligible for the epoch (era1-aligned) layout — the core eth
// block segments. Whether a given file/production actually uses epoch is decided per chain (see
// snaptype2.RegimeFor); this set only gates which types can ever be epoch. Written only at init.
var epochTypes = map[Enum]bool{Unknown: true}

// RegisterEpochType marks a segment type as epoch-eligible.
func RegisterEpochType(t Enum) { epochTypes[t] = true }

// EpochType reports whether type t is epoch-eligible (a core block segment). The actual regime for a
// file is carried by FileInfo.Epoch (reads) or chosen by the chain at production (writes).
func EpochType(t Enum) bool { return epochTypes[t] }

// EpochRegimeMismatch reports whether snapshot file `name` belongs to the block-segment regime
// opposite to epochOn, so a chain in that regime must skip it: an epoch chain skips decimal
// (no "ep" marker) block files and a decimal chain skips epoch ("ep"-marked) block files. Files
// that are not core block segments (state, Caplin, Bor, salt) never mismatch.
func EpochRegimeMismatch(name string, epochOn bool) bool {
	fi, stateFile, ok := ParseFileName("", name)
	if !ok || stateFile || fi.Type == nil {
		return false
	}
	enum := fi.Type.Enum()
	if enum == Unknown || !epochTypes[enum] {
		return false
	}
	return fi.Epoch != epochOn
}

// FileInfo - parsed file metadata
type FileInfo struct {
	Version         Version
	From, To        uint64
	name, Path, Ext string
	Type            Type
	Epoch           bool // epoch (era1-aligned, "ep"-marked, block/1024) vs decimal (block/1000)

	CaplinTypeString string // part of file-name - without version, range, ext
	TypeString       string
}

func (f FileInfo) TorrentFileExists() (bool, error) { return dir.FileExist(f.Path + ".torrent") }

func (f FileInfo) Name() string { return f.name }
func (f FileInfo) Dir() string  { return filepath.Dir(f.Path) }
func (f FileInfo) Base() string { return path.Base(f.Path) }
func (f FileInfo) Len() uint64  { return f.To - f.From }

func (f FileInfo) GetRange() (from, to uint64) { return f.From, f.To }
func (f FileInfo) GetType() Type               { return f.Type }
func (f FileInfo) GetGrouping() string {
	// range + grouping uniquely identifies a file i.e. range "+" grouping = filename
	return f.Type.Name() + "_" + f.TypeString + "_" + f.Ext
}

func (f FileInfo) CompareTo(o FileInfo) int {
	if res := cmp.Compare(f.From, o.From); res != 0 {
		return res
	}

	if res := cmp.Compare(f.To, o.To); res != 0 {
		return res
	}

	return strings.Compare(f.name, o.name)
}

// As returns the same block range and version as f, for a different type. The name must come from
// FileName: an epoch block segment encodes block/1024, and a hand-rolled /1000 here would address a
// different range for every segment whose two encodings disagree (any tier from 8192 up).
func (f FileInfo) As(t Type) FileInfo {
	name := fileName(f.Version, f.Epoch, f.From, f.To, t.Name()) + f.Ext
	return FileInfo{
		Version: f.Version,
		Epoch:   f.Epoch,
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
	return parseDirEntries(name, files)
}

func parseDirEntries(name string, files []os.DirEntry) (res []FileInfo, err error) {
	for _, f := range files {
		fileInfo, err := f.Info()
		if err != nil {
			// Deleted between ReadDir and this stat: merged-over segments are unlinked
			// concurrently with directory scans.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("ParseDir: %s: %w", name, err)
		}
		if f.IsDir() || fileInfo.Size() == 0 || len(f.Name()) < 3 {
			continue
		}

		meta, _, ok := ParseFileName(name, f.Name())
		if !ok || meta.Type == nil {
			continue
		}
		res = append(res, meta)
	}
	slices.SortFunc(res, func(i, j FileInfo) int {
		switch {
		case i.Version != j.Version:
			return i.Version.Cmp(j.Version)

		case i.From != j.From:
			return cmp.Compare(i.From, j.From)

		case i.To != j.To:
			return cmp.Compare(i.To, j.To)
		case i.Type != nil && j.Type != nil && i.Type.Enum() != j.Type.Enum():
			return cmp.Compare(i.Type.Enum(), j.Type.Enum())

		case i.TypeString != j.TypeString:
			return cmp.Compare(i.TypeString, j.TypeString)
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
