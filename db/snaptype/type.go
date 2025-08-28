// Copyright 2024 The Erigon Authors
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
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
)

type Version = version.Version
type Versions = version.Versions

type FirstKeyGetter func(ctx context.Context) uint64

type BlockHashResolver interface {
	CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (h common.Hash, ok bool, err error)
}

type RangeExtractor interface {
	Extract(ctx context.Context, blockFrom, blockTo uint64, firstKey FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger, hashResolver BlockHashResolver) (uint64, error)
}

type RangeExtractorFunc func(ctx context.Context, blockFrom, blockTo uint64, firstKey FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger, hashResolver BlockHashResolver) (uint64, error)

func (f RangeExtractorFunc) Extract(ctx context.Context, blockFrom, blockTo uint64, firstKey FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger, hashResolver BlockHashResolver) (uint64, error) {
	return f(ctx, blockFrom, blockTo, firstKey, db, chainConfig, collect, workers, lvl, logger, hashResolver)
}

type IndexBuilder interface {
	Build(ctx context.Context, info FileInfo, salt uint32, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error
}

type IndexBuilderFunc func(ctx context.Context, info FileInfo, salt uint32, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error

func (f IndexBuilderFunc) Build(ctx context.Context, info FileInfo, salt uint32, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	return f(ctx, info, salt, chainConfig, tmpDir, p, lvl, logger)
}

var saltMap = map[string]uint32{}
var saltLock sync.RWMutex

func LoadSalt(baseDir string, autoCreate bool, logger log.Logger) (*uint32, error) {
	// issue: https://github.com/erigontech/erigon/issues/14300
	// NOTE: The salt value from this is read after snapshot stage AND the value is not
	// cached before snapshot stage (which downloads salt-blocks.txt too), and therefore
	// we're good as far as the above issue is concerned.
	fpath := filepath.Join(baseDir, "salt-blocks.txt")
	exists, err := dir.FileExist(fpath)
	if err != nil {
		return nil, err
	}

	if !exists {
		if !autoCreate {
			logger.Debug("snaptype salt file not found + autocreate disabled")
			return nil, nil
		}
		dir.MustExist(baseDir)

		saltBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(saltBytes, randUint32())
		if err := dir.WriteFileWithFsync(fpath, saltBytes, os.ModePerm); err != nil {
			return nil, err
		}
	}
	saltBytes, err := os.ReadFile(fpath)
	if err != nil {
		return nil, err
	}
	if len(saltBytes) != 4 {
		dir.MustExist(baseDir)

		saltBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(saltBytes, randUint32())
		if err := dir.WriteFileWithFsync(fpath, saltBytes, os.ModePerm); err != nil {
			return nil, err
		}
	}

	salt := binary.BigEndian.Uint32(saltBytes)
	return &salt, nil

}

// GetIndicesSalt - try read salt for all indices from DB. Or fall-back to new salt creation.
// if db is Read-Only (for example remote RPCDaemon or utilities) - we will not create new indices -
// and existing indices have salt in metadata.
func GetIndexSalt(baseDir string, logger log.Logger) (uint32, error) {
	saltLock.RLock()
	salt, ok := saltMap[baseDir]
	saltLock.RUnlock()
	if ok {
		return salt, nil
	}

	saltp, err := LoadSalt(baseDir, false, logger)
	if err != nil {
		return 0, err
	}
	if saltp == nil {
		logger.Error("salt not found", "stack", dbg.Stack())
		return 0, errors.New("salt not found in GetIndexSalt")
	}
	salt = *saltp

	saltLock.Lock()
	saltMap[baseDir] = salt
	saltLock.Unlock()

	return salt, nil
}

type Index struct {
	Name   string
	Offset int
}

var CaplinIndexes = struct {
	BeaconBlockSlot,
	BlobSidecarSlot Index
}{
	BeaconBlockSlot: Index{Name: "beaconblocks"},
	BlobSidecarSlot: Index{Name: "blocksidecars"},
}

func (i Index) HasFile(info FileInfo, logger log.Logger) bool {
	dir := info.Dir()
	fName := IdxFileName(info.Version, info.From, info.To, i.Name)

	segment, err := seg.NewDecompressor(info.Path)

	if err != nil {
		return false
	}

	defer segment.Close()

	idx, err := recsplit.OpenIndex(filepath.Join(dir, fName))

	if err != nil {
		return false
	}

	defer idx.Close()

	return true // idx.ModTime().After(segment.ModTime())
}

type Type interface {
	Enum() Enum
	Versions() Versions
	Name() string
	FileName(version Version, from uint64, to uint64) string
	FileInfo(dir string, from uint64, to uint64) FileInfo
	IdxFileName(version Version, from uint64, to uint64, index ...Index) string
	IdxFileNames(version Version, from uint64, to uint64) []string
	Indexes() []Index
	HasIndexFiles(info FileInfo, logger log.Logger) bool
	BuildIndexes(ctx context.Context, info FileInfo, indexBuilder IndexBuilder, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error
	ExtractRange(ctx context.Context, info FileInfo, rangeExtractor RangeExtractor, indexBuilder IndexBuilder, firstKeyGetter FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger, hashResolver BlockHashResolver) (uint64, error)

	RangeExtractor() RangeExtractor
}

type snapType struct {
	enum           Enum
	name           string
	versions       Versions
	indexes        []Index
	indexBuilder   IndexBuilder
	rangeExtractor RangeExtractor
}

// These are raw maps with no mutex protection becuase they are
// expected to be written to once during program initialization
// and them be readonly
var registeredTypes = map[Enum]Type{}
var namedTypes = map[string]Type{}

func RegisterType(enum Enum, name string, versions Versions, rangeExtractor RangeExtractor, indexes []Index, indexBuilder IndexBuilder) Type {
	t := snapType{
		enum: enum, name: name, versions: versions, indexes: indexes, rangeExtractor: rangeExtractor, indexBuilder: indexBuilder,
	}

	registeredTypes[enum] = t
	namedTypes[strings.ToLower(name)] = t

	for _, index := range indexes {
		if _, ok := namedTypes[strings.ToLower(index.Name)]; !ok {
			namedTypes[strings.ToLower(index.Name)] = t
		}
	}

	return t
}

func (s snapType) Enum() Enum {
	return s.enum
}

func (s snapType) Versions() Versions {
	return s.versions
}

func (s snapType) Name() string {
	return s.name
}

func (s snapType) String() string {
	return s.Name()
}

func (s snapType) RangeExtractor() RangeExtractor {
	return s.rangeExtractor
}

func (s snapType) FileName(version Version, from uint64, to uint64) string {
	if version.Major == 0 && version.Minor == 0 {
		version = s.versions.Current
	}

	return SegmentFileName(version, from, to, s.enum)
}

func (s snapType) FileInfo(dir string, from uint64, to uint64) FileInfo {
	f, _, _ := ParseFileName(dir, s.FileName(s.versions.Current, from, to))
	return f
}

func (s snapType) ExtractRange(ctx context.Context, info FileInfo, rangeExtractor RangeExtractor, indexBuilder IndexBuilder, firstKeyGetter FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger, hashResolver BlockHashResolver) (uint64, error) {
	if rangeExtractor == nil {
		rangeExtractor = s.rangeExtractor
	}
	return ExtractRange(ctx, info, rangeExtractor, indexBuilder, firstKeyGetter, db, chainConfig, tmpDir, workers, lvl, logger, hashResolver)
}

func (s snapType) Indexes() []Index {
	return s.indexes
}

func (s snapType) BuildIndexes(ctx context.Context, info FileInfo, indexBuilder IndexBuilder, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	salt, err := GetIndexSalt(info.Dir(), logger)

	if err != nil {
		return err
	}

	if indexBuilder == nil {
		indexBuilder = s.indexBuilder
	}

	return indexBuilder.Build(ctx, info, salt, chainConfig, tmpDir, p, lvl, logger)
}

func (s snapType) HasIndexFiles(info FileInfo, logger log.Logger) bool {
	for _, index := range s.indexes {
		if !index.HasFile(info, logger) {
			return false
		}
	}

	return true
}

func (s snapType) IdxFileNames(version Version, from uint64, to uint64) []string {
	fileNames := make([]string, len(s.indexes))
	for i, index := range s.indexes {
		fileNames[i] = IdxFileName(version, from, to, index.Name)
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

	return IdxFileName(version, from, to, index[0].Name)
}

func ParseFileType(s string) (Type, bool) {
	enum, ok := ParseEnum(s)

	if !ok {
		return nil, false
	}

	return enum.Type(), true
}

type Enum int

const Unknown Enum = 0

type Enums struct {
	Unknown Enum
}

const MinCoreEnum = 1
const MinBorEnum = 5
const MinCaplinEnum = 9

const MaxEnum = 12

var CaplinEnums = struct {
	Enums
	BeaconBlocks,
	BlobSidecars Enum
}{
	Enums:        Enums{},
	BeaconBlocks: MinCaplinEnum,
	BlobSidecars: MinCaplinEnum + 1,
}

func (ft Enum) String() string {
	switch ft {
	case CaplinEnums.BeaconBlocks:
		return "beaconblocks"
	case CaplinEnums.BlobSidecars:
		return "blobsidecars"
	default:
		if t, ok := registeredTypes[ft]; ok {
			return t.Name()
		}

		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func (ft Enum) Type() Type {
	switch ft {
	case CaplinEnums.BeaconBlocks:
		return BeaconBlocks
	case CaplinEnums.BlobSidecars:
		return BlobSidecars
	default:
		return registeredTypes[ft]
	}
}

func (e Enum) FileName(from uint64, to uint64) string {
	return SegmentFileName(e.Type().Versions().Current, from, to, e)
}

func (e Enum) FileInfo(dir string, from uint64, to uint64) FileInfo {
	f, _, _ := ParseFileName(dir, e.FileName(from, to))
	return f
}

func (e Enum) HasIndexFiles(info FileInfo, logger log.Logger) bool {
	return e.Type().HasIndexFiles(info, logger)
}

func (e Enum) BuildIndexes(ctx context.Context, info FileInfo, indexBuilder IndexBuilder, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	return e.Type().BuildIndexes(ctx, info, indexBuilder, chainConfig, tmpDir, p, lvl, logger)
}

func ParseEnum(s string) (Enum, bool) {
	s = strings.ToLower(s)
	switch s {
	case "beaconblocks":
		return CaplinEnums.BeaconBlocks, true
	case "blobsidecars":
		return CaplinEnums.BlobSidecars, true
	default:
		if t, ok := namedTypes[s]; ok {
			return t.Enum(), true
		}
		return Enums{}.Unknown, false
	}
}

// Idx - iterate over segment and building .idx file
func BuildIndex(ctx context.Context, info FileInfo, cfg recsplit.RecSplitArgs, lvl log.Lvl, p *background.Progress, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("index panic: at=%s, %v, %s", info.Name(), rec, dbg.Stack())
		}
	}()

	d, err := seg.NewDecompressor(info.Path)
	if err != nil {
		return fmt.Errorf("can't open %s for indexing: %w", info.Name(), err)
	}
	defer d.Close()

	if p != nil {
		fname := info.Name()
		p.Name.Store(&fname)
		p.Total.Store(uint64(d.Count()))
	}
	cfg.KeyCount = d.Count()
	cfg.IndexFile = filepath.Join(info.Dir(), info.Type.IdxFileName(info.Version, info.From, info.To))
	rs, err := recsplit.NewRecSplit(cfg, logger)
	if err != nil {
		return err
	}
	defer rs.Close()
	rs.LogLvl(lvl)

	defer d.MadvSequential().DisableReadAhead()

	for {
		g := d.MakeGetter()
		var i, offset, nextPos uint64
		word := make([]byte, 0, 4096)

		for g.HasNext() {
			word, nextPos = g.Next(word[:0])
			if err := walker(rs, i, offset, word); err != nil {
				return err
			}
			i++
			offset = nextPos

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		if err = rs.Build(ctx); err != nil {
			if errors.Is(err, recsplit.ErrCollision) {
				logger.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
				rs.ResetNextSalt()
				continue
			}
			return err
		}

		return nil
	}
}

func BuildIndexWithSnapName(ctx context.Context, info FileInfo, cfg recsplit.RecSplitArgs, lvl log.Lvl, p *background.Progress, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("index panic: at=%s, %v, %s", info.Name(), rec, dbg.Stack())
		}
	}()

	d, err := seg.NewDecompressor(info.Path)
	if err != nil {
		return fmt.Errorf("can't open %s for indexing: %w", info.Name(), err)
	}
	defer d.Close()

	if p != nil {
		fname := info.Name()
		p.Name.Store(&fname)
		p.Total.Store(uint64(d.Count()))
	}
	cfg.KeyCount = d.Count()
	cfg.IndexFile = filepath.Join(info.Dir(), strings.ReplaceAll(info.name, ".seg", ".idx"))
	rs, err := recsplit.NewRecSplit(cfg, logger)
	if err != nil {
		return err
	}
	defer rs.Close()
	rs.LogLvl(lvl)

	defer d.MadvSequential().DisableReadAhead()

	for {
		g := d.MakeGetter()
		var i, offset, nextPos uint64
		word := make([]byte, 0, 4096)

		for g.HasNext() {
			word, nextPos = g.Next(word[:0])
			if err := walker(rs, i, offset, word); err != nil {
				return err
			}
			i++
			offset = nextPos

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		if err = rs.Build(ctx); err != nil {
			if errors.Is(err, recsplit.ErrCollision) {
				logger.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
				rs.ResetNextSalt()
				continue
			}
			return err
		}

		return nil
	}
}

func ExtractRange(ctx context.Context, f FileInfo, extractor RangeExtractor, indexBuilder IndexBuilder, firstKey FirstKeyGetter, chainDB kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger, hashResolver BlockHashResolver) (uint64, error) {
	var lastKeyValue uint64

	sn, err := seg.NewCompressor(ctx, "Snapshot "+f.Type.Name(), f.Path, tmpDir, seg.DefaultCfg, lvl, logger)

	if err != nil {
		return lastKeyValue, err
	}
	defer sn.Close()

	lastKeyValue, err = extractor.Extract(ctx, f.From, f.To, firstKey, chainDB, chainConfig, func(v []byte) error {
		return sn.AddWord(v)
	}, workers, lvl, logger, hashResolver)

	if err != nil {
		return lastKeyValue, fmt.Errorf("ExtractRange: %w", err)
	}

	ext := filepath.Ext(f.Name())
	logger.Log(lvl, "[snapshots] Compression start", "file", f.Name()[:len(f.Name())-len(ext)], "workers", sn.WorkersAmount())

	if err := sn.Compress(); err != nil {
		return lastKeyValue, fmt.Errorf("compress: %w", err)
	}

	p := &background.Progress{}

	if err := f.Type.BuildIndexes(ctx, f, indexBuilder, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return lastKeyValue, err
	}

	return lastKeyValue, nil
}

func randUint32() uint32 {
	var buf [4]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint32(buf[:])
}
