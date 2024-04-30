package snaptype

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/log/v3"
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

type FirstKeyGetter func(ctx context.Context) uint64

type RangeExtractor interface {
	Extract(ctx context.Context, blockFrom, blockTo uint64, firstKey FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error)
}

type RangeExtractorFunc func(ctx context.Context, blockFrom, blockTo uint64, firstKey FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error)

func (f RangeExtractorFunc) Extract(ctx context.Context, blockFrom, blockTo uint64, firstKey FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	return f(ctx, blockFrom, blockTo, firstKey, db, chainConfig, collect, workers, lvl, logger)
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

// GetIndicesSalt - try read salt for all indices from DB. Or fall-back to new salt creation.
// if db is Read-Only (for example remote RPCDaemon or utilities) - we will not create new indices -
// and existing indices have salt in metadata.
func GetIndexSalt(baseDir string) (uint32, error) {
	saltLock.RLock()
	salt, ok := saltMap[baseDir]
	saltLock.RUnlock()

	if ok {
		return salt, nil
	}

	fpath := filepath.Join(baseDir, "salt-blocks.txt")
	if !dir.FileExist(fpath) {
		dir.MustExist(baseDir)

		saltBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(saltBytes, rand.Uint32())
		if err := dir.WriteFileWithFsync(fpath, saltBytes, os.ModePerm); err != nil {
			return 0, err
		}
	}
	saltBytes, err := os.ReadFile(fpath)
	if err != nil {
		return 0, err
	}

	salt = binary.BigEndian.Uint32(saltBytes)

	saltLock.Lock()
	saltMap[baseDir] = salt
	saltLock.Unlock()

	return salt, nil
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
	BorCheckpointId,
	BorMilestoneId,
	BeaconBlockSlot,
	BlobSidecarSlot Index
}{
	Unknown:          -1,
	HeaderHash:       0,
	BodyHash:         1,
	TxnHash:          2,
	TxnHash2BlockNum: 3,
	BorTxnHash:       4,
	BorSpanId:        5,
	BorCheckpointId:  6,
	BorMilestoneId:   7,
	BeaconBlockSlot:  8,
	BlobSidecarSlot:  9,
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
	case Indexes.BorCheckpointId:
		return Enums.BorCheckpoints.String()
	case Indexes.BorMilestoneId:
		return Enums.BorMilestones.String()
	case Indexes.BeaconBlockSlot:
		return Enums.BeaconBlocks.String()
	case Indexes.BlobSidecarSlot:
		return Enums.BlobSidecars.String()
	default:
		panic(fmt.Sprintf("unknown index: %d", i))
	}
}

func (i Index) HasFile(info FileInfo, logger log.Logger) bool {
	dir := info.Dir()
	fName := IdxFileName(info.Version, info.From, info.To, i.String())

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
	String() string
	FileName(version Version, from uint64, to uint64) string
	FileInfo(dir string, from uint64, to uint64) FileInfo
	IdxFileName(version Version, from uint64, to uint64, index ...Index) string
	IdxFileNames(version Version, from uint64, to uint64) []string
	Indexes() []Index
	HasIndexFiles(info FileInfo, logger log.Logger) bool
	BuildIndexes(ctx context.Context, info FileInfo, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error
	ExtractRange(ctx context.Context, info FileInfo, firstKeyGetter FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger) (uint64, error)
}

type snapType struct {
	enum           Enum
	versions       Versions
	indexes        []Index
	indexBuilder   IndexBuilder
	rangeExtractor RangeExtractor
}

var registeredTypes = map[Enum]Type{}

func RegisterType(enum Enum, versions Versions, rangeExtractor RangeExtractor, indexes []Index, indexBuilder IndexBuilder) Type {
	t := snapType{
		enum: enum, versions: versions, indexes: indexes, rangeExtractor: rangeExtractor, indexBuilder: indexBuilder,
	}

	registeredTypes[enum] = t

	return t
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
	f, _, _ := ParseFileName(dir, s.FileName(s.versions.Current, from, to))
	return f
}

func (s snapType) ExtractRange(ctx context.Context, info FileInfo, firstKeyGetter FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	return ExtractRange(ctx, info, s.rangeExtractor, firstKeyGetter, db, chainConfig, tmpDir, workers, lvl, logger)
}

func (s snapType) Indexes() []Index {
	return s.indexes
}

func (s snapType) BuildIndexes(ctx context.Context, info FileInfo, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	salt, err := GetIndexSalt(info.Dir())

	if err != nil {
		return err
	}

	return s.indexBuilder.Build(ctx, info, salt, chainConfig, tmpDir, p, lvl, logger)
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
	BorCheckpoints,
	BorMilestones,
	BeaconBlocks,
	BlobSidecars Enum
}{
	Unknown:        -1,
	Headers:        0,
	Bodies:         1,
	Transactions:   2,
	BorEvents:      3,
	BorSpans:       4,
	BorCheckpoints: 5,
	BorMilestones:  6,
	BeaconBlocks:   7,
	BlobSidecars:   8,
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
	case Enums.BorCheckpoints:
		return "borcheckpoints"
	case Enums.BorMilestones:
		return "bormilestones"
	case Enums.BeaconBlocks:
		return "beaconblocks"
	case Enums.BlobSidecars:
		return "blobsidecars"
	default:
		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func (ft Enum) Type() Type {
	switch ft {
	case Enums.BeaconBlocks:
		return BeaconBlocks
	case Enums.BlobSidecars:
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

func (e Enum) BuildIndexes(ctx context.Context, info FileInfo, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	return e.Type().BuildIndexes(ctx, info, chainConfig, tmpDir, p, lvl, logger)
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
	case "borcheckpoints":
		return Enums.BorCheckpoints, true
	case "bormilestones":
		return Enums.BorMilestones, true
	case "beaconblocks":
		return Enums.BeaconBlocks, true
	case "blobsidecars":
		return Enums.BlobSidecars, true
	default:
		return Enums.Unknown, false
	}
}

// Idx - iterate over segment and building .idx file
func BuildIndex(ctx context.Context, info FileInfo, salt uint32, firstDataId uint64, tmpDir string, lvl log.Lvl, p *background.Progress, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error, logger log.Logger) (err error) {
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

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      true,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(info.Dir(), info.Type.IdxFileName(info.Version, info.From, info.To)),
		BaseDataID: firstDataId,
		Salt:       &salt,
	}, logger)
	if err != nil {
		return err
	}
	rs.LogLvl(log.LvlDebug)

	defer d.EnableReadAhead().DisableReadAhead()

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

func ExtractRange(ctx context.Context, f FileInfo, extractor RangeExtractor, firstKey FirstKeyGetter, chainDB kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	var lastKeyValue uint64

	sn, err := seg.NewCompressor(ctx, "Snapshot "+f.Type.String(), f.Path, tmpDir, seg.MinPatternScore, workers, log.LvlTrace, logger)

	if err != nil {
		return lastKeyValue, err
	}
	defer sn.Close()

	lastKeyValue, err = extractor.Extract(ctx, f.From, f.To, firstKey, chainDB, chainConfig, func(v []byte) error {
		return sn.AddWord(v)
	}, workers, lvl, logger)

	if err != nil {
		return lastKeyValue, fmt.Errorf("ExtractRange: %w", err)
	}

	ext := filepath.Ext(f.Name())
	logger.Log(lvl, "[snapshots] Compression start", "file", f.Name()[:len(f.Name())-len(ext)], "workers", sn.Workers())

	if err := sn.Compress(); err != nil {
		return lastKeyValue, fmt.Errorf("compress: %w", err)
	}

	p := &background.Progress{}

	if err := f.Type.BuildIndexes(ctx, f, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return lastKeyValue, err
	}

	return lastKeyValue, nil
}
