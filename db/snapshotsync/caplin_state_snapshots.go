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

package snapshotsync

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/ethconfig"
)

func BeaconSimpleIdx(ctx context.Context, sn snaptype.FileInfo, salt uint32, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	num := make([]byte, binary.MaxVarintLen64)
	cfg := recsplit.RecSplitArgs{
		Enums:      true,
		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     tmpDir,
		Salt:       &salt,
		BaseDataID: sn.From,
	}
	if err := snaptype.BuildIndex(ctx, sn, version.Versions{
		Current:      sn.Version,
		MinSupported: sn.Version,
	}, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if i%20_000 == 0 {
			logger.Log(lvl, "Generating idx for "+sn.Type.Name(), "progress", i)
		}
		p.Processed.Add(1)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("idx: %w", err)
	}

	return nil
}

func getKvGetterForStateTable(db kv.RoDB, tableName string) KeyValueGetter {
	return func(numId uint64) ([]byte, []byte, error) {
		var key, value []byte
		var err error
		if err := db.View(context.TODO(), func(tx kv.Tx) error {
			key = base_encoding.Encode64ToBytes4(numId)
			value, err = tx.GetOne(tableName, key)
			value = bytes.Clone(value)
			return err
		}); err != nil {
			return nil, nil, err
		}
		return key, value, nil
	}
}

func MakeCaplinStateSnapshotsTypes(db kv.RoDB) SnapshotTypes {
	return SnapshotTypes{
		KeyValueGetters: map[string]KeyValueGetter{
			kv.ValidatorEffectiveBalance:     getKvGetterForStateTable(db, kv.ValidatorEffectiveBalance),
			kv.ValidatorSlashings:            getKvGetterForStateTable(db, kv.ValidatorSlashings),
			kv.ValidatorBalance:              getKvGetterForStateTable(db, kv.ValidatorBalance),
			kv.StateEvents:                   getKvGetterForStateTable(db, kv.StateEvents),
			kv.ActiveValidatorIndicies:       getKvGetterForStateTable(db, kv.ActiveValidatorIndicies),
			kv.StateRoot:                     getKvGetterForStateTable(db, kv.StateRoot),
			kv.BlockRoot:                     getKvGetterForStateTable(db, kv.BlockRoot),
			kv.SlotData:                      getKvGetterForStateTable(db, kv.SlotData),
			kv.EpochData:                     getKvGetterForStateTable(db, kv.EpochData),
			kv.InactivityScores:              getKvGetterForStateTable(db, kv.InactivityScores),
			kv.NextSyncCommittee:             getKvGetterForStateTable(db, kv.NextSyncCommittee),
			kv.CurrentSyncCommittee:          getKvGetterForStateTable(db, kv.CurrentSyncCommittee),
			kv.Eth1DataVotes:                 getKvGetterForStateTable(db, kv.Eth1DataVotes),
			kv.IntraRandaoMixes:              getKvGetterForStateTable(db, kv.IntraRandaoMixes),
			kv.RandaoMixes:                   getKvGetterForStateTable(db, kv.RandaoMixes),
			kv.BalancesDump:                  getKvGetterForStateTable(db, kv.BalancesDump),
			kv.EffectiveBalancesDump:         getKvGetterForStateTable(db, kv.EffectiveBalancesDump),
			kv.PendingConsolidations:         getKvGetterForStateTable(db, kv.PendingConsolidations),
			kv.PendingPartialWithdrawals:     getKvGetterForStateTable(db, kv.PendingPartialWithdrawals),
			kv.PendingDeposits:               getKvGetterForStateTable(db, kv.PendingDeposits),
			kv.PendingConsolidationsDump:     getKvGetterForStateTable(db, kv.PendingConsolidationsDump),
			kv.PendingPartialWithdrawalsDump: getKvGetterForStateTable(db, kv.PendingPartialWithdrawalsDump),
			kv.PendingDepositsDump:           getKvGetterForStateTable(db, kv.PendingDepositsDump),
			// GLOAS (EIP-7732)
			kv.Builders:                          getKvGetterForStateTable(db, kv.Builders),
			kv.BuildersDump:                      getKvGetterForStateTable(db, kv.BuildersDump),
			kv.BuilderPendingWithdrawals:         getKvGetterForStateTable(db, kv.BuilderPendingWithdrawals),
			kv.BuilderPendingWithdrawalsDump:     getKvGetterForStateTable(db, kv.BuilderPendingWithdrawalsDump),
			kv.PayloadExpectedWithdrawals:        getKvGetterForStateTable(db, kv.PayloadExpectedWithdrawals),
			kv.PayloadExpectedWithdrawalsDump:    getKvGetterForStateTable(db, kv.PayloadExpectedWithdrawalsDump),
			kv.ExecutionPayloadAvailabilityTable: getKvGetterForStateTable(db, kv.ExecutionPayloadAvailabilityTable),
			kv.BuilderPendingPaymentsTable:       getKvGetterForStateTable(db, kv.BuilderPendingPaymentsTable),
			kv.PtcWindowTable:                    getKvGetterForStateTable(db, kv.PtcWindowTable),
			kv.LatestExecutionPayloadBidTable:    getKvGetterForStateTable(db, kv.LatestExecutionPayloadBidTable),
		},
		Compression: map[string]bool{},
	}
}

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinStateSnapshots struct {
	*BaseRoSnapshots
	Salt uint32

	snapshotTypes SnapshotTypes
	tmpdir        string
	typeEnums     map[string]snaptype.Enum
}

type KeyValueGetter func(numId uint64) ([]byte, []byte, error)

type SnapshotTypes struct {
	KeyValueGetters map[string]KeyValueGetter
	Compression     map[string]bool
}

// NewCaplinStateSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewCaplinStateSnapshots(cfg ethconfig.BlocksFreezing, beaconCfg *clparams.BeaconChainConfig, dirs datadir.Dirs, snapshotTypes SnapshotTypes, logger log.Logger) *CaplinStateSnapshots {
	if cfg.ChainName == "" {
		log.Debug("[dbg] NewCaplinSnapshots created with empty ChainName", "stack", dbg.Stack())
	}

	types := make([]snaptype.Type, 0, len(snapshotTypes.KeyValueGetters))
	typeEnums := make(map[string]snaptype.Enum, len(snapshotTypes.KeyValueGetters))
	for name := range snapshotTypes.KeyValueGetters {
		enum, ok := snaptype.ParseEnum(name)
		if !ok || enum < snaptype.MinCaplinEnum+2 || enum >= snaptype.MinBorEnum {
			panic(fmt.Sprintf("caplin state snapshot type %q is not registered", name))
		}
		typ := enum.Type()
		if typ == nil {
			panic(fmt.Sprintf("caplin state snapshot type %q has no registered type", name))
		}
		types = append(types, typ)
		typeEnums[name] = enum
	}
	if len(types) == 0 {
		panic("caplin state snapshot KeyValueGetters is empty")
	}
	slices.SortFunc(types, func(a, b snaptype.Type) int { return cmp.Compare(a.Enum(), b.Enum()) })

	return &CaplinStateSnapshots{
		BaseRoSnapshots: NewBaseRoSnapshots(cfg, dirs.SnapCaplin, types, types[0], false, logger),
		snapshotTypes:   snapshotTypes,
		tmpdir:          dirs.Tmp,
		typeEnums:       typeEnums,
	}
}

func (s *CaplinStateSnapshots) SegFileNames(from, to uint64) []string {
	view := s.View()
	defer view.Close()

	var res []string

	for _, typ := range s.Types() {
		for _, seg := range view.view.Segments(typ) {
			if seg.from >= to || seg.to <= from {
				continue
			}
			res = append(res, filepath.Join(s.Dir(), seg.src.FileName()))
		}
	}
	return res
}

func (s *CaplinStateSnapshots) BlocksAvailable() uint64 {
	return min(s.SegmentsMax(), s.IndicesMax())
}

func (s *CaplinStateSnapshots) IndicesMax() uint64 {
	if s == nil {
		return 0
	}
	minTo := uint64(math.MaxUint64)
	for _, typ := range s.Types() {
		to := s.VisibleSegmentsMaxTo(typ.Enum())
		if to == 0 {
			return 0
		}
		minTo = min(minTo, to)
	}
	if minTo == math.MaxUint64 {
		return 0
	}
	return minTo
}

func (s *CaplinStateSnapshots) SegmentsMax() uint64 {
	if s == nil {
		return 0
	}
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		return 0
	}
	var max uint64
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".seg" {
			continue
		}
		f, _, ok := snaptype.ParseFileName(s.Dir(), entry.Name())
		if !ok || f.Type == nil || !s.BaseRoSnapshots.HasType(f.Type) || f.To == 0 {
			continue
		}
		max = f.To - 1
	}
	return max
}

func (s *CaplinStateSnapshots) LogStat(str string) {
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", str),
		"blocks", common.PrettyExact(s.SegmentsMax()+1), "indices", common.PrettyExact(s.IndicesMax()+1))
}

func (s *CaplinStateSnapshots) TypeNames() []string {
	names := make([]string, 0, len(s.snapshotTypes.KeyValueGetters))
	for name := range s.snapshotTypes.KeyValueGetters {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *CaplinStateSnapshots) coveredRangesForType(name string) []Range {
	enum, ok := s.typeEnums[name]
	if !ok {
		return nil
	}
	segs := s.visible.Load().segments[enum]
	ranges := make([]Range, 0, len(segs))
	for _, seg := range segs {
		ranges = append(ranges, seg.Range)
	}
	return ranges
}

// ContiguousCoverageEnd returns the end of the unbroken visible-segment run that
// starts at slot 0 for the given type, or 0 when coverage is not rooted at genesis.
func (s *CaplinStateSnapshots) ContiguousCoverageEnd(typeName string) uint64 {
	ranges := s.coveredRangesForType(typeName)
	slices.SortFunc(ranges, func(a, b Range) int { return cmp.Compare(a.from, b.from) })
	var end uint64
	for _, r := range ranges {
		if r.from > end {
			break
		}
		if r.to > end {
			end = r.to
		}
	}
	return end
}

type CaplinStateView struct {
	s      *CaplinStateSnapshots
	view   *View
	closed bool
}

func (s *CaplinStateSnapshots) View() *CaplinStateView {
	if s == nil {
		return nil
	}
	return &CaplinStateView{s: s, view: s.BaseRoSnapshots.View()}
}

func (v *CaplinStateView) Close() {
	if v == nil {
		return
	}
	if v.closed {
		return
	}
	v.view.Close()
	v.view = nil
	v.s = nil
	v.closed = true
}

func (v *CaplinStateView) VisibleSegments(tbl string) VisibleSegments {
	// if v.s == nil || v.s.visible[tbl] == nil {
	// 	return nil
	// }
	// return v.s.visible[tbl]
	if v.s == nil || v.view == nil {
		return nil
	}
	if enum, ok := v.s.typeEnums[tbl]; ok {
		return v.view.Segments(enum.Type())
	}
	return nil
}

func (v *CaplinStateView) VisibleSegment(slot uint64, tbl string) (*VisibleSegment, bool) {
	for _, seg := range v.VisibleSegments(tbl) {
		if !(slot >= seg.from && slot < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

// errIncompleteStateRange signals that a mandatory-dense state table (block/state
// roots) has a missing entry in the range being dumped, so the range must not be
// frozen yet.
var errIncompleteStateRange = errors.New("state range not fully reconstructed")

func dumpCaplinState(ctx context.Context, snapName string, kvGetter KeyValueGetter, fromSlot uint64, toSlot, blocksPerFile uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger, compress bool) error {
	tmpDir, snapDir := dirs.Tmp, dirs.SnapCaplin

	segName, err := caplinStateFileName(snapName, fromSlot, toSlot)
	if err != nil {
		return err
	}
	f, _, ok := snaptype.ParseFileName(snapDir, segName)
	if !ok || f.Type == nil {
		return fmt.Errorf("invalid caplin state snapshot filename %q", segName)
	}

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshots "+snapName, f.Path, tmpDir, compressCfg, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	// block_roots/state_roots are written every slot; an empty entry means the DB
	// range isn't fully reconstructed. Freezing it writes a blank word that then
	// permanently shadows the DB (snapshots take read precedence), so refuse.
	mustBeDense := snapName == kv.BlockRoot || snapName == kv.StateRoot

	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		// read root.
		_, dump, err := kvGetter(i)
		if err != nil {
			return err
		}
		if mustBeDense && len(dump) != length.Hash {
			// An empty entry is a not-yet-reconstructed slot (retry later); a
			// non-empty entry of the wrong length is corruption (surface it).
			if len(dump) != 0 {
				return fmt.Errorf("%s slot %d: corrupt root, %d bytes (want %d)", snapName, i, len(dump), length.Hash)
			}
			return fmt.Errorf("%w: %s slot %d", errIncompleteStateRange, snapName, i)
		}
		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping "+snapName, "progress", i)
		}
		if compress {
			if err := sn.AddWord(dump); err != nil {
				return err
			}
		} else {
			if err := sn.AddUncompressedWord(dump); err != nil {
				return err
			}
		}
	}
	if sn.Count() != int(blocksPerFile) {
		return fmt.Errorf("expected %d blocks, got %d", blocksPerFile, sn.Count())
	}
	if err := sn.Compress(); err != nil {
		return err
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return simpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func simpleIdx(ctx context.Context, sn snaptype.FileInfo, salt uint32, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	num := make([]byte, binary.MaxVarintLen64)
	cfg := recsplit.RecSplitArgs{
		Enums:      true,
		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     tmpDir,
		Salt:       &salt,
		BaseDataID: sn.From,
	}
	if err := snaptype.BuildIndexWithSnapName(ctx, sn, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if i%20_000 == 0 {
			logger.Log(lvl, "Generating idx for "+sn.Name(), "progress", i)
		}
		p.Processed.Add(1)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("idx: %w", err)
	}

	return nil
}

func caplinStateFileName(snapName string, fromSlot, toSlot uint64) (string, error) {
	enum, ok := snaptype.ParseEnum(snapName)
	if !ok || enum < snaptype.MinCaplinEnum+2 || enum >= snaptype.MinBorEnum {
		return "", fmt.Errorf("unknown caplin state snapshot type %q", snapName)
	}
	typ := enum.Type()
	if typ == nil {
		return "", fmt.Errorf("unknown caplin state snapshot type %q", snapName)
	}
	return typ.FileName(version.ZeroVersion, fromSlot, toSlot), nil
}

type caplinStateDumpJob struct {
	name     string
	from, to uint64
}

// missingRanges returns the sub-ranges of [0, toSlot) not covered by `covered`
// (the type's existing segment ranges, sorted by `from`).
func missingRanges(covered []Range, toSlot uint64) []Range {
	var missing []Range
	var cur uint64
	for _, r := range covered {
		if r.from > cur {
			gapEnd := min(r.from, toSlot)
			missing = append(missing, Range{from: cur, to: gapEnd})
		}
		cur = max(cur, r.to)
		if cur >= toSlot {
			return missing
		}
	}
	if cur < toSlot {
		missing = append(missing, Range{from: cur, to: toSlot})
	}
	return missing
}

// planStateDump schedules only the ranges each type is missing within
// [0, toSlot), starting every full file at a gap boundary so it fills holes and
// the trailing tail without overlapping an existing segment.
func planStateDump(coverage map[string][]Range, toSlot, blocksPerFile uint64) []caplinStateDumpJob {
	toSlot = (toSlot / blocksPerFile) * blocksPerFile

	names := make([]string, 0, len(coverage))
	for name := range coverage {
		names = append(names, name)
	}
	sort.Strings(names)

	jobs := make([]caplinStateDumpJob, 0)
	for _, name := range names {
		for _, gap := range missingRanges(coverage[name], toSlot) {
			for i := gap.from; i+blocksPerFile <= gap.to; i += blocksPerFile {
				jobs = append(jobs, caplinStateDumpJob{name: name, from: i, to: i + blocksPerFile})
			}
		}
	}
	return jobs
}

func (s *CaplinStateSnapshots) DumpCaplinState(ctx context.Context, toSlot, blocksPerFile uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	coverage := make(map[string][]Range, len(s.snapshotTypes.KeyValueGetters))
	for name := range s.snapshotTypes.KeyValueGetters {
		coverage[name] = s.coveredRangesForType(name)
	}

	for _, job := range planStateDump(coverage, toSlot, blocksPerFile) {
		logger.Log(lvl, "Dumping "+job.name, "from", job.from, "to", job.to)
		if err := dumpCaplinState(ctx, job.name, s.snapshotTypes.KeyValueGetters[job.name], job.from, job.to, blocksPerFile, salt, dirs, workers, lvl, logger, s.snapshotTypes.Compression[job.name]); err != nil {
			if errors.Is(err, errIncompleteStateRange) {
				logger.Warn("[Caplin] skipping incomplete state range, will retry after reconstruction", "type", job.name, "from", job.from, "to", job.to, "err", err)
				continue
			}
			return err
		}
	}
	return nil
}

func (s *CaplinStateSnapshots) BuildMissingIndices(ctx context.Context, logger log.Logger) error {
	if s == nil {
		return nil
	}
	// if !s.segmentsReady.Load() {
	// 	return fmt.Errorf("not all snapshot segments are available")
	// }

	// wait for Downloader service to download all expected snapshots

	noneDone := true

	for _, typ := range s.Types() {
		var files []*DirtySegment
		s.WalkDirtySegments(typ.Enum(), func(df *DirtySegment) bool {
			files = append(files, df)
			return true
		})
		for _, df := range files {
			if df.Decompressor == nil {
				return fmt.Errorf("segment %s is not opened", df.FilePath())
			}
			if df.IsIndexed() {
				continue
			}
			sn, _, ok := snaptype.ParseFileName(s.Dir(), df.FileName())
			if !ok || sn.Type == nil {
				return fmt.Errorf("invalid caplin state snapshot filename %q", df.FileName())
			}

			indexFile := filepath.Join(sn.Dir(), snaptype.IdxFileName(sn.Version, sn.From, sn.To, sn.CaplinTypeString))
			if _, err := os.Stat(indexFile); err == nil {
				logger.Info("index file already exists, yet dirtyFile didn't have it opened", "seg", sn.Name())
				continue
			}
			logger.Info("building index file", "seg", sn.Name())
			p := &background.Progress{}
			noneDone = false

			if err := simpleIdx(ctx, sn, s.Salt, s.tmpdir, p, log.LvlDebug, logger); err != nil {
				return err
			}
		}
	}
	if noneDone {
		return nil
	}

	return s.OpenFolder()
}

func (s *CaplinStateSnapshots) Get(tbl string, slot uint64) ([]byte, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("Get(%s, %d), %s, %s\n", tbl, slot, rec, debug.Stack()))
		}
	}()

	view := s.View()
	defer view.Close()

	seg, ok := view.VisibleSegment(slot, tbl)
	if !ok {
		return nil, nil
	}

	return seg.Get(slot)
}
