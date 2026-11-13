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

package freezeblocks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/ethconfig"
)

var sidecarSSZSize = (&cltypes.BlobSidecar{}).EncodingSizeSSZ()

// value: chunked(ssz(SignedBeaconBlocks))
// slot       -> beacon_slot_segment_offset

type CaplinSnapshots struct {
	snapshotsync.BaseRoSnapshots

	// salt-blocks.txt is the EL blocks salt; caplin has no salt file of its own, so its
	// indexes stay on 0 as they always have been.
	Salt uint32

	tmpdir string
	logger log.Logger
	// chain cfg
	beaconCfg *clparams.BeaconChainConfig
}

// NewCaplinSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
//
// alignMin is false: blobs only start at Deneb and trail blocks, and aligning would pin
// block availability to the blob tip.
func NewCaplinSnapshots(cfg ethconfig.BlocksFreezing, beaconCfg *clparams.BeaconChainConfig, dirs datadir.Dirs, logger log.Logger) *CaplinSnapshots {
	c := &CaplinSnapshots{
		BaseRoSnapshots: *snapshotsync.NewBaseRoSnapshots(cfg, dirs.Snap, snaptype.CaplinSnapshotTypes, snaptype.BeaconBlocks, false, logger),
		tmpdir:          dirs.Tmp,
		logger:          logger,
		beaconCfg:       beaconCfg,
	}
	// The same slot=>offset mapping indexes both caplin types.
	beaconIdx := snaptype.IndexBuilderFunc(func(ctx context.Context, info snaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
		return snapshotsync.BeaconSimpleIdx(ctx, info, salt, tmpDir, p, lvl, logger)
	})
	c.SetIndexBuilder(snaptype.BeaconBlocks, beaconIdx)
	c.SetIndexBuilder(snaptype.BlobSidecars, beaconIdx)
	c.AnchorVisibleAtLowestDirty(snaptype.CaplinEnums.BeaconBlocks)
	return c
}

func (s *CaplinSnapshots) Close() {
	if s == nil {
		return
	}
	s.BaseRoSnapshots.Close()
}

func (s *CaplinSnapshots) SegmentsMax() uint64 { return s.IndicesMax() }

func (s *CaplinSnapshots) LogStat(str string) {
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", str),
		"blocks", common.PrettyExact(s.SegmentsMax()+1), "indices", common.PrettyExact(s.IndicesMax()+1))
}

func (s *CaplinSnapshots) LS() {
	if s == nil {
		return
	}
	view := s.View()
	defer view.Close()

	var stats seg.Stats
	lsSeg := func(d *seg.Decompressor) {
		log.Info("[agg] ", "f", d.FileName(), "words", d.Count(), "dictOnDisk", common.ByteCount(d.SerializedTotalDictSize()), "dictMem", common.ByteCount(d.DictMemSize()))
		stats.Add(d)
	}
	for _, sn := range view.BeaconBlocks() {
		lsSeg(sn.Src().Decompressor)
	}
	for _, sn := range view.BlobSidecars() {
		lsSeg(sn.Src().Decompressor)
	}
	log.Info("[agg] total", "words", stats.Words, "dictOnDisk", common.ByteCount(stats.Dict), "dictMem", common.ByteCount(stats.DictMem))
}

func (s *CaplinSnapshots) SegFileNames(from, to uint64) []string {
	view := s.View()
	defer view.Close()

	var res []string
	for _, seg := range view.BeaconBlocks() {
		if seg.From() >= from && seg.To() <= to {
			res = append(res, seg.Src().FileName())
		}
	}
	for _, seg := range view.BlobSidecars() {
		if seg.From() >= from && seg.To() <= to {
			res = append(res, seg.Src().FileName())
		}
	}
	return res
}

func (s *CaplinSnapshots) OpenBeaconBlocks() error {
	return s.openFolder(false, func(f snaptype.FileInfo) bool {
		return f.Type != nil && f.Type.Enum() == snaptype.CaplinEnums.BeaconBlocks
	})
}

func (s *CaplinSnapshots) openFolder(optimistic bool, fileFilter func(snaptype.FileInfo) bool) error {
	files, err := snapshotsync.AllTypedSegments(s.Dir(), s.Types())
	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for i := range files {
		if fileFilter != nil && !fileFilter(files[i]) {
			continue
		}
		f := &files[i]
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.OpenList(list, optimistic)
}

type CaplinView struct {
	base *snapshotsync.View
}

func (s *CaplinSnapshots) View() *CaplinView {
	return &CaplinView{base: s.BaseRoSnapshots.View()}
}

func (v *CaplinView) Close() { v.base.Close() }

func (v *CaplinView) BeaconBlocks() []*snapshotsync.VisibleSegment {
	return v.base.Segments(snaptype.BeaconBlocks)
}

func (v *CaplinView) BlobSidecars() []*snapshotsync.VisibleSegment {
	return v.base.Segments(snaptype.BlobSidecars)
}

func (v *CaplinView) BeaconBlocksSegment(slot uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(snaptype.BeaconBlocks, slot)
}

func dumpBeaconBlocksRange(ctx context.Context, db kv.RoDB, fromSlot uint64, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BeaconBlocks.FileName(version.ZeroVersion, fromSlot, toSlot)
	f, _, _ := snaptype.ParseFileName(snapDir, segName)

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshot BeaconBlocks", f.Path, tmpDir, compressCfg, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	skippedInARow := 0
	var prevBlockRoot common.Hash

	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		// read root.
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return err
		}
		parentRoot, err := beacon_indicies.ReadParentBlockRoot(tx, blockRoot)
		if err != nil {
			return err
		}
		if blockRoot != (common.Hash{}) && prevBlockRoot != (common.Hash{}) && parentRoot != prevBlockRoot {
			return fmt.Errorf("parent block root mismatch at slot %d", i)
		}

		dump, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(i, blockRoot))
		if err != nil {
			return err
		}
		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping beacon blocks", "progress", i)
		}
		if dump == nil {
			skippedInARow++
		} else {
			prevBlockRoot = blockRoot
			skippedInARow = 0
		}
		if skippedInARow > 1000 {
			return fmt.Errorf("skipped too many blocks in a row during snapshot generation, range %d-%d at slot %d", fromSlot, toSlot, i)
		}
		if err := sn.AddWord(dump); err != nil {
			return err
		}
	}
	tx.Rollback()
	if sn.Count() != snaptype.CaplinMergeLimit {
		return fmt.Errorf("expected %d blocks, got %d", snaptype.CaplinMergeLimit, sn.Count())
	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return snapshotsync.BeaconSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func DumpBlobSidecarsRange(ctx context.Context, db kv.RoDB, storage blob_storage.BlobStorage, fromSlot uint64, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, blobCountFn BlobCountBySlotFn, lvl log.Lvl, logger log.Logger) error {
	if blobCountFn == nil {
		return errors.New("dump blob sidecars: a blob count from the blocks is required to tell a blobless slot from a missing count row")
	}
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BlobSidecars.FileName(version.ZeroVersion, fromSlot, toSlot)
	f, _, _ := snaptype.ParseFileName(snapDir, segName)
	stagingPath := f.Path + ".tmp"

	compressCfg := seg.DefaultCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshot BlobSidecars", stagingPath, tmpDir, compressCfg, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	reusableBuf := []byte{}

	// Generate .seg file, which is just the list of beacon blocks.
	for i := fromSlot; i < toSlot; i++ {
		// read root.
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return err
		}

		commitmentsCount, err := storage.KzgCommitmentsCount(ctx, blockRoot)
		if err != nil {
			return err
		}
		blobCount, payloadSkipped, err := blobCountFn(tx, i)
		if err != nil {
			return err
		}
		if payloadSkipped {
			if err := sn.AddWord(nil); err != nil {
				return err
			}
			continue
		}
		if blobCount != uint64(commitmentsCount) {
			return fmt.Errorf("blob storage count mismatch at slot %d: %d != %d", i, blobCount, commitmentsCount)
		}
		if commitmentsCount == 0 {
			if err := sn.AddWord(nil); err != nil {
				return err
			}
			continue
		}
		sidecars, found, err := storage.ReadBlobSidecars(ctx, i, blockRoot)
		if err != nil {
			return err
		}
		if uint64(len(sidecars)) != blobCount {
			return fmt.Errorf("blob sidecars count mismatch at slot %d: %d != %d", i, len(sidecars), blobCount)
		}
		if !found {
			return fmt.Errorf("blob sidecars not found for block %d", i)
		}
		reusableBuf = reusableBuf[:0]
		// Make a concatenated SSZ of all sidecars.
		for _, sidecar := range sidecars {
			reusableBuf, err = sidecar.EncodeSSZ(reusableBuf)
			if err != nil {
				return err
			}
		}

		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping beacon blobs", "progress", i)
		}
		if err := sn.AddWord(reusableBuf); err != nil {
			return err
		}

	}
	tx.Rollback()
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	if err := removeBlobIndexAndTorrents(snapDir, fromSlot, toSlot); err != nil {
		return err
	}
	if err := os.Rename(stagingPath, f.Path); err != nil {
		return err
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return snapshotsync.BeaconSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func removeBlobIndexAndTorrents(snapDir string, from, to uint64) error {
	idx := snaptype.IdxFileMask(from, to, snaptype.CaplinIndexes.BlobSidecarSlot.Name)
	masks := []string{idx, idx + ".torrent", snaptype.SegmentFileMask(from, to, snaptype.CaplinEnums.BlobSidecars) + ".torrent"}
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if slices.ContainsFunc(masks, func(mask string) bool {
			matched, _ := filepath.Match(mask, e.Name())
			return matched
		}) {
			if err := dir.RemoveFile(filepath.Join(snapDir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func DumpBeaconBlocks(ctx context.Context, db kv.RoDB, fromSlot, toSlot uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	for i := fromSlot; i+snaptype.CaplinMergeLimit <= toSlot; i += snaptype.CaplinMergeLimit {
		to := i + snaptype.CaplinMergeLimit
		logger.Log(lvl, "Dumping beacon blocks", "from", i, "to", to)
		if err := dumpBeaconBlocksRange(ctx, db, i, to, salt, dirs, workers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

type BlobCountBySlotFn func(tx kv.Tx, slot uint64) (count uint64, payloadSkipped bool, err error)

func BlobCountFromBlocks(ctx context.Context, sn *CaplinSnapshots, r BeaconSnapshotReader) BlobCountBySlotFn {
	return func(tx kv.Tx, slot uint64) (uint64, bool, error) {
		view := sn.View()
		_, ok := view.BeaconBlocksSegment(slot)
		view.Close()
		if !ok {
			return 0, false, fmt.Errorf("slot %d: no frozen beacon block segment to count its blobs against", slot)
		}
		block, root, err := readCanonicalBlock(ctx, tx, r, slot)
		if err != nil || block == nil {
			return 0, false, err
		}
		commitments := block.Block.Body.GetBlobKzgCommitments()
		if commitments == nil || commitments.Len() == 0 {
			return 0, false, nil
		}
		if block.Version() >= clparams.GloasVersion {
			skipped, err := PayloadSkipped(ctx, tx, sn, r, block, root)
			if err != nil || skipped {
				return 0, skipped, err
			}
		}
		return uint64(commitments.Len()), false, nil
	}
}

func PayloadSkipped(ctx context.Context, tx kv.Tx, sn *CaplinSnapshots, r BeaconSnapshotReader, block *cltypes.SignedBeaconBlock, root common.Hash) (bool, error) {
	finalized, err := beacon_indicies.ReadHighestFinalized(tx)
	if err != nil {
		return false, err
	}
	checkpoint := finalized / sn.beaconCfg.SlotsPerEpoch * sn.beaconCfg.SlotsPerEpoch
	for slot := block.Block.Slot + 1; slot <= max(sn.BlocksAvailable(), checkpoint); slot++ {
		child, _, err := readCanonicalBlock(ctx, tx, r, slot)
		if err != nil {
			return false, err
		}
		if child == nil {
			continue
		}
		if child.Version() < clparams.GloasVersion {
			return false, fmt.Errorf("block at slot %d: pre-Gloas block after a Gloas block", slot)
		}
		return child.Block.ParentRoot == root && child.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash != block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash, nil
	}
	return false, nil
}

func readCanonicalBlock(ctx context.Context, tx kv.Tx, r BeaconSnapshotReader, slot uint64) (block *cltypes.SignedBeaconBlock, root common.Hash, err error) {
	if err := ctx.Err(); err != nil {
		return nil, common.Hash{}, err
	}
	block, err = r.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
	if err != nil {
		return nil, common.Hash{}, err
	}
	if block == nil || block.Version() < clparams.GloasVersion {
		return block, common.Hash{}, nil
	}
	root, err = block.Block.HashSSZ()
	if err != nil {
		return nil, common.Hash{}, err
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return nil, common.Hash{}, err
	}
	if block.Block.Slot != slot || root != canonicalRoot {
		return nil, common.Hash{}, fmt.Errorf("block at slot %d is not canonical", slot)
	}
	return block, root, nil
}

func DumpBlobsSidecar(ctx context.Context, blobStorage blob_storage.BlobStorage, db kv.RoDB, fromSlot, toSlot uint64, salt uint32, dirs datadir.Dirs, compressWorkers int, blobCountFn BlobCountBySlotFn, lvl log.Lvl, logger log.Logger) error {
	cfg := snapcfg.KnownCfgOrDevnet("")
	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, cfg) {
		blocksPerFile := snapcfg.MergeLimitFromCfg(cfg, snaptype.CaplinEnums.BlobSidecars, i)

		if toSlot-i < blocksPerFile {
			break
		}
		to := chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, cfg)
		logger.Log(lvl, "Dumping blobs sidecars", "from", i, "to", to)
		if err := DumpBlobSidecarsRange(ctx, db, blobStorage, i, to, salt, dirs, compressWorkers, blobCountFn, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

// BuildMissingIndices indexes every caplin segment that is on disk but not indexed yet.
// It opens the folder first because the dirty set it walks only exists after an open, and
// the antiquary calls this before its own OpenFolder. That open is optimistic so that a
// corrupt .idx reaches the dirty set unindexed and gets rebuilt here, rather than failing
// the open and leaving the node with no way to repair it.
func (s *CaplinSnapshots) BuildMissingIndices(ctx context.Context, logger log.Logger) error {
	if s == nil {
		return nil
	}
	if err := s.openFolder(true, nil); err != nil {
		return err
	}
	return s.IndexMissed(ctx, "caplin", s.tmpdir, nil, logger)
}

// ReadFrozenBeaconBlockBodyForIntegrity validates the stored body root before promoting blinded blocks.
func (s *CaplinSnapshots) ReadFrozenBeaconBlockBodyForIntegrity(slot uint64) (*cltypes.SignedBeaconBlock, error) {
	sn, ok, closeSegment := s.ViewSingleFile(snaptype.BeaconBlocks, slot)
	defer closeSegment()
	if !ok {
		return nil, nil
	}

	buf, err := sn.Get(slot)
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, nil
	}
	reader, err := getZstdReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	defer putZstdReader(reader)
	return snapshot_format.ReadBeaconBlockBodyFromSnapshotForIntegrity(reader, s.beaconCfg)
}

func (s *CaplinSnapshots) ReadHeader(slot uint64, tx kv.Tx) (*cltypes.SignedBeaconBlockHeader, uint64, common.Hash, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("ReadHeader(%d), %s, %s\n", slot, rec, dbg.Stack()))
		}
	}()

	sn, ok, closeSegment := s.ViewSingleFile(snaptype.BeaconBlocks, slot)
	defer closeSegment()
	if !ok {
		return nil, 0, common.Hash{}, nil
	}

	buf, err := sn.Get(slot)
	if err != nil {
		return nil, 0, common.Hash{}, err
	}
	if len(buf) == 0 {
		return nil, 0, common.Hash{}, nil
	}
	// Decompress this thing
	reader, err := getZstdReader(bytes.NewReader(buf))
	if err != nil {
		return nil, 0, common.Hash{}, err
	}
	defer putZstdReader(reader)

	// Use pooled readers to avoid allocations.
	header, elBlockNumber, elBlockHash, err := snapshot_format.ReadBlockHeaderFromSnapshotWithExecutionData(reader, s.beaconCfg)
	if err != nil {
		return nil, 0, common.Hash{}, err
	}

	// [New in Gloas:EIP7732] The beacon block snapshot for GLOAS contains no ExecutionPayload,
	// so ReadBlockHeaderFromSnapshotWithExecutionData returns 0/zero for the execution indices.
	// For FULL blocks the indices were written to KV by WriteExecutionPayloadEnvelopeIndicies
	// when the envelope arrived; fall back to reading them here if a transaction is provided.
	if tx != nil && header != nil && elBlockNumber == 0 && elBlockHash == (common.Hash{}) {
		epoch := slot / s.beaconCfg.SlotsPerEpoch
		if s.beaconCfg.GetCurrentStateVersion(epoch) >= clparams.GloasVersion {
			blockRoot, err := header.Header.HashSSZ()
			if err != nil {
				return nil, 0, common.Hash{}, err
			}
			n, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
			if err != nil {
				return nil, 0, common.Hash{}, err
			}
			if n != nil {
				elBlockNumber = *n
			}
			elBlockHash, err = beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
			if err != nil {
				return nil, 0, common.Hash{}, err
			}
		}
	}

	return header, elBlockNumber, elBlockHash, nil
}

func (s *CaplinSnapshots) ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error) {
	sn, ok, closeSegment := s.ViewSingleFile(snaptype.BlobSidecars, slot)
	defer closeSegment()
	if !ok {
		return nil, nil
	}

	buf, err := sn.Get(slot)
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf)%sidecarSSZSize != 0 {
		return nil, errors.New("invalid sidecar list length")
	}
	sidecars := make([]*cltypes.BlobSidecar, len(buf)/sidecarSSZSize)
	for i := 0; i < len(buf); i += sidecarSSZSize {
		sidecars[i/sidecarSSZSize] = &cltypes.BlobSidecar{}
		if err := sidecars[i/sidecarSSZSize].DecodeSSZ(buf[i:i+sidecarSSZSize], int(clparams.DenebVersion)); err != nil {
			return nil, err
		}
	}
	return sidecars, nil
}

// FrozenBlobs is exclusive - the first slot NOT frozen - because its consumers compare
// with `slot < FrozenBlobs()` and resume dumping at it. Do not swap in a To-1 accessor.
func (s *CaplinSnapshots) FrozenBlobs() uint64 {
	if s.beaconCfg.DenebForkEpoch == math.MaxUint64 {
		return 0
	}
	return s.VisibleSegmentsMaxTo(snaptype.BlobSidecars.Enum())
}

// RetireBlobsBelow retires blob sidecar segments holding no slot at or above floor and hands
// their files to onDelete so the seeder drops them. The newest segment always stays, so
// FrozenBlobs and the backfill boundary it sets do not move. It waits while superseded segments
// are still on disk: retiring the visible copy of a range would surface the hidden one and cut
// visibility at the next gap.
func (s *CaplinSnapshots) RetireBlobsBelow(floor uint64, onDelete func([]string) error) (bool, error) {
	if floor == 0 {
		return false, nil
	}
	var dirty int
	s.WalkDirtySegments(snaptype.BlobSidecars.Enum(), func(*snapshotsync.DirtySegment) bool {
		dirty++
		return true
	})
	view := s.View()
	visible := len(view.BlobSidecars())
	view.Close()
	if dirty != visible {
		s.logger.Debug("Skipping blob retirement while dirty and visible segment counts differ", "dirty", dirty, "visible", visible)
		return false, nil
	}
	return s.RetireFilesBelow(snaptype.BlobSidecars, min(floor+1, s.FrozenBlobs()), onDelete)
}
