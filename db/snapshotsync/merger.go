package snapshotsync

import (
	"cmp"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/mvcc"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
)

type Merger struct {
	lvl             log.Lvl
	compressWorkers int
	tmpDir          string
	chainConfig     *chain.Config
	chainDB         kv.RoDB
	logger          log.Logger
	noFsync         bool // fsync is enabled by default, but tests can manually disable
	snCfg           *snapcfg.Cfg
}

func NewMerger(tmpDir string, compressWorkers int, lvl log.Lvl, chainDB kv.RoDB, chainConfig *chain.Config, logger log.Logger) *Merger {
	snCfg := snapcfg.KnownCfgOrDevnet(chainConfig.ChainName)
	return &Merger{tmpDir: tmpDir, compressWorkers: compressWorkers, lvl: lvl, chainDB: chainDB, chainConfig: chainConfig, logger: logger, snCfg: snCfg}
}
func (m *Merger) DisableFsync() { m.noFsync = true }

func (m *Merger) FindMergeRanges(currentRanges []Range, snapType snaptype.Enum) (toMerge []Range) {
	cfg := m.snCfg
	for i := len(currentRanges) - 1; i > 0; i-- {
		r := currentRanges[i]
		mergeLimit := cfg.MergeLimit(snapType, r.From())
		if r.To()-r.From() >= mergeLimit {
			continue
		}
		for _, span := range snapcfg.MergeStepsFromCfg(cfg, snapType, r.From()) {
			if r.To()%span != 0 {
				continue
			}
			if r.To()-r.From() == span {
				break
			}
			aggFrom := r.To() - span
			if aggFrom < currentRanges[0].From() {
				continue
			}
			toMerge = append(toMerge, NewRange(aggFrom, r.To()))
			for i >= 0 && currentRanges[i].From() > aggFrom {
				i--
			}
			break
		}
	}
	slices.SortFunc(toMerge, func(i, j Range) int { return cmp.Compare(i.From(), j.From()) })
	return toMerge
}

func (m *Merger) filesByRange(v *View, snapTypes []snaptype.Type, from, to uint64) (map[snaptype.Enum][]*DirtySegment, error) {
	toMerge := map[snaptype.Enum][]*DirtySegment{}
	for _, t := range snapTypes {
		toMerge[t.Enum()] = m.filesByRangeOfType(v, from, to, t)
	}

	return toMerge, nil
}

func (m *Merger) filesByRangeOfType(view *View, from, to uint64, snapshotType snaptype.Type) (out []*DirtySegment) {
	for _, sn := range view.Segments(snapshotType) {
		if sn.from < from {
			continue
		}
		if sn.To() > to {
			break
		}

		out = append(out, sn.src)
	}
	return
}

// removeMergedFiles removes a merge output's files. A same-range input with an older segment version resolves to the
// output's index names, so its index is kept unless the output's index was built: that replaced the file.
func removeMergedFiles(sn snaptype.FileInfo, inputs []*DirtySegment, indexBuilt bool) {
	_ = dir.RemoveFile(sn.Path)
	_ = dir.RemoveFile(sn.Path + ".torrent")
	for _, idx := range sn.Type.IdxFileNames(sn.From, sn.To) {
		idxPath := filepath.Join(filepath.Dir(sn.Path), idx)
		if !indexBuilt && slices.ContainsFunc(inputs, func(in *DirtySegment) bool {
			return slices.ContainsFunc(in.indexes, func(x *recsplit.Index) bool { return x != nil && x.FilePath() == idxPath })
		}) {
			continue
		}
		_ = dir.RemoveFile(idxPath)
		_ = dir.RemoveFile(idxPath + ".torrent")
	}
}

func (m *Merger) mergeSubSegment(
	ctx context.Context,
	sn snaptype.FileInfo,
	toMerge []*DirtySegment,
	snapDir string,
	doIndex bool,
	indexBuilder snaptype.IndexBuilder,
) (newDirtySegment *DirtySegment, err error) {
	indexBuilt := false
	defer func() {
		if err == nil {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic: %v", rec)
			}
		}
		if err != nil {
			newDirtySegment.close()
			removeMergedFiles(sn, toMerge, indexBuilt)
		}
	}()
	if len(toMerge) == 0 {
		return
	}
	if newDirtySegment, err = m.merge(ctx, toMerge, sn, snapDir); err != nil {
		err = fmt.Errorf("mergeByAppendSegments: %w", err)
		return
	}

	// new way to build index
	if doIndex {
		p := &background.Progress{}
		indexBuilt = true // a partial build may already have replaced an index file
		if err = buildIdx(ctx, sn, indexBuilder, m.chainConfig, m.tmpDir, p, m.lvl, m.logger); err != nil {
			return
		}
		err = newDirtySegment.openIdx(snapDir, nil)
		if err != nil {
			return
		}
	}

	return
}

func buildIdx(ctx context.Context, sn snaptype.FileInfo, indexBuilder snaptype.IndexBuilder, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	//log.Info("[snapshots] build idx", "file", sn.Name())
	if err := sn.Type.BuildIndexes(ctx, sn, indexBuilder, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return fmt.Errorf("buildIdx: %s: %w", sn.Type, err)
	}
	//log.Info("[snapshots] finish build idx", "file", fName)
	return nil
}

// Merge does merge segments in given ranges
func (m *Merger) Merge(
	ctx context.Context,
	snapshots *BaseRoSnapshots,
	snapTypes []snaptype.Type,
	mergeRanges []Range,
	snapDir string,
	doIndex bool,
	onMerge func(mergedFileNames []string) error,
	onDelete func(context.Context, []string) error,
) (err error) {
	v := snapshots.View()
	defer v.Close()

	if len(mergeRanges) == 0 {
		return nil
	}

	// Claim every (type, range) we're about to produce before writing any file,
	// so openers skip the not-yet-indexed output and no other builder races us.
	// A range we can't fully claim is being built elsewhere — skip it this pass.
	claimed := make([]Range, 0, len(mergeRanges))
	defer func() {
		for _, r := range claimed {
			for _, t := range snapTypes {
				snapshots.ReleaseRange(t.Enum(), r.From(), r.To())
			}
		}
	}()
	for _, r := range mergeRanges {
		ok := true
		for i, t := range snapTypes {
			if !snapshots.TryAcquireRange(t.Enum(), r.From(), r.To()) {
				for _, t2 := range snapTypes[:i] {
					snapshots.ReleaseRange(t2.Enum(), r.From(), r.To())
				}
				ok = false
				break
			}
		}
		if ok {
			claimed = append(claimed, r)
		}
	}
	if len(claimed) == 0 {
		return nil
	}
	mergeRanges = claimed

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	mergedFileNames := make([]string, 0, 16)
	var in map[snaptype.Enum][]*DirtySegment
	defer func() { // segments of a range that failed before integration
		for _, segs := range in {
			for _, s := range segs {
				s.close()
			}
		}
	}()

	for _, r := range mergeRanges {
		mergedFileNames = mergedFileNames[:0]
		in = make(map[snaptype.Enum][]*DirtySegment)
		out := make(map[snaptype.Enum][]*DirtySegment)
		toMerge, err := m.filesByRange(v, snapTypes, r.From(), r.To())
		if err != nil {
			return err
		}
		for snapType, t := range toMerge {
			if out[snapType] == nil {
				out[snapType] = make([]*DirtySegment, 0, len(t))
			}
			out[snapType] = append(out[snapType], t...)
		}

		built := make([]*DirtySegment, 0, len(snapTypes))
		outputs := make([]snaptype.FileInfo, 0, len(snapTypes))
		var inputs []*DirtySegment
		builtFileNames := make([]string, 0, len(snapTypes))
		samePathTypes := make(map[snaptype.Enum]struct{})
		abort := func() { // a later OpenFolder must not adopt the outputs of an unfinished range
			for _, sn := range built {
				sn.close()
			}
			if onDelete != nil {
				_ = onDelete(ctx, slices.Clone(builtFileNames))
			}
			for i := range outputs {
				removeMergedFiles(outputs[i], inputs, doIndex)
			}
		}
		for _, t := range snapTypes {
			output := t.FileInfo(snapDir, r.From(), r.To())
			if len(toMerge[t.Enum()]) == 1 && toMerge[t.Enum()][0].FilePath() == output.Path {
				samePathTypes[t.Enum()] = struct{}{}
				out[t.Enum()] = nil
				mergedFileNames = append(mergedFileNames, toMerge[t.Enum()][0].FilePath())
				continue
			}
			newDirtySegment, err := m.mergeSubSegment(
				ctx,
				output,
				toMerge[t.Enum()],
				snapDir,
				doIndex,
				snapshots.IndexBuilder(t),
			)
			if err != nil {
				abort()
				return err
			}
			if in[t.Enum()] == nil {
				in[t.Enum()] = make([]*DirtySegment, 0, len(toMerge[t.Enum()]))
			}
			in[t.Enum()] = append(in[t.Enum()], newDirtySegment)
			built = append(built, newDirtySegment)
			outputs = append(outputs, output)
			inputs = append(inputs, toMerge[t.Enum()]...)
			fileName := newDirtySegment.FilePath()
			mergedFileNames = append(mergedFileNames, fileName)
			builtFileNames = append(builtFileNames, fileName)
		}

		snapshots.LogStat("merge")

		if onMerge != nil {
			if err := onMerge(slices.Clone(mergedFileNames)); err != nil {
				abort()
				return err
			}
		}

		toMergeFileNames := make([]string, 0, 16)
		for enum, segments := range toMerge {
			if _, ok := samePathTypes[enum]; ok {
				continue
			}
			for _, segment := range segments {
				toMergeFileNames = append(toMergeFileNames, segment.FilePaths(snapDir)...)
			}
		}
		if onDelete != nil {
			if err := onDelete(ctx, toMergeFileNames); err != nil {
				return fmt.Errorf("merger.Merge: onDelete: %w", err)
			}
		}
		m.integrateMergedDirtyFiles(snapshots, in, out)
		in = nil
	}
	m.logger.Log(m.lvl, "[snapshots] Merge done", "ranges", len(mergeRanges), "from", mergeRanges[0].from, "to", mergeRanges[len(mergeRanges)-1].to)
	return nil
}

func (m *Merger) integrateMergedDirtyFiles(snapshots *BaseRoSnapshots, in, out map[snaptype.Enum][]*DirtySegment) {
	var retired retiredSegments

	snapshots.dirtyLock.Lock()
	defer snapshots.dirtyLock.Unlock()
	// Publish under the same lock so the dirty mutation and the bundle publish are one
	// atomic step (no window for a concurrent open to re-adopt a just-retired file).
	defer func() {
		retire(mvcc.RetireReasonMerged, retired) // merge output is on disk and must be deleted on reclaim
		snapshots.recalcVisibleFiles(snapshots.alignMin, retired)
	}()

	// add new segments
	for enum, newSegs := range in {
		dirtySegments := snapshots.dirty[enum]
		for _, newSeg := range newSegs {
			dirtySegments.Set(newSeg)
			if newSeg.frozen {
				dirtySegments.Walk(func(items []*DirtySegment) bool {
					for _, item := range items {
						if item.frozen || item.from < newSeg.from || item.to > newSeg.to {
							continue
						}
						if out[enum] == nil {
							out[enum] = make([]*DirtySegment, 0, 1)
						}
						out[enum] = append(out[enum], item)
					}
					return true
				})
			}
		}
	}

	// delete old sub segments. out may list the same segment more than once (a merge
	// output subsumes the same subs the frozen-walk re-collects); dedup so each is
	// retired — and later unlinked — exactly once.
	seen := make(map[*DirtySegment]struct{})
	for enum, delSegs := range out {
		dirtySegments := snapshots.dirty[enum]
		inDirtySegments := in[enum]

		for _, delSeg := range delSegs {
			skip := false
			for _, inDSeg := range inDirtySegments {
				if inDSeg.from == delSeg.from && inDSeg.to == delSeg.to {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
			if _, ok := seen[delSeg]; ok {
				continue
			}
			seen[delSeg] = struct{}{}

			dirtySegments.Delete(delSeg)
			retired = append(retired, delSeg)
		}
	}
}

func (m *Merger) merge(ctx context.Context, toMerge []*DirtySegment, targetFile snaptype.FileInfo, snapDir string) (*DirtySegment, error) {
	word := make([]byte, 0, 4096)
	var expectedTotal int
	cList := make([]*seg.Decompressor, len(toMerge))
	defer func() {
		for _, d := range cList {
			if d != nil {
				d.Close()
			}
		}
	}()
	for i, cFile := range toMerge {
		d, err := seg.NewDecompressor(cFile.FilePath())
		if err != nil {
			return nil, err
		}
		cList[i] = d
		expectedTotal += d.Count()
	}

	compresCfg := seg.DefaultCfg
	compresCfg.Workers = m.compressWorkers
	f, err := seg.NewCompressor(ctx, "Snapshots merge", targetFile.Path, m.tmpDir, compresCfg, log.LvlTrace, m.logger)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if m.noFsync {
		f.DisableFsync()
	}
	m.logger.Debug("[snapshots] merge", "file", targetFile.Name())

	for _, d := range cList {
		if err := func() error {
			view, err := d.OpenSequentialView()
			if err != nil {
				return err
			}
			defer view.Close()
			g := view.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				if err := f.AddWord(word); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}
	if f.Count() != expectedTotal {
		return nil, fmt.Errorf("unexpected amount after segments merge. got: %d, expected: %d", f.Count(), expectedTotal)
	}
	if err := f.Compress(); err != nil {
		return nil, err
	}
	sn := &DirtySegment{
		segType: targetFile.Type,
		version: targetFile.Version,
		Range:   Range{targetFile.From, targetFile.To},
		frozen:  m.snCfg.IsFrozen(targetFile),
	}

	err = sn.Open(snapDir)
	if err != nil {
		return nil, err
	}
	return sn, nil
}
