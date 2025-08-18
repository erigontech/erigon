package snapshotsync

import (
	"cmp"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
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
}

func NewMerger(tmpDir string, compressWorkers int, lvl log.Lvl, chainDB kv.RoDB, chainConfig *chain.Config, logger log.Logger) *Merger {
	return &Merger{tmpDir: tmpDir, compressWorkers: compressWorkers, lvl: lvl, chainDB: chainDB, chainConfig: chainConfig, logger: logger}
}
func (m *Merger) DisableFsync() { m.noFsync = true }

func (m *Merger) FindMergeRanges(currentRanges []Range, maxBlockNum uint64) (toMerge []Range) {
	cfg, _ := snapcfg.KnownCfg(m.chainConfig.ChainName)
	for i := len(currentRanges) - 1; i > 0; i-- {
		r := currentRanges[i]
		mergeLimit := cfg.MergeLimit(snaptype.Unknown, r.From())
		if r.To()-r.From() >= mergeLimit {
			continue
		}
		for _, span := range snapcfg.MergeStepsFromCfg(cfg, snaptype.Unknown, r.From()) {
			if r.To()%span != 0 {
				continue
			}
			if r.To()-r.From() == span {
				break
			}
			aggFrom := r.To() - span
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

func (m *Merger) filesByRange(v *View, from, to uint64) (map[snaptype.Enum][]*DirtySegment, error) {
	toMerge := map[snaptype.Enum][]*DirtySegment{}
	for _, t := range v.s.types {
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

func (m *Merger) mergeSubSegment(ctx context.Context, v *View, sn snaptype.FileInfo, toMerge []*DirtySegment, snapDir string, doIndex bool, indexBuilder snaptype.IndexBuilder, onMerge func(r Range) error) (newDirtySegment *DirtySegment, err error) {
	defer func() {
		if err == nil {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic: %v", rec)
			}
		}
		if err != nil {
			f := sn.Path
			_ = dir.RemoveFile(f)
			_ = dir.RemoveFile(f + ".torrent")
			ext := filepath.Ext(f)
			withoutExt := f[:len(f)-len(ext)]
			_ = dir.RemoveFile(withoutExt + ".idx")
			_ = dir.RemoveFile(withoutExt + ".idx.torrent")
			isTxnType := strings.HasSuffix(withoutExt, snaptype2.Transactions.Name())
			if isTxnType {
				_ = dir.RemoveFile(withoutExt + "-to-block.idx")
				_ = dir.RemoveFile(withoutExt + "-to-block.idx.torrent")
			}
		}
	}()
	if len(toMerge) == 0 {
		return
	}
	if newDirtySegment, err = m.merge(ctx, v, toMerge, sn, snapDir, nil); err != nil {
		err = fmt.Errorf("mergeByAppendSegments: %w", err)
		return
	}

	// new way to build index
	if doIndex {
		p := &background.Progress{}
		if err = buildIdx(ctx, sn, indexBuilder, m.chainConfig, m.tmpDir, p, m.lvl, m.logger); err != nil {
			return
		}
		err = newDirtySegment.openIdx(snapDir)
		if err != nil {
			return
		}
	}

	return
}

func buildIdx(ctx context.Context, sn snaptype.FileInfo, indexBuilder snaptype.IndexBuilder, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	//log.Info("[snapshots] build idx", "file", sn.Name())
	if err := sn.Type.BuildIndexes(ctx, sn, indexBuilder, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return fmt.Errorf("buildIdx: %s: %s", sn.Type, err)
	}
	//log.Info("[snapshots] finish build idx", "file", fName)
	return nil
}

// Merge does merge segments in given ranges
func (m *Merger) Merge(ctx context.Context, snapshots *RoSnapshots, snapTypes []snaptype.Type, mergeRanges []Range, snapDir string, doIndex bool, onMerge func(r Range) error, onDelete func(l []string) error) (err error) {
	v := snapshots.View()
	defer v.Close()

	if len(mergeRanges) == 0 {
		return nil
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	in := make(map[snaptype.Enum][]*DirtySegment)
	out := make(map[snaptype.Enum][]*DirtySegment)

	for _, r := range mergeRanges {
		toMerge, err := m.filesByRange(v, r.From(), r.To())
		if err != nil {
			return err
		}
		for snapType, t := range toMerge {
			if out[snapType] == nil {
				out[snapType] = make([]*DirtySegment, 0, len(t))
			}
			out[snapType] = append(out[snapType], t...)
		}

		for _, t := range snapTypes {
			newDirtySegment, err := m.mergeSubSegment(ctx, v, t.FileInfo(snapDir, r.From(), r.To()), toMerge[t.Enum()], snapDir, doIndex, snapshots.IndexBuilder(t), onMerge)
			if err != nil {
				return err
			}
			if in[t.Enum()] == nil {
				in[t.Enum()] = make([]*DirtySegment, 0, len(toMerge[t.Enum()]))
			}
			in[t.Enum()] = append(in[t.Enum()], newDirtySegment)
		}

		snapshots.LogStat("merge")

		if onMerge != nil {
			if err := onMerge(r); err != nil {
				return err
			}
		}

		//TODO: or move it inside `integrateMergedDirtyFiles`, or move `integrateMergedDirtyFiles` here. Merge can be long - means call `integrateMergedDirtyFiles` earliear can make sense.
		toMergeFileNames := make([]string, 0, 16)
		for _, segments := range toMerge {
			for _, segment := range segments {
				toMergeFileNames = append(toMergeFileNames, segment.FilePaths(snapDir)...)
			}
		}
		if onDelete != nil {
			if err := onDelete(toMergeFileNames); err != nil {
				return fmt.Errorf("merger.Merge: onDelete: %w", err)
			}
		}
	}
	m.integrateMergedDirtyFiles(snapshots, in, out)
	m.logger.Log(m.lvl, "[snapshots] Merge done", "from", mergeRanges[0].from, "to", mergeRanges[0].to)
	return nil
}

func (m *Merger) integrateMergedDirtyFiles(snapshots *RoSnapshots, in, out map[snaptype.Enum][]*DirtySegment) {
	defer snapshots.recalcVisibleFiles(snapshots.alignMin)

	snapshots.dirtyLock.Lock()
	defer snapshots.dirtyLock.Unlock()

	// add new segments
	for enum, newSegs := range in {
		dirtySegments := snapshots.dirty[enum]
		for _, newSeg := range newSegs {
			dirtySegments.Set(newSeg)
			if newSeg.frozen {
				dirtySegments.Walk(func(items []*DirtySegment) bool {
					for _, item := range items {
						if item.frozen || item.to > newSeg.to {
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

	// delete old sub segments
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

			dirtySegments.Delete(delSeg)
			delSeg.canDelete.Store(true)
		}
	}
}

func (m *Merger) merge(ctx context.Context, v *View, toMerge []*DirtySegment, targetFile snaptype.FileInfo, snapDir string, logEvery *time.Ticker) (*DirtySegment, error) {
	var word = make([]byte, 0, 4096)
	var expectedTotal int
	cList := make([]*seg.Decompressor, len(toMerge))
	for i, cFile := range toMerge {
		d, err := seg.NewDecompressor(cFile.FilePath())
		if err != nil {
			return nil, err
		}
		defer d.Close()
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
		if err := d.WithReadAhead(func() error {
			g := d.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				if err := f.AddWord(word); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if f.Count() != expectedTotal {
		return nil, fmt.Errorf("unexpected amount after segments merge. got: %d, expected: %d", f.Count(), expectedTotal)
	}
	if err = f.Compress(); err != nil {
		return nil, err
	}
	sn := &DirtySegment{segType: targetFile.Type, version: targetFile.Version, Range: Range{targetFile.From, targetFile.To},
		frozen: snapcfg.Seedable(v.s.cfg.ChainName, targetFile)}

	err = sn.Open(snapDir)
	if err != nil {
		return nil, err
	}
	return sn, nil
}
