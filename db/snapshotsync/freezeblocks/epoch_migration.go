// Copyright 2026 The Erigon Authors
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

// Turns old decimal block segments (named by block/1000) into epoch ones ("ep"-marked, block/1024),
// at startup before anything opens a segment.
//
// Crash-safe: we drop a decimal segment only after every block in it is safe elsewhere — in an epoch
// segment, or (for the sub-1024 tail) in the DB. Segments and indexes are written to a temp file then
// renamed, so anything on disk is complete and a re-run after a crash just carries on.

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// planEpochSegments cuts [from, frozenMax) into epoch segment ranges. tailFrom is where the leftover
// too small for a segment starts — those blocks go to the DB.
func planEpochSegments(from, frozenMax uint64, snCfg *snapcfg.Cfg) (segs [][2]uint64, tailFrom uint64) {
	for {
		bf, bt, can := snapshotsync.CanRetire(true, from, frozenMax, snaptype.Unknown, snCfg, 0)
		if !can {
			return segs, from
		}
		segs = append(segs, [2]uint64{bf, bt})
		from = bt
	}
}

func sortedNoOverlaps(segs []snaptype.FileInfo) []snaptype.FileInfo {
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].From != segs[j].From {
			return segs[i].From < segs[j].From
		}
		return segs[i].To > segs[j].To
	})
	out := segs[:0:0]
	var maxTo uint64
	for i := range segs {
		if len(out) == 0 || segs[i].To > maxTo {
			out = append(out, segs[i])
			maxTo = segs[i].To
		}
	}
	return out
}

// classifyByType splits one dir scan into type t's epoch ("ep"-marked) and decimal segments, sorted
// and overlap-free. We scan the dir once and reuse this everywhere.
func classifyByType(all []snaptype.FileInfo, t snaptype.Enum) (epoch, decimal []snaptype.FileInfo) {
	for i := range all {
		f := &all[i]
		switch {
		case f.Type == nil || f.Type.Enum() != t:
		case f.Epoch:
			epoch = append(epoch, *f)
		default:
			decimal = append(decimal, *f)
		}
	}
	return sortedNoOverlaps(epoch), sortedNoOverlaps(decimal)
}

// coverage walks one type as a single run and returns:
//   - start: where conversion resumes. Either the end of the epoch segments already written, or, for a
//     type with none, its own data frontier — which a pruned node leaves far above block 0. The caller
//     snaps it to the shared tiling.
//   - coveredTo: how far the run reaches. frozenMax is the smallest coveredTo across types.
//   - runLen: how many decimal segments from the front have no gaps — the ones a repack reads.
//
// A gap stops both the run and coveredTo. Inputs must be sorted and overlap-free.
func coverage(epoch, decimal []snaptype.FileInfo) (start, coveredTo uint64, runLen int) {
	epochEnd := epochRunEnd(epoch)
	haveEpoch := len(epoch) > 0
	var decimalStart, decimalEnd uint64
	if len(decimal) > 0 {
		decimalStart = decimal[0].From
		decimalEnd = decimalStart
		for i := range decimal {
			if decimal[i].From != decimalEnd {
				break // gap: the contiguous decimal run ends here
			}
			runLen++
			decimalEnd = decimal[i].To
		}
	}

	switch {
	case runLen == 0:
		return epochEnd, epochEnd, 0
	case !haveEpoch:
		return decimalStart, decimalEnd, runLen
	case decimalStart > epochEnd:
		return 0, 0, 0 // epoch run and decimal run do not meet: leave this type alone
	default:
		return epochEnd, max(epochEnd, decimalEnd), runLen
	}
}

// epochRunEnd is where the gap-free run of epoch segments from the first one ends, 0 with none.
func epochRunEnd(epoch []snaptype.FileInfo) uint64 {
	if len(epoch) == 0 {
		return 0
	}
	end := epoch[0].From
	for i := range epoch {
		if epoch[i].From != end {
			break
		}
		end = epoch[i].To
	}
	return end
}

// dropConverted drops the decimal segments the epoch run already holds. The repacks delete the ones
// they read, but a crash between a merge and its cleanup leaves the merge inputs too, and once the
// epoch run is past them they would read as a gap before the decimal segment that continues it.
func dropConverted(epoch, decimal []snaptype.FileInfo) []snaptype.FileInfo {
	if len(epoch) == 0 {
		return decimal
	}
	end := epochRunEnd(epoch)
	i := 0
	for i < len(decimal) && decimal[i].To <= end {
		i++
	}
	return decimal[i:]
}

// planEpochConversion aligns each type's start up to the first smallest-tier boundary at or above it
// and tiles [0, frozenMax) so that every aligned start is a boundary. All types convert on this one
// tiling, because the transactions index is built by walking a bodies segment of the identical range.
// A pruned type gives up the blocks below its aligned start, fewer than one smallest tier.
//
// written holds where each type's epoch segments begin. After a crash a type that has some resumes at
// their end, so its aligned start is no longer in start, but the segments already written were cut
// at it, and the types still to convert must be too.
func planEpochConversion(start map[snaptype.Enum]uint64, written []uint64, frozenMax uint64, snCfg *snapcfg.Cfg) (plan [][2]uint64, tailFrom uint64) {
	cuts := slices.Clone(written)
	for t, s := range start {
		if r := s % snaptype.EpochMinSegmentSize; r != 0 {
			s += snaptype.EpochMinSegmentSize - r
		}
		start[t] = s
		cuts = append(cuts, s)
	}
	slices.Sort(cuts)
	var from uint64
	for _, cut := range slices.Compact(cuts) {
		if cut <= from || cut >= frozenMax {
			continue
		}
		segs, end := planEpochSegments(from, cut, snCfg)
		plan, from = append(plan, segs...), end
	}
	segs, tailFrom := planEpochSegments(from, frozenMax, snCfg)
	return append(plan, segs...), tailFrom
}

// planFrom is the part of the shared plan a type starting at startBlock converts.
func (m *epochMigrator) planFrom(startBlock uint64) [][2]uint64 {
	i, _ := slices.BinarySearchFunc(m.plan, startBlock, func(r [2]uint64, b uint64) int { return cmp.Compare(r[0], b) })
	return m.plan[i:]
}

// removeDecimalSegFiles deletes each decimal segment and everything with its range and type: the .seg,
// its .torrent and its indexes (an index may have a different version, so we match range+type with the
// version left wild). The trailing wildcard that picks up those extensions also picks up the epoch file
// printing the same digits, so each match is decoded and only the decimal ones are deleted.
//
// A removal that fails is ignored, as elsewhere in the snapshot code. On Windows it fails while
// anything still holds the file, and by this point its blocks are in an epoch segment, so the
// leftover is redundant: the next run sweeps it.
func removeDecimalSegFiles(segs []snaptype.FileInfo) error {
	for i := range segs {
		f := &segs[i]
		mask := filepath.Join(filepath.Dir(f.Path), snaptype.FileMask(false, f.From, f.To, f.TypeString)+"*")
		matches, err := filepath.Glob(mask)
		if err != nil {
			return err
		}
		for _, m := range matches {
			if fi, _, ok := snaptype.ParseFileName("", filepath.Base(m)); ok && fi.Epoch {
				continue
			}
			_ = dir.RemoveFile(m)
			if err := afterStep(); err != nil {
				return err
			}
		}
	}
	return nil
}

// seqWords reads a run of segments word by word in block order, opening one segment at a time and
// closing it before the next.
type seqWords struct {
	segs []snaptype.FileInfo
	i    int
	cur  *seg.Decompressor
	g    *seg.Getter
}

func newSeqWords(segs []snaptype.FileInfo) *seqWords { return &seqWords{segs: segs, i: -1} }

// next returns the next word, moving across segments and closing each one as it's finished. ok is
// false once all segments are read.
func (s *seqWords) next() (word []byte, ok bool, err error) {
	for {
		if s.g != nil && s.g.HasNext() {
			word, _ = s.g.Next(nil)
			return word, true, nil
		}
		if s.cur != nil {
			s.cur.Close()
			s.cur, s.g = nil, nil
		}
		s.i++
		if s.i >= len(s.segs) {
			return nil, false, nil
		}
		d, err := seg.NewDecompressor(s.segs[s.i].Path)
		if err != nil {
			return nil, false, err
		}
		s.cur, s.g = d, d.MakeGetter()
	}
}

func (s *seqWords) Close() {
	if s.cur != nil {
		s.cur.Close()
		s.cur = nil
	}
}

type epochMigrator struct {
	uncovered   []string // types with no usable segments; the refusal names them
	dirs        datadir.Dirs
	snCfg       *snapcfg.Cfg
	db          kv.RwDB
	chainConfig *chain.Config
	workers     int
	lvl         log.Lvl
	logger      log.Logger

	// decimalSegs holds each type's decimal segments (sorted, overlap-free) from the one startup scan.
	// contiguousLen is how many from the front have no gaps — the run we convert; anything past a gap
	// is left for the final cleanup. Repacks trim decimalSegs as they delete. loadSegs fills both.
	decimalSegs   map[snaptype.Enum][]snaptype.FileInfo
	contiguousLen map[snaptype.Enum]int
	// allDecimal is every decimal segment the scan found, including those a merged segment contains:
	// a crash between a merge and its cleanup leaves both, and cleanup has to remove both.
	allDecimal map[snaptype.Enum][]snaptype.FileInfo
	hasEpoch   map[snaptype.Enum]bool
	epochFrom  []uint64    // where each type's epoch segments begin
	plan       [][2]uint64 // the one tiling every type converts on
}

// crashHook is for tests: it runs after every step that changes the disk or the DB, and an error
// stops the migration right there, as a crash would.
var crashHook func() error

func afterStep() error {
	if crashHook != nil {
		return crashHook()
	}
	return nil
}

// epochMigrationKey marks a conversion that has started and not finished. Once the repacks delete the
// last decimal segment, the files alone no longer show that the indexes are still to be built.
var epochMigrationKey = []byte("epochMigrationInProgress")

func (m *epochMigrator) segsToMigrate(t snaptype.Enum) []snaptype.FileInfo {
	return m.decimalSegs[t][:m.contiguousLen[t]]
}

// repackWordPerBlock re-segments a one-word-per-block type (headers, bodies). It reads the decimal
// run one file at a time, skips blocks already in epoch segments ([firstFrom, startBlock)), and writes
// the epoch segments planned for [startBlock, frozenMax), copying each block's word as-is. As soon as
// an epoch segment is on disk, the decimal segments it fully covers get deleted, to keep peak disk low.
// The sub-1024 tail goes to onTail for the DB.
// startBlock is the type's epoch frontier, so a crash just resumes from there; frozenMax is the
// cross-type frontier.
func (m *epochMigrator) repackWordPerBlock(ctx context.Context, t snaptype.Type, startBlock, frozenMax uint64, onTail func(blockNum uint64, word []byte) error) error {
	if startBlock >= frozenMax {
		return nil
	}
	segs := m.segsToMigrate(t.Enum())
	plan := m.planFrom(startBlock)
	sw := newSeqWords(segs)
	defer sw.Close()

	blockNum := segs[0].From

	// skip blocks already in an epoch segment.
	for ; blockNum < startBlock; blockNum++ {
		if _, ok, err := sw.next(); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("epoch-migrate %s: ran out of words skipping to %d", t.Name(), startBlock)
		}
	}

	// decimal segments already covered by written epoch segments.
	delIdx := 0

	writeSeg := func(a, b uint64) error {
		compressCfg := BlockCompressCfg
		compressCfg.Workers = m.workers
		path := t.FileInfo(m.dirs.Snap, true, a, b).Path
		c, err := seg.NewCompressor(ctx, "epoch-migrate "+t.Name(), path, m.dirs.Tmp, compressCfg, m.lvl, m.logger)
		if err != nil {
			return err
		}
		defer c.Close()
		noCompress := segmentNoCompress(true, b-a)
		for ; blockNum < b; blockNum++ {
			w, ok, err := sw.next()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("epoch-migrate %s: ran out of words at block %d (segment [%d,%d))", t.Name(), blockNum, a, b)
			}
			if noCompress {
				if err := c.AddUncompressedWord(w); err != nil {
					return err
				}
			} else if err := c.AddWord(w); err != nil {
				return err
			}
		}
		if err := c.Compress(); err != nil {
			return err
		}
		return afterStep()
	}

	for _, r := range plan {
		if err := writeSeg(r[0], r[1]); err != nil {
			return err
		}
		start := delIdx
		for delIdx < len(segs) && segs[delIdx].To <= r[1] {
			delIdx++
		}
		if err := removeDecimalSegFiles(segs[start:delIdx]); err != nil {
			return err
		}
	}

	for ; blockNum < frozenMax; blockNum++ {
		w, ok, err := sw.next()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("epoch-migrate %s: ran out of tail words at block %d", t.Name(), blockNum)
		}
		if err := onTail(blockNum, w); err != nil {
			return err
		}
	}

	m.decimalSegs[t.Enum()] = m.decimalSegs[t.Enum()][delIdx:]
	return nil
}

func writeTailBlock(rwTx kv.RwTx, headerWord, bodyWord []byte, txWords [][]byte) error {
	var h types.Header
	if err := rlp.DecodeBytes(headerWord[1:], &h); err != nil {
		return fmt.Errorf("epoch-migrate tail: decode header: %w", err)
	}
	num := h.Number.Uint64()
	hash := h.Hash()
	if err := rawdb.WriteHeaderRaw(rwTx, num, hash, headerWord[1:], false); err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(rwTx, hash, num); err != nil {
		return err
	}
	if err := rwTx.Put(kv.BlockBody, dbutils.BlockBodyKey(num, hash), bodyWord); err != nil {
		return err
	}

	var b types.BodyForStorage
	if err := rlp.DecodeBytes(bodyWord, &b); err != nil {
		return fmt.Errorf("epoch-migrate tail: decode body %d: %w", num, err)
	}
	base := b.BaseTxnID.U64()
	senders := make([]common.Address, 0, len(txWords))
	lookup := make([]byte, 16)
	for i, w := range txWords {
		if i == 0 || i == len(txWords)-1 {
			continue
		}

		// segment word is firstByte(1) + sender(20) + txRLP; EthTx stores just the txRLP, keyed by the
		// global txn id (base + i, since begin-system is index 0).
		txnRlp := w[1+length.Addr:]
		if err := rwTx.Put(kv.EthTx, hexutil.EncodeTs(base+uint64(i)), txnRlp); err != nil {
			return err
		}
		senders = append(senders, common.BytesToAddress(w[1:1+length.Addr]))

		// while the block was frozen, hash->block lookups came from the tx-to-block index and
		// stage_txlookup pruned kv.TxLookup for this range. The block is out of the epoch index now, so
		// without this entry the txn can't be found by hash — and the stage won't rebuild it, it only
		// runs forward from the tip.
		txn, err := types.DecodeTransaction(txnRlp)
		if err != nil {
			return fmt.Errorf("epoch-migrate tail: decode txn %d of block %d: %w", i, num, err)
		}
		txnHash := txn.Hash()
		binary.BigEndian.PutUint64(lookup[:8], num)
		binary.BigEndian.PutUint64(lookup[8:], base+uint64(i))
		if err := rwTx.Put(kv.TxLookup, txnHash[:], lookup); err != nil {
			return err
		}
	}
	return rawdb.WriteSenders(rwTx, hash, num, senders)
}

// repackTransactions re-segments the transactions type. It's txn-granular, not block-granular: a block
// has exactly BodyForStorage.TxCount words (1 begin-system + TxCount-2 real + 1 end-system, or none if
// TxCount==0). So it reads bodies (for each block's TxCount and BaseTxnID) and transactions together,
// taking TxCount tx words per block and splitting on epoch boundaries. Tail blocks go to onTail with
// their BaseTxnID, needed to put them back in the EthTx table.
func (m *epochMigrator) repackTransactions(ctx context.Context, startBlock, frozenMax uint64, onTail func(blockNum, baseTxnID uint64, txnWords [][]byte) error) error {
	if startBlock >= frozenMax {
		return nil
	}
	bodySegs := m.segsToMigrate(snaptype2.Bodies.Enum())
	txSegs := m.segsToMigrate(snaptype2.Transactions.Enum())
	bodyFirstFrom := bodySegs[0].From
	txFirstFrom := txSegs[0].From
	plan := m.planFrom(startBlock)

	bodies := newSeqWords(bodySegs)
	defer bodies.Close()
	txs := newSeqWords(txSegs)
	defer txs.Close()

	// nextBlock reads one body word and returns that block's txn count and base txn id.
	nextBlock := func(blockNum uint64) (txCount, baseTxnID uint64, err error) {
		w, ok, err := bodies.next()
		if err != nil {
			return 0, 0, err
		}
		if !ok {
			return 0, 0, fmt.Errorf("epoch-migrate transactions: ran out of bodies at block %d", blockNum)
		}
		var b types.BodyForStorage
		if err := rlp.DecodeBytes(w, &b); err != nil {
			return 0, 0, fmt.Errorf("epoch-migrate transactions: decode body %d: %w", blockNum, err)
		}
		return uint64(b.TxCount), b.BaseTxnID.U64(), nil
	}

	// advance bodies without decoding (we don't need TxCount here)
	advanceBody := func(blockNum uint64) error {
		if _, ok, err := bodies.next(); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("epoch-migrate transactions: ran out of bodies at block %d", blockNum)
		}
		return nil
	}
	skipTx := func(n uint64) error {
		for range n {
			if _, ok, err := txs.next(); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("epoch-migrate transactions: ran out of tx words during skip")
			}
		}
		return nil
	}

	// Line both streams up to startBlock. Bodies is repacked last, so its decimal is still complete here
	// and runs from block 0; transactions may start higher, its lower segments already converted and
	// deleted. Below txFirstFrom a block has no tx words, so we just move bodies along and don't bother
	// decoding it.
	for b := bodyFirstFrom; b < startBlock; b++ {
		if b < txFirstFrom {
			if err := advanceBody(b); err != nil {
				return err
			}
			continue
		}
		txCount, _, err := nextBlock(b)
		if err != nil {
			return err
		}
		if err := skipTx(txCount); err != nil {
			return err
		}
	}

	tt := snaptype2.Transactions
	blockNum := startBlock
	writeSeg := func(a, b uint64) error {
		compressCfg := BlockCompressCfg
		compressCfg.Workers = m.workers
		path := tt.FileInfo(m.dirs.Snap, true, a, b).Path
		c, err := seg.NewCompressor(ctx, "epoch-migrate transactions", path, m.dirs.Tmp, compressCfg, m.lvl, m.logger)
		if err != nil {
			return err
		}
		defer c.Close()
		noCompress := segmentNoCompress(true, b-a)
		for ; blockNum < b; blockNum++ {
			txCount, _, err := nextBlock(blockNum)
			if err != nil {
				return err
			}
			for range txCount {
				w, ok, err := txs.next()
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("epoch-migrate transactions: ran out of tx words at block %d", blockNum)
				}
				if noCompress {
					if err := c.AddUncompressedWord(w); err != nil {
						return err
					}
				} else if err := c.AddWord(w); err != nil {
					return err
				}
			}
		}
		if err := c.Compress(); err != nil {
			return err
		}
		return afterStep()
	}

	delIdx := 0
	for _, r := range plan {
		if err := writeSeg(r[0], r[1]); err != nil {
			return err
		}
		start := delIdx
		for delIdx < len(txSegs) && txSegs[delIdx].To <= r[1] {
			delIdx++
		}
		if err := removeDecimalSegFiles(txSegs[start:delIdx]); err != nil {
			return err
		}
	}
	for ; blockNum < frozenMax; blockNum++ {
		txCount, baseTxnID, err := nextBlock(blockNum)
		if err != nil {
			return err
		}
		words := make([][]byte, 0, txCount)
		for range txCount {
			w, ok, err := txs.next()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("epoch-migrate transactions: ran out of tail tx words at block %d", blockNum)
			}
			words = append(words, append([]byte(nil), w...))
		}
		if err := onTail(blockNum, baseTxnID, words); err != nil {
			return err
		}
	}
	m.decimalSegs[snaptype2.Transactions.Enum()] = m.decimalSegs[snaptype2.Transactions.Enum()][delIdx:]
	return nil
}

// ErrUnfinishedLegacyDownload names a datadir that cannot go either way: converting it would build on
// a segment set with holes, and leaving it alone does not help either, because this build asks the
// manifest for epoch files only, so the decimal download it was in the middle of cannot resume.
var ErrUnfinishedLegacyDownload = errors.New("this datadir holds an unfinished pre-epoch snapshot download: " +
	"finish it with the previous release, or remove the snapshots directory and sync again")

// CheckLegacyDownloadFinished guards every entry point that would convert a datadir. It reports
// ErrUnfinishedLegacyDownload when decimal segments are present from a download that never finished,
// so the operator is told what the datadir actually is rather than that some type is missing.
func CheckLegacyDownloadFinished(db kv.RoDB, dirs datadir.Dirs, chainConfig *chain.Config) error {
	complete, err := rawdb.AllSegmentsDownloadCompleteFromDB(db)
	if err != nil {
		return err
	}
	if complete {
		return nil
	}
	hasDecimal, err := HasDecimalBlockSegments(dirs, chainConfig)
	if err != nil {
		return err
	}
	if hasDecimal {
		return ErrUnfinishedLegacyDownload
	}
	return nil
}

// HasDecimalBlockSegments reports whether the datadir still holds legacy decimal block segments that
// MigrateDecimalToEpoch would convert. Read-only consumers use it to refuse such a datadir rather than
// convert one they do not own. Always false on a decimal chain, where decimal is the regime, not
// pending work.
func HasDecimalBlockSegments(dirs datadir.Dirs, chainConfig *chain.Config) (bool, error) {
	if !snaptype2.RegimeFor(chainConfig) {
		return false, nil
	}
	all, err := snaptype.Segments(dirs.Snap)
	if err != nil {
		return false, err
	}
	for _, t := range snaptype2.BlockSnapshotTypes {
		if _, decimal := classifyByType(all, t.Enum()); len(decimal) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// EpochMigrationPending reports whether MigrateDecimalToEpoch still has work here: decimal segments
// to convert, or a conversion that deleted the last of them and stopped before every index was built.
// Entry points use it to decide whether to migrate, or, read-only, to refuse the datadir.
func EpochMigrationPending(ctx context.Context, db kv.RoDB, dirs datadir.Dirs, chainConfig *chain.Config) (bool, error) {
	if pending, err := HasDecimalBlockSegments(dirs, chainConfig); err != nil || pending {
		return pending, err
	}
	return migrationInProgress(ctx, db)
}

// MigrateDecimalToEpoch turns any decimal block segments in dirs.Snap into epoch ones and writes the
// sub-1024 tail back to the DB. Run it at startup, before the block snapshots are opened. It's a no-op
// when there's no decimal, and safe to re-run after a crash (it resumes from the epoch segments already
// on disk).
func MigrateDecimalToEpoch(ctx context.Context, dirs datadir.Dirs, db kv.RwDB, chainConfig *chain.Config, workers int, logger log.Logger) error {
	if !snaptype2.RegimeFor(chainConfig) {
		return nil // decimal chain (Bor/Gnosis): nothing to convert
	}
	snCfg := snapcfg.KnownCfgOrDevnet(chainConfig.ChainName)
	m := &epochMigrator{
		dirs:        dirs,
		snCfg:       snCfg,
		db:          db,
		chainConfig: chainConfig,
		workers:     workers,
		lvl:         log.LvlInfo,
		logger:      logger,
	}
	return m.run(ctx)
}

// run keeps no marker file: every step is idempotent and a crash resumes from the epoch segments on
// disk. Types go in dependency order — headers, transactions, bodies — because the transactions repack
// reads the decimal bodies, so those go last. Each repack deletes its own decimal segments as their
// blocks become safe in epoch segments; the segment straddling tailFrom is kept until the DB tail is
// committed, then removed.
func (m *epochMigrator) run(ctx context.Context) error {
	start, frozenMax, hasDecimal, err := m.loadSegs()
	if err != nil {
		return err
	}
	inProgress, err := m.inProgress(ctx)
	if err != nil {
		return err
	}
	if !hasDecimal {
		if !inProgress {
			return nil
		}
		// Every block is in an epoch segment or the DB tail; what may be missing is their indexes.
		if err := m.buildEpochIndexes(ctx); err != nil {
			return err
		}
		return m.setInProgress(ctx, false)
	}
	if frozenMax == 0 {
		// Every block type must be present and reachable: the visible-file calculation drops every
		// type when one of them has none, so converting such a datadir would only move the breakage.
		return fmt.Errorf("epoch-migration: no usable segments for %s; refusing to convert a datadir that is already unopenable",
			strings.Join(m.uncovered, ", "))
	}

	frontier := maps.Clone(start)
	var tailFrom uint64
	m.plan, tailFrom = planEpochConversion(start, m.epochFrom, frozenMax, m.snCfg)
	for _, t := range snaptype2.BlockSnapshotTypes {
		// The same visible-file rule: a type left with no segment hides every other type's.
		if start[t.Enum()] >= tailFrom && !m.hasEpoch[t.Enum()] {
			return fmt.Errorf("epoch-migration: %s segments start at block %d, too close to the frozen frontier %d to fill one epoch segment; "+
				"converting would leave the datadir unopenable", t.Name(), frontier[t.Enum()], frozenMax)
		}
	}
	if err := m.warnDropped(ctx, frontier, start, frozenMax); err != nil {
		return err
	}
	if err := m.setInProgress(ctx, true); err != nil {
		return err
	}
	if err := afterStep(); err != nil {
		return err
	}
	m.logger.Info("[epoch-migration] converting decimal block segments to epoch", "upto", frozenMax)

	tailHeaders := make(map[uint64][]byte)
	tailBodies := make(map[uint64][]byte)
	tailTxs := make(map[uint64][][]byte)

	if err := m.repackWordPerBlock(ctx, snaptype2.Headers, start[snaptype2.Headers.Enum()], frozenMax, func(bn uint64, w []byte) error {
		tailHeaders[bn] = append([]byte(nil), w...)
		return nil
	}); err != nil {
		return err
	}
	if err := m.repackTransactions(ctx, start[snaptype2.Transactions.Enum()], frozenMax, func(bn, _ uint64, ws [][]byte) error {
		tailTxs[bn] = ws
		return nil
	}); err != nil {
		return err
	}
	if err := m.repackWordPerBlock(ctx, snaptype2.Bodies, start[snaptype2.Bodies.Enum()], frozenMax, func(bn uint64, w []byte) error {
		tailBodies[bn] = append([]byte(nil), w...)
		return nil
	}); err != nil {
		return err
	}

	if err := m.db.Update(ctx, func(tx kv.RwTx) error {
		for bn := tailFrom; bn < frozenMax; bn++ {
			if err := writeTailBlock(tx, tailHeaders[bn], tailBodies[bn], tailTxs[bn]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	tailHeaders, tailBodies, tailTxs = nil, nil, nil
	if err := afterStep(); err != nil {
		return err
	}

	if err := m.buildEpochIndexes(ctx); err != nil {
		return err
	}

	// tail is in the DB now, so the straddle segments (and any leftovers) can go.
	if err := m.deleteDecimalSegs(frozenMax); err != nil {
		return err
	}
	if err := m.setInProgress(ctx, false); err != nil {
		return err
	}
	m.logger.Info("[epoch-migration] done", "upto", frozenMax, "tailToDB", [2]uint64{tailFrom, frozenMax})
	return nil
}

func (m *epochMigrator) inProgress(ctx context.Context) (bool, error) {
	if m.db == nil {
		return false, nil
	}
	return migrationInProgress(ctx, m.db)
}

func migrationInProgress(ctx context.Context, db kv.RoDB) (started bool, err error) {
	err = db.View(ctx, func(tx kv.Tx) error {
		v, getErr := tx.GetOne(kv.DatabaseInfo, epochMigrationKey)
		started = len(v) > 0
		return getErr
	})
	return started, err
}

func (m *epochMigrator) setInProgress(ctx context.Context, started bool) error {
	if m.db == nil {
		return nil
	}
	return m.db.Update(ctx, func(tx kv.RwTx) error {
		if started {
			return tx.Put(kv.DatabaseInfo, epochMigrationKey, []byte{1})
		}
		return tx.Delete(kv.DatabaseInfo, epochMigrationKey)
	})
}

// warnDropped logs the blocks a pruned type gives up to start on the epoch grid, and how many of them
// the node's block retention still covers. Those are fewer than one smallest tier, the oldest the node
// keeps, and would be pruned once the chain advances by as many blocks. The cut that start adds to
// the tiling is not one the published files have, so the segments around it never merge or match them.
func (m *epochMigrator) warnDropped(ctx context.Context, frontier, start map[snaptype.Enum]uint64, frozenMax uint64) error {
	var mode prune.Mode
	if m.db != nil {
		if err := m.db.View(ctx, func(tx kv.Tx) (err error) {
			mode, err = prune.Get(tx)
			return err
		}); err != nil {
			return err
		}
	}
	for _, t := range snaptype2.BlockSnapshotTypes {
		from, to := frontier[t.Enum()], start[t.Enum()]
		if from == to {
			continue
		}
		var retained uint64
		if mode.Blocks != nil && mode.Blocks.Enabled() {
			if cutoff := mode.Blocks.PruneTo(frozenMax); to > cutoff {
				retained = to - max(cutoff, from)
			}
		}
		m.logger.Warn("[epoch-migration] dropping blocks below the epoch grid; segments around this cut will not merge or match published files",
			"type", t.Name(), "from", from, "to", to, "stillRetained", retained)
	}
	return nil
}

// loadSegs does the single startup dir scan: it caches each type's decimal segments and returns, per
// type, the start block (epoch prefix end), the overall frozenMax (min coverage across types) and
// whether there's any decimal segment at all.
func (m *epochMigrator) loadSegs() (start map[snaptype.Enum]uint64, frozenMax uint64, hasDecimal bool, err error) {
	all, err := snaptype.Segments(m.dirs.Snap)
	if err != nil {
		return nil, 0, false, err
	}
	m.decimalSegs = make(map[snaptype.Enum][]snaptype.FileInfo)
	m.contiguousLen = make(map[snaptype.Enum]int)
	m.allDecimal = make(map[snaptype.Enum][]snaptype.FileInfo)
	m.hasEpoch = make(map[snaptype.Enum]bool)
	m.epochFrom = nil
	start = make(map[snaptype.Enum]uint64)
	frozenMax = ^uint64(0)
	for _, t := range snaptype2.BlockSnapshotTypes {
		epoch, decimal := classifyByType(all, t.Enum())
		if len(decimal) > 0 {
			hasDecimal = true
		}
		decimal = dropConverted(epoch, decimal)
		typeStart, coveredTo, contiguousLen := coverage(epoch, decimal)
		m.decimalSegs[t.Enum()] = decimal
		m.hasEpoch[t.Enum()] = len(epoch) > 0
		if len(epoch) > 0 {
			m.epochFrom = append(m.epochFrom, epoch[0].From)
		}
		for i := range all {
			if f := &all[i]; f.Type != nil && f.Type.Enum() == t.Enum() && !f.Epoch {
				m.allDecimal[t.Enum()] = append(m.allDecimal[t.Enum()], *f)
			}
		}
		m.contiguousLen[t.Enum()] = contiguousLen
		start[t.Enum()] = typeStart
		frozenMax = min(frozenMax, coveredTo)
		if coveredTo == 0 {
			m.uncovered = append(m.uncovered, t.Name())
		}
	}
	return start, frozenMax, hasDecimal, nil
}

func hasEpochIndexes(info snaptype.FileInfo, names []string) bool {
	for _, n := range info.Type.IdxFileNames(info.Epoch, info.From, info.To) {
		if !slices.Contains(names, n) {
			return false
		}
	}
	return true
}

// buildEpochIndexes builds the indexes the epoch segments on disk lack. It runs after every segment is
// written, as the transactions index needs the epoch bodies of the same range, and skips the ones
// already built so a re-run does not rebuild everything.
func (m *epochMigrator) buildEpochIndexes(ctx context.Context) error {
	des, err := os.ReadDir(m.dirs.Snap)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(des))
	for _, de := range des {
		names = append(names, de.Name())
	}
	all, err := snaptype.Segments(m.dirs.Snap)
	if err != nil {
		return err
	}
	for _, t := range snaptype2.BlockSnapshotTypes {
		epoch, _ := classifyByType(all, t.Enum())
		for i := range epoch {
			info := epoch[i]
			if hasEpochIndexes(info, names) {
				continue
			}
			p := &background.Progress{}
			if err := t.BuildIndexes(ctx, info, nil, m.chainConfig, m.dirs.Tmp, p, m.lvl, m.logger); err != nil {
				return err
			}
			if err := afterStep(); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteDecimalSegs removes the decimal segments still cached after the repacks — the one straddling
// tailFrom per type (kept until the DB tail was committed) and anything after a gap. The repacks
// already deleted everything below the frontier, so there's no dir re-scan here.
//
// Segments above frozenMax are dropped, not converted: they're past the contiguous frontier (a gap, or
// a type that reaches further than the others), so nothing can read them anyway. That still throws away
// blocks the node has to fetch again, so each one is logged.
func (m *epochMigrator) deleteDecimalSegs(frozenMax uint64) error {
	for _, t := range snaptype2.BlockSnapshotTypes {
		segs := m.decimalSegs[t.Enum()]
		for i := range segs {
			f := &segs[i]
			if f.From >= frozenMax {
				m.logger.Warn("[epoch-migration] dropping decimal segment above the migrated frontier; its blocks were not converted and will need re-downloading",
					"file", f.Name(), "from", f.From, "to", f.To, "frozenMax", frozenMax)
			}
		}
		if err := removeDecimalSegFiles(m.allDecimal[t.Enum()]); err != nil {
			return err
		}
		m.decimalSegs[t.Enum()] = nil
	}
	return nil
}
