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
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
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
		return segs[i].To < segs[j].To
	})
	return snapshotsync.NoOverlaps(segs)
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

// coverage walks type t as one run from block 0 and returns:
//   - epochStart: end of the epoch segments we already made; a re-run picks up here.
//   - coveredTo: how far the run goes once the decimal segments continue past the epoch part.
//     frozenMax is the smallest coveredTo of the three types (a block is done only when all three
//     have it — same min RoSnapshots uses).
//   - runLen: how many decimal segments from the front have no gaps — the ones a repack reads.
//     After earlier deletions the first one may not start at block 0.
//
// The decimal segment sitting across the epoch frontier is caught by From <= coveredTo < To; a gap
// stops both the run and coveredTo. Inputs must be sorted and overlap-free.
func coverage(epoch, decimal []snaptype.FileInfo) (epochStart, coveredTo uint64, runLen int) {
	for i := range epoch { // contiguous epoch prefix from block 0
		f := &epoch[i]
		if f.From != epochStart {
			break
		}
		epochStart = f.To
	}
	coveredTo = epochStart
	end := uint64(0)
	if len(decimal) > 0 {
		end = decimal[0].From
	}
	for i := range decimal {
		f := &decimal[i]
		if f.From != end {
			break // gap: the contiguous decimal run ends here
		}
		runLen++
		end = f.To
		if f.From <= coveredTo && coveredTo < f.To {
			coveredTo = f.To
		}
	}
	return epochStart, coveredTo, runLen
}

// removeDecimalSegFiles deletes each decimal segment and everything with its range and type: the .seg,
// its .torrent and its indexes (an index may have a different version, so we match range+type with the
// version left wild). The trailing wildcard that picks up those extensions also picks up the epoch file
// printing the same digits, so each match is decoded and only the decimal ones are deleted.
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
			if err := dir.RemoveFile(m); err != nil {
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
}

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
	plan, _ := planEpochSegments(startBlock, frozenMax, m.snCfg)
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
		return c.Compress()
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
	plan, _ := planEpochSegments(startBlock, frozenMax, m.snCfg)

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
		return c.Compress()
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
	if !hasDecimal {
		return nil
	}
	if frozenMax == 0 {
		return fmt.Errorf("epoch-migration: decimal segments present but none reach a contiguous frontier from block 0; refusing to delete anything")
	}
	m.logger.Info("[epoch-migration] converting decimal block segments to epoch", "upto", frozenMax)

	tailFrom := frozenMax - frozenMax%snaptype.EpochMinSegmentSize
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

	if err := m.buildEpochIndexes(ctx, frozenMax); err != nil {
		return err
	}

	// tail is in the DB now, so the straddle segments (and any leftovers) can go.
	if err := m.deleteDecimalSegs(frozenMax); err != nil {
		return err
	}
	m.logger.Info("[epoch-migration] done", "upto", frozenMax, "tailToDB", [2]uint64{tailFrom, frozenMax})
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
	start = make(map[snaptype.Enum]uint64)
	frozenMax = ^uint64(0)
	for _, t := range snaptype2.BlockSnapshotTypes {
		epoch, decimal := classifyByType(all, t.Enum())
		epochStart, coveredTo, contiguousLen := coverage(epoch, decimal)
		m.decimalSegs[t.Enum()] = decimal
		m.contiguousLen[t.Enum()] = contiguousLen
		start[t.Enum()] = epochStart
		frozenMax = min(frozenMax, coveredTo)
		if len(decimal) > 0 {
			hasDecimal = true
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

// buildEpochIndexes builds the epoch indexes that are missing. It runs after every segment is on disk
// (the transactions index needs the epoch bodies), and skips any segment whose index already exists so
// a re-run doesn't rebuild everything.
func (m *epochMigrator) buildEpochIndexes(ctx context.Context, frozenMax uint64) error {
	plan, _ := planEpochSegments(0, frozenMax, m.snCfg)
	des, err := os.ReadDir(m.dirs.Snap)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(des))
	for _, de := range des {
		names = append(names, de.Name())
	}
	for _, t := range snaptype2.BlockSnapshotTypes {
		for _, r := range plan {
			info := t.FileInfo(m.dirs.Snap, true, r[0], r[1])
			if hasEpochIndexes(info, names) {
				continue
			}
			p := &background.Progress{}
			if err := t.BuildIndexes(ctx, info, nil, m.chainConfig, m.dirs.Tmp, p, m.lvl, m.logger); err != nil {
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
		if err := removeDecimalSegFiles(m.decimalSegs[t.Enum()]); err != nil {
			return err
		}
		m.decimalSegs[t.Enum()] = nil
	}
	return nil
}
