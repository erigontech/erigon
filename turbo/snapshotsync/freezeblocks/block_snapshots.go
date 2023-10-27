package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/crypto/cryptopool"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type HeaderSegment struct {
	seg           *compress.Decompressor // value: first_byte_of_header_hash + header_rlp
	idxHeaderHash *recsplit.Index        // header_hash       -> headers_segment_offset
	ranges        Range
}

type BodySegment struct {
	seg           *compress.Decompressor // value: rlp(types.BodyForStorage)
	idxBodyNumber *recsplit.Index        // block_num_u64     -> bodies_segment_offset
	ranges        Range
}

type TxnSegment struct {
	Seg                 *compress.Decompressor // value: first_byte_of_transaction_hash + sender_address + transaction_rlp
	IdxTxnHash          *recsplit.Index        // transaction_hash  -> transactions_segment_offset
	IdxTxnHash2BlockNum *recsplit.Index        // transaction_hash  -> block_number
	ranges              Range
}

func (sn *HeaderSegment) closeIdx() {
	if sn.idxHeaderHash != nil {
		sn.idxHeaderHash.Close()
		sn.idxHeaderHash = nil
	}
}
func (sn *HeaderSegment) closeSeg() {
	if sn.seg != nil {
		sn.seg.Close()
		sn.seg = nil
	}
}
func (sn *HeaderSegment) close() {
	sn.closeSeg()
	sn.closeIdx()
}
func (sn *HeaderSegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.ranges.from, sn.ranges.to, snaptype.Headers)
	sn.seg, err = compress.NewDecompressor(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}
func (sn *HeaderSegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if sn.idxHeaderHash != nil {
		return nil
	}
	err = sn.reopenIdx(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			if optimistic {
				log.Warn("[snapshots] open index", "err", err)
			} else {
				return err
			}
		}
	}
	return nil
}
func (sn *HeaderSegment) reopenIdx(dir string) (err error) {
	sn.closeIdx()
	if sn.seg == nil {
		return nil
	}
	fileName := snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.Headers.String())
	sn.idxHeaderHash, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}

	return nil
}

func (sn *BodySegment) closeSeg() {
	if sn.seg != nil {
		sn.seg.Close()
		sn.seg = nil
	}
}
func (sn *BodySegment) closeIdx() {
	if sn.idxBodyNumber != nil {
		sn.idxBodyNumber.Close()
		sn.idxBodyNumber = nil
	}
}
func (sn *BodySegment) close() {
	sn.closeSeg()
	sn.closeIdx()
}

func (sn *BodySegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.ranges.from, sn.ranges.to, snaptype.Bodies)
	sn.seg, err = compress.NewDecompressor(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}
func (sn *BodySegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if sn.idxBodyNumber != nil {
		return nil
	}
	err = sn.reopenIdx(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			if optimistic {
				log.Warn("[snapshots] open index", "err", err)
			} else {
				return err
			}
		}
	}
	return nil
}

func (sn *BodySegment) reopenIdx(dir string) (err error) {
	sn.closeIdx()
	if sn.seg == nil {
		return nil
	}
	fileName := snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.Bodies.String())
	sn.idxBodyNumber, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}

func (sn *TxnSegment) closeIdx() {
	if sn.IdxTxnHash != nil {
		sn.IdxTxnHash.Close()
		sn.IdxTxnHash = nil
	}
	if sn.IdxTxnHash2BlockNum != nil {
		sn.IdxTxnHash2BlockNum.Close()
		sn.IdxTxnHash2BlockNum = nil
	}
}
func (sn *TxnSegment) closeSeg() {
	if sn.Seg != nil {
		sn.Seg.Close()
		sn.Seg = nil
	}
}
func (sn *TxnSegment) close() {
	sn.closeSeg()
	sn.closeIdx()
}
func (sn *TxnSegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.ranges.from, sn.ranges.to, snaptype.Transactions)
	sn.Seg, err = compress.NewDecompressor(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}
func (sn *TxnSegment) reopenIdx(dir string) (err error) {
	sn.closeIdx()
	if sn.Seg == nil {
		return nil
	}
	fileName := snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.Transactions.String())
	sn.IdxTxnHash, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}

	/*
		// Historically we had several times when:
		//  - erigon downloaded new version of .seg file
		//  - or didn't finish download and start indexing
		// this was a "quick-fix protection" against this cases
		// but now we have other protections for this cases
		// let's try to remove this one - because it's not compatible with "copy datadir" and "restore datadir from backup" scenarios
		if sn.IdxTxnHash.ModTime().Before(sn.Seg.ModTime()) {
			log.Trace("[snapshots] skip index because it modify time is ahead before .seg file", "name", sn.IdxTxnHash.FileName())
			//Index has been created before the segment file, needs to be ignored (and rebuilt) as inconsistent
			sn.IdxTxnHash.Close()
			sn.IdxTxnHash = nil
		}
	*/

	fileName = snaptype.IdxFileName(sn.ranges.from, sn.ranges.to, snaptype.Transactions2Block.String())
	sn.IdxTxnHash2BlockNum, err = recsplit.OpenIndex(path.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("%w, fileName: %s", err, fileName)
	}
	return nil
}

func (sn *TxnSegment) reopenIdxIfNeed(dir string, optimistic bool) (err error) {
	if sn.IdxTxnHash != nil && sn.IdxTxnHash2BlockNum != nil {
		return nil
	}
	err = sn.reopenIdx(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			if optimistic {
				log.Warn("[snapshots] open index", "err", err)
			} else {
				return err
			}
		}
	}
	return nil
}

type headerSegments struct {
	lock     sync.RWMutex
	segments []*HeaderSegment
}

func (s *headerSegments) View(f func(segments []*HeaderSegment) error) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return f(s.segments)
}

type bodySegments struct {
	lock     sync.RWMutex
	segments []*BodySegment
}

func (s *bodySegments) View(f func([]*BodySegment) error) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return f(s.segments)
}
func (s *bodySegments) ViewSegment(blockNum uint64, f func(*BodySegment) error) (found bool, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for _, seg := range s.segments {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return true, f(seg)
	}
	return false, nil
}

type txnSegments struct {
	lock     sync.RWMutex
	segments []*TxnSegment
}

func (s *txnSegments) View(f func([]*TxnSegment) error) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return f(s.segments)
}
func (s *txnSegments) ViewSegment(blockNum uint64, f func(*TxnSegment) error) (found bool, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for _, seg := range s.segments {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return true, f(seg)
	}
	return false, nil
}

type RoSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Headers *headerSegments
	Bodies  *bodySegments
	Txs     *txnSegments

	dir         string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
}

// NewRoSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, logger log.Logger) *RoSnapshots {
	return &RoSnapshots{dir: snapDir, cfg: cfg, Headers: &headerSegments{}, Bodies: &bodySegments{}, Txs: &txnSegments{}, logger: logger}
}

func (s *RoSnapshots) Cfg() ethconfig.BlocksFreezing { return s.cfg }
func (s *RoSnapshots) Dir() string                   { return s.dir }
func (s *RoSnapshots) SegmentsReady() bool           { return s.segmentsReady.Load() }
func (s *RoSnapshots) IndicesReady() bool            { return s.indicesReady.Load() }
func (s *RoSnapshots) IndicesMax() uint64            { return s.idxMax.Load() }
func (s *RoSnapshots) SegmentsMax() uint64           { return s.segmentsMax.Load() }
func (s *RoSnapshots) BlocksAvailable() uint64       { return cmp.Min(s.segmentsMax.Load(), s.idxMax.Load()) }
func (s *RoSnapshots) LogStat() {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	s.logger.Info("[snapshots] Blocks Stat",
		"blocks", fmt.Sprintf("%dk", (s.BlocksAvailable()+1)/1000),
		"indices", fmt.Sprintf("%dk", (s.IndicesMax()+1)/1000),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
}

func (s *RoSnapshots) ScanDir() (map[string]struct{}, []*services.Range, error) {
	existingFiles, missingSnapshots, err := Segments(s.dir)
	if err != nil {
		return nil, nil, err
	}
	existingFilesMap := map[string]struct{}{}
	for _, existingFile := range existingFiles {
		_, fname := filepath.Split(existingFile.Path)
		existingFilesMap[fname] = struct{}{}
	}

	res := make([]*services.Range, 0, len(missingSnapshots))
	for _, sn := range missingSnapshots {
		res = append(res, &services.Range{From: sn.from, To: sn.to})
	}
	return existingFilesMap, res, nil
}
func (s *RoSnapshots) EnsureExpectedBlocksAreAvailable(cfg *snapcfg.Cfg) error {
	if s.BlocksAvailable() < cfg.ExpectBlocks {
		return fmt.Errorf("app must wait until all expected snapshots are available. Expected: %d, Available: %d", cfg.ExpectBlocks, s.BlocksAvailable())
	}
	return nil
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (s *RoSnapshots) DisableReadAhead() {
	s.Headers.lock.RLock()
	defer s.Headers.lock.RUnlock()
	s.Bodies.lock.RLock()
	defer s.Bodies.lock.RUnlock()
	s.Txs.lock.RLock()
	defer s.Txs.lock.RUnlock()
	for _, sn := range s.Headers.segments {
		sn.seg.DisableReadAhead()
	}
	for _, sn := range s.Bodies.segments {
		sn.seg.DisableReadAhead()
	}
	for _, sn := range s.Txs.segments {
		sn.Seg.DisableReadAhead()
	}
}
func (s *RoSnapshots) EnableReadAhead() *RoSnapshots {
	s.Headers.lock.RLock()
	defer s.Headers.lock.RUnlock()
	s.Bodies.lock.RLock()
	defer s.Bodies.lock.RUnlock()
	s.Txs.lock.RLock()
	defer s.Txs.lock.RUnlock()
	for _, sn := range s.Headers.segments {
		sn.seg.EnableReadAhead()
	}
	for _, sn := range s.Bodies.segments {
		sn.seg.EnableReadAhead()
	}
	for _, sn := range s.Txs.segments {
		sn.Seg.EnableReadAhead()
	}
	return s
}
func (s *RoSnapshots) EnableMadvWillNeed() *RoSnapshots {
	s.Headers.lock.RLock()
	defer s.Headers.lock.RUnlock()
	s.Bodies.lock.RLock()
	defer s.Bodies.lock.RUnlock()
	s.Txs.lock.RLock()
	defer s.Txs.lock.RUnlock()
	for _, sn := range s.Headers.segments {
		sn.seg.EnableWillNeed()
	}
	for _, sn := range s.Bodies.segments {
		sn.seg.EnableWillNeed()
	}
	for _, sn := range s.Txs.segments {
		sn.Seg.EnableWillNeed()
	}
	return s
}
func (s *RoSnapshots) EnableMadvNormal() *RoSnapshots {
	s.Headers.lock.RLock()
	defer s.Headers.lock.RUnlock()
	s.Bodies.lock.RLock()
	defer s.Bodies.lock.RUnlock()
	s.Txs.lock.RLock()
	defer s.Txs.lock.RUnlock()
	for _, sn := range s.Headers.segments {
		sn.seg.EnableMadvNormal()
	}
	for _, sn := range s.Bodies.segments {
		sn.seg.EnableMadvNormal()
	}
	for _, sn := range s.Txs.segments {
		sn.Seg.EnableMadvNormal()
	}
	return s
}

func (s *RoSnapshots) idxAvailability() uint64 {
	var headers, bodies, txs uint64
	for _, seg := range s.Headers.segments {
		if seg.idxHeaderHash == nil {
			break
		}
		headers = seg.ranges.to - 1
	}
	for _, seg := range s.Bodies.segments {
		if seg.idxBodyNumber == nil {
			break
		}
		bodies = seg.ranges.to - 1
	}
	for _, seg := range s.Txs.segments {
		if seg.IdxTxnHash == nil || seg.IdxTxnHash2BlockNum == nil {
			break
		}
		txs = seg.ranges.to - 1
	}
	return cmp.Min(headers, cmp.Min(bodies, txs))
}

// OptimisticReopenWithDB - optimistically open snapshots (ignoring error), useful at App startup because:
// - user must be able: delete any snapshot file and Erigon will self-heal by re-downloading
// - RPC return Nil for historical blocks if snapshots are not open
func (s *RoSnapshots) OptimisticReopenWithDB(db kv.RoDB) {
	_ = db.View(context.Background(), func(tx kv.Tx) error {
		snList, _, err := rawdb.ReadSnapshots(tx)
		if err != nil {
			return err
		}
		return s.ReopenList(snList, true)
	})
}

func (s *RoSnapshots) Files() (list []string) {
	s.Headers.lock.RLock()
	defer s.Headers.lock.RUnlock()
	s.Bodies.lock.RLock()
	defer s.Bodies.lock.RUnlock()
	s.Txs.lock.RLock()
	defer s.Txs.lock.RUnlock()
	max := s.BlocksAvailable()
	for _, seg := range s.Bodies.segments {
		if seg.seg == nil {
			continue
		}
		if seg.ranges.from > max {
			continue
		}
		_, fName := filepath.Split(seg.seg.FilePath())
		list = append(list, fName)
	}
	for _, seg := range s.Headers.segments {
		if seg.seg == nil {
			continue
		}
		if seg.ranges.from > max {
			continue
		}
		_, fName := filepath.Split(seg.seg.FilePath())
		list = append(list, fName)
	}
	for _, seg := range s.Txs.segments {
		if seg.Seg == nil {
			continue
		}
		if seg.ranges.from > max {
			continue
		}
		_, fName := filepath.Split(seg.Seg.FilePath())
		list = append(list, fName)
	}
	slices.Sort(list)
	return list
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *RoSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	s.Headers.lock.Lock()
	defer s.Headers.lock.Unlock()
	s.Bodies.lock.Lock()
	defer s.Bodies.lock.Unlock()
	s.Txs.lock.Lock()
	defer s.Txs.lock.Unlock()

	s.closeWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok {
			continue
		}
		var processed bool = true

		switch f.T {
		case snaptype.Headers:
			var sn *HeaderSegment
			var exists bool
			for _, sn2 := range s.Headers.segments {
				if sn2.seg == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.seg.FileName() {
					sn = sn2
					exists = true
					break
				}
			}
			if !exists {
				sn = &HeaderSegment{ranges: Range{f.From, f.To}}
			}
			if err := sn.reopenSeg(s.dir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if optimistic {
						continue Loop
					} else {
						break Loop
					}
				}
				if optimistic {
					s.logger.Warn("[snapshots] open segment", "err", err)
					continue Loop
				} else {
					return err
				}
			}

			if !exists {
				// it's possible to iterate over .seg file even if you don't have index
				// then make segment available even if index open may fail
				s.Headers.segments = append(s.Headers.segments, sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
		case snaptype.Bodies:
			var sn *BodySegment
			var exists bool
			for _, sn2 := range s.Bodies.segments {
				if sn2.seg == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.seg.FileName() {
					sn = sn2
					exists = true
					break
				}
			}
			if !exists {
				sn = &BodySegment{ranges: Range{f.From, f.To}}
			}
			if err := sn.reopenSeg(s.dir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if optimistic {
						continue Loop
					} else {
						break Loop
					}
				}
				if optimistic {
					s.logger.Warn("[snapshots] open segment", "err", err)
					continue Loop
				} else {
					return err
				}
			}
			if !exists {
				s.Bodies.segments = append(s.Bodies.segments, sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
		case snaptype.Transactions:
			var sn *TxnSegment
			var exists bool
			for _, sn2 := range s.Txs.segments {
				if sn2.Seg == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.Seg.FileName() {
					sn = sn2
					exists = true
					break
				}
			}
			if !exists {
				sn = &TxnSegment{ranges: Range{f.From, f.To}}
			}
			if err := sn.reopenSeg(s.dir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if optimistic {
						continue Loop
					} else {
						break Loop
					}
				}
				if optimistic {
					s.logger.Warn("[snapshots] open segment", "err", err)
					continue Loop
				} else {
					return err
				}
			}
			if !exists {
				s.Txs.segments = append(s.Txs.segments, sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
		default:
			processed = false
		}

		if processed {
			if f.To > 0 {
				segmentsMax = f.To - 1
			} else {
				segmentsMax = 0
			}
			segmentsMaxSet = true
		}
	}
	if segmentsMaxSet {
		s.segmentsMax.Store(segmentsMax)
	}
	s.segmentsReady.Store(true)
	s.idxMax.Store(s.idxAvailability())
	s.indicesReady.Store(true)

	return nil
}

func (s *RoSnapshots) Ranges() (ranges []Range) {
	view := s.View()
	defer view.Close()

	for _, sn := range view.Headers() {
		ranges = append(ranges, sn.ranges)
	}
	return ranges
}

func (s *RoSnapshots) OptimisticalyReopenFolder()           { _ = s.ReopenFolder() }
func (s *RoSnapshots) OptimisticalyReopenWithDB(db kv.RoDB) { _ = s.ReopenWithDB(db) }
func (s *RoSnapshots) ReopenFolder() error {
	files, _, err := Segments(s.dir)
	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.ReopenList(list, false)
}
func (s *RoSnapshots) ReopenWithDB(db kv.RoDB) error {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		snList, _, err := rawdb.ReadSnapshots(tx)
		if err != nil {
			return err
		}
		return s.ReopenList(snList, true)
	}); err != nil {
		return err
	}
	return nil
}

func (s *RoSnapshots) Close() {
	s.Headers.lock.Lock()
	defer s.Headers.lock.Unlock()
	s.Bodies.lock.Lock()
	defer s.Bodies.lock.Unlock()
	s.Txs.lock.Lock()
	defer s.Txs.lock.Unlock()
	s.closeWhatNotInList(nil)
}

func (s *RoSnapshots) closeWhatNotInList(l []string) {
Loop1:
	for i, sn := range s.Headers.segments {
		if sn.seg == nil {
			continue Loop1
		}
		_, name := filepath.Split(sn.seg.FilePath())
		for _, fName := range l {
			if fName == name {
				continue Loop1
			}
		}
		sn.close()
		s.Headers.segments[i] = nil
	}
Loop2:
	for i, sn := range s.Bodies.segments {
		if sn.seg == nil {
			continue Loop2
		}
		_, name := filepath.Split(sn.seg.FilePath())
		for _, fName := range l {
			if fName == name {
				continue Loop2
			}
		}
		sn.close()
		s.Bodies.segments[i] = nil
	}
Loop3:
	for i, sn := range s.Txs.segments {
		if sn.Seg == nil {
			continue Loop3
		}
		_, name := filepath.Split(sn.Seg.FilePath())
		for _, fName := range l {
			if fName == name {
				continue Loop3
			}
		}
		sn.close()
		s.Txs.segments[i] = nil
	}
	var i int
	for i = 0; i < len(s.Headers.segments) && s.Headers.segments[i] != nil && s.Headers.segments[i].seg != nil; i++ {
	}
	tail := s.Headers.segments[i:]
	s.Headers.segments = s.Headers.segments[:i]
	for i = 0; i < len(tail); i++ {
		if tail[i] != nil {
			tail[i].close()
			tail[i] = nil
		}
	}

	for i = 0; i < len(s.Bodies.segments) && s.Bodies.segments[i] != nil && s.Bodies.segments[i].seg != nil; i++ {
	}
	tailB := s.Bodies.segments[i:]
	s.Bodies.segments = s.Bodies.segments[:i]
	for i = 0; i < len(tailB); i++ {
		if tailB[i] != nil {
			tailB[i].close()
			tailB[i] = nil
		}
	}

	for i = 0; i < len(s.Txs.segments) && s.Txs.segments[i] != nil && s.Txs.segments[i].Seg != nil; i++ {
	}
	tailC := s.Txs.segments[i:]
	s.Txs.segments = s.Txs.segments[:i]
	for i = 0; i < len(tailC); i++ {
		if tailC[i] != nil {
			tailC[i].close()
			tailC[i] = nil
		}
	}
}

func (s *RoSnapshots) PrintDebug() {
	s.Headers.lock.RLock()
	defer s.Headers.lock.RUnlock()
	s.Bodies.lock.RLock()
	defer s.Bodies.lock.RUnlock()
	s.Txs.lock.RLock()
	defer s.Txs.lock.RUnlock()
	fmt.Println("    == Snapshots, Header")
	for _, sn := range s.Headers.segments {
		fmt.Printf("%d,  %t\n", sn.ranges.from, sn.idxHeaderHash == nil)
	}
	fmt.Println("    == Snapshots, Body")
	for _, sn := range s.Bodies.segments {
		fmt.Printf("%d,  %t\n", sn.ranges.from, sn.idxBodyNumber == nil)
	}
	fmt.Println("    == Snapshots, Txs")
	for _, sn := range s.Txs.segments {
		fmt.Printf("%d,  %t, %t\n", sn.ranges.from, sn.IdxTxnHash == nil, sn.IdxTxnHash2BlockNum == nil)
	}
}

func (s *RoSnapshots) AddSnapshotsToSilkworm(silkwormInstance *silkworm.Silkworm) error {
	mappedHeaderSnapshots := make([]*silkworm.MappedHeaderSnapshot, 0)
	err := s.Headers.View(func(segments []*HeaderSegment) error {
		for _, headerSegment := range segments {
			mappedHeaderSnapshots = append(mappedHeaderSnapshots, headerSegment.mappedSnapshot())
		}
		return nil
	})
	if err != nil {
		return err
	}

	mappedBodySnapshots := make([]*silkworm.MappedBodySnapshot, 0)
	err = s.Bodies.View(func(segments []*BodySegment) error {
		for _, bodySegment := range segments {
			mappedBodySnapshots = append(mappedBodySnapshots, bodySegment.mappedSnapshot())
		}
		return nil
	})
	if err != nil {
		return err
	}

	mappedTxnSnapshots := make([]*silkworm.MappedTxnSnapshot, 0)
	err = s.Txs.View(func(segments []*TxnSegment) error {
		for _, txnSegment := range segments {
			mappedTxnSnapshots = append(mappedTxnSnapshots, txnSegment.mappedSnapshot())
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(mappedHeaderSnapshots) != len(mappedBodySnapshots) || len(mappedBodySnapshots) != len(mappedTxnSnapshots) {
		return fmt.Errorf("addSnapshots: the number of headers/bodies/txs snapshots must be the same")
	}

	for i := 0; i < len(mappedHeaderSnapshots); i++ {
		mappedSnapshot := &silkworm.MappedChainSnapshot{
			Headers: mappedHeaderSnapshots[i],
			Bodies:  mappedBodySnapshots[i],
			Txs:     mappedTxnSnapshots[i],
		}
		err := silkwormInstance.AddSnapshot(mappedSnapshot)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildIdx(ctx context.Context, sn snaptype.FileInfo, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	//_, fName := filepath.Split(sn.Path)
	//log.Info("[snapshots] build idx", "file", fName)
	switch sn.T {
	case snaptype.Headers:
		if err := HeadersIdx(ctx, chainConfig, sn.Path, sn.From, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.Bodies:
		if err := BodiesIdx(ctx, sn.Path, sn.From, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.Transactions:
		dir, _ := filepath.Split(sn.Path)
		if err := TransactionsIdx(ctx, chainConfig, sn.From, sn.To, dir, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.BorEvents:
		dir, _ := filepath.Split(sn.Path)
		if err := BorEventsIdx(ctx, sn.Path, sn.From, sn.To, dir, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.BorSpans:
		dir, _ := filepath.Split(sn.Path)
		if err := BorSpansIdx(ctx, sn.Path, sn.From, sn.To, dir, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}
	//log.Info("[snapshots] finish build idx", "file", fName)
	return nil
}

func BuildMissedIndices(logPrefix string, ctx context.Context, dirs datadir.Dirs, chainConfig *chain.Config, workers int, logger log.Logger) error {
	dir, tmpDir := dirs.Snap, dirs.Tmp
	//log.Log(lvl, "[snapshots] Build indices", "from", min)

	segments, _, err := Segments(dir)
	if err != nil {
		return err
	}
	ps := background.NewProgressSet()
	startIndexingTime := time.Now()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	finish := make(chan struct{})

	go func() {
		for {
			select {
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				logger.Info(fmt.Sprintf("[%s] Indexing", logPrefix), "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			case <-finish:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for _, t := range snaptype.AllSnapshotTypes {
		for index := range segments {
			segment := segments[index]
			if segment.T != t {
				continue
			}
			if hasIdxFile(segment, logger) {
				continue
			}
			sn := segment
			g.Go(func() error {
				p := &background.Progress{}
				ps.Add(p)
				defer ps.Delete(p)
				return buildIdx(gCtx, sn, chainConfig, tmpDir, p, log.LvlInfo, logger)
			})
		}
	}
	go func() {
		defer close(finish)
		g.Wait()
	}()

	// Block main thread
	select {
	case <-finish:
		return g.Wait()
	case <-ctx.Done():
		return ctx.Err()
	}

}

func BuildBorMissedIndices(logPrefix string, ctx context.Context, dirs datadir.Dirs, chainConfig *chain.Config, workers int, logger log.Logger) error {
	dir, tmpDir := dirs.Snap, dirs.Tmp

	segments, _, err := BorSegments(dir)
	if err != nil {
		return err
	}
	ps := background.NewProgressSet()
	startIndexingTime := time.Now()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, t := range []snaptype.Type{snaptype.BorEvents, snaptype.BorSpans} {
		for _, segment := range segments {
			if segment.T != t {
				continue
			}
			if hasIdxFile(segment, logger) {
				continue
			}
			sn := segment
			g.Go(func() error {
				p := &background.Progress{}
				ps.Add(p)
				defer ps.Delete(p)
				return buildIdx(gCtx, sn, chainConfig, tmpDir, p, log.LvlInfo, logger)
			})
		}
	}
	finish := make(chan struct{})
	go func() {
		defer close(finish)
		g.Wait()
	}()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	for {
		select {
		case <-finish:
			return g.Wait()
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			logger.Info(fmt.Sprintf("[%s] Indexing", logPrefix), "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
		}
	}
}

func noGaps(in []snaptype.FileInfo) (out []snaptype.FileInfo, missingSnapshots []Range) {
	var prevTo uint64
	for _, f := range in {
		if f.To <= prevTo {
			continue
		}
		if f.From != prevTo { // no gaps
			missingSnapshots = append(missingSnapshots, Range{prevTo, f.From})
			continue
		}
		prevTo = f.To
		out = append(out, f)
	}
	return out, missingSnapshots
}

func allTypeOfSegmentsMustExist(dir string, in []snaptype.FileInfo) (res []snaptype.FileInfo) {
MainLoop:
	for _, f := range in {
		if f.From == f.To {
			continue
		}
		for _, t := range snaptype.AllSnapshotTypes {
			p := filepath.Join(dir, snaptype.SegmentFileName(f.From, f.To, t))
			if !dir2.FileExist(p) {
				continue MainLoop
			}
		}
		res = append(res, f)
	}
	return res
}

func borSegmentsMustExist(dir string, in []snaptype.FileInfo) (res []snaptype.FileInfo) {
MainLoop:
	for _, f := range in {
		if f.From == f.To {
			continue
		}
		for _, t := range []snaptype.Type{snaptype.BorEvents, snaptype.BorSpans} {
			p := filepath.Join(dir, snaptype.SegmentFileName(f.From, f.To, t))
			if !dir2.FileExist(p) {
				continue MainLoop
			}
		}
		res = append(res, f)
	}
	return res
}

// noOverlaps - keep largest ranges and avoid overlap
func noOverlaps(in []snaptype.FileInfo) (res []snaptype.FileInfo) {
	for i := range in {
		f := in[i]
		if f.From == f.To {
			continue
		}

		for j := i + 1; j < len(in); j++ { // if there is file with larger range - use it instead
			f2 := in[j]
			if f.From == f.To {
				continue
			}
			if f2.From > f.From {
				break
			}
			f = f2
			i++
		}

		res = append(res, f)
	}
	return res
}

func SegmentsCaplin(dir string) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	list, err := snaptype.Segments(dir)
	if err != nil {
		return nil, missingSnapshots, err
	}

	{
		var l []snaptype.FileInfo
		var m []Range
		for _, f := range list {
			if f.T != snaptype.BeaconBlocks {
				continue
			}
			l = append(l, f)
		}
		l, m = noGaps(noOverlaps(l))
		res = append(res, l...)
		missingSnapshots = append(missingSnapshots, m...)
	}
	return res, missingSnapshots, nil
}

func Segments(dir string) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	list, err := snaptype.Segments(dir)
	if err != nil {
		return nil, missingSnapshots, err
	}
	{
		var l []snaptype.FileInfo
		var m []Range
		for _, f := range list {
			if f.T != snaptype.Headers {
				continue
			}
			l = append(l, f)
		}
		l, m = noGaps(noOverlaps(allTypeOfSegmentsMustExist(dir, l)))
		res = append(res, l...)
		missingSnapshots = append(missingSnapshots, m...)
	}
	{
		var l []snaptype.FileInfo
		for _, f := range list {
			if f.T != snaptype.Bodies {
				continue
			}
			l = append(l, f)
		}
		l, _ = noGaps(noOverlaps(allTypeOfSegmentsMustExist(dir, l)))
		res = append(res, l...)
	}
	{
		var l []snaptype.FileInfo
		for _, f := range list {
			if f.T != snaptype.Transactions {
				continue
			}
			l = append(l, f)
		}
		l, _ = noGaps(noOverlaps(allTypeOfSegmentsMustExist(dir, l)))
		res = append(res, l...)
	}

	return res, missingSnapshots, nil
}

func chooseSegmentEnd(from, to, blocksPerFile uint64) uint64 {
	next := (from/blocksPerFile + 1) * blocksPerFile
	to = cmp.Min(next, to)

	if to < snaptype.Erigon2MinSegmentSize {
		return to
	}

	return to - (to % snaptype.Erigon2MinSegmentSize) // round down to the nearest 1k
}

type BlockRetire struct {
	working               atomic.Bool
	needSaveFilesListInDB atomic.Bool

	workers int
	tmpDir  string
	db      kv.RoDB

	notifier    services.DBEventNotifier
	logger      log.Logger
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
	dirs        datadir.Dirs
}

func NewBlockRetire(workers int, dirs datadir.Dirs, blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter, db kv.RoDB, notifier services.DBEventNotifier, logger log.Logger) *BlockRetire {
	return &BlockRetire{workers: workers, tmpDir: dirs.Tmp, dirs: dirs, blockReader: blockReader, blockWriter: blockWriter, db: db, notifier: notifier, logger: logger}
}

func (br *BlockRetire) snapshots() *RoSnapshots { return br.blockReader.Snapshots().(*RoSnapshots) }

func (br *BlockRetire) borSnapshots() *BorRoSnapshots {
	return br.blockReader.BorSnapshots().(*BorRoSnapshots)
}

func (br *BlockRetire) HasNewFrozenFiles() bool {
	return br.needSaveFilesListInDB.CompareAndSwap(true, false)
}

func CanRetire(curBlockNum uint64, blocksInSnapshots uint64) (blockFrom, blockTo uint64, can bool) {
	if curBlockNum <= params.FullImmutabilityThreshold {
		return
	}
	blockFrom = blocksInSnapshots + 1
	return canRetire(blockFrom, curBlockNum-params.FullImmutabilityThreshold)
}

func canRetire(from, to uint64) (blockFrom, blockTo uint64, can bool) {
	if to <= from {
		return
	}
	blockFrom = (from / 1_000) * 1_000
	roundedTo1K := (to / 1_000) * 1_000
	var maxJump uint64 = 1_000
	if blockFrom%500_000 == 0 {
		maxJump = 500_000
	} else if blockFrom%100_000 == 0 {
		maxJump = 100_000
	} else if blockFrom%10_000 == 0 {
		maxJump = 10_000
	}
	//roundedTo1K := (to / 1_000) * 1_000
	jump := cmp.Min(maxJump, roundedTo1K-blockFrom)
	switch { // only next segment sizes are allowed
	case jump >= 500_000:
		blockTo = blockFrom + 500_000
	case jump >= 100_000:
		blockTo = blockFrom + 100_000
	case jump >= 10_000:
		blockTo = blockFrom + 10_000
	case jump >= 1_000:
		blockTo = blockFrom + 1_000
	default:
		blockTo = blockFrom
	}
	return blockFrom, blockTo, blockTo-blockFrom >= 1_000
}

func CanDeleteTo(curBlockNum uint64, blocksInSnapshots uint64) (blockTo uint64) {
	if curBlockNum+999 < params.FullImmutabilityThreshold {
		// To prevent overflow of uint64 below
		return blocksInSnapshots + 1
	}
	hardLimit := (curBlockNum/1_000)*1_000 - params.FullImmutabilityThreshold
	return cmp.Min(hardLimit, blocksInSnapshots+1)
}

func (br *BlockRetire) RetireBlocks(ctx context.Context, blockFrom, blockTo uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error) error {
	chainConfig := fromdb.ChainConfig(br.db)
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	logger.Log(lvl, "[snapshots] Retire Blocks", "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))
	snapshots := br.snapshots()
	firstTxNum := blockReader.(*BlockReader).FirstTxNumNotInSnapshots()

	// in future we will do it in background
	if err := DumpBlocks(ctx, blockFrom, blockTo, snaptype.Erigon2SegmentSize, tmpDir, snapshots.Dir(), firstTxNum, db, workers, lvl, logger, blockReader); err != nil {
		return fmt.Errorf("DumpBlocks: %w", err)
	}
	if err := snapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen: %w", err)
	}
	snapshots.LogStat()
	if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
		notifier.OnNewSnapshot()
	}
	merger := NewMerger(tmpDir, workers, lvl, db, chainConfig, notifier, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges())
	if len(rangesToMerge) == 0 {
		return nil
	}
	err := merger.Merge(ctx, snapshots, rangesToMerge, snapshots.Dir(), true /* doIndex */)
	if err != nil {
		return err
	}
	if err := snapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen: %w", err)
	}
	snapshots.LogStat()
	if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
		notifier.OnNewSnapshot()
	}

	downloadRequest := make([]services.DownloadRequest, 0, len(rangesToMerge))
	for i := range rangesToMerge {
		r := &services.Range{From: rangesToMerge[i].from, To: rangesToMerge[i].to}
		downloadRequest = append(downloadRequest, services.NewDownloadRequest(r, "", "", false /* Bor */))
	}

	if seedNewSnapshots != nil {
		if err := seedNewSnapshots(downloadRequest); err != nil {
			return err
		}
	}
	return nil
}

func (br *BlockRetire) PruneAncientBlocks(tx kv.RwTx, limit int, includeBor bool) error {
	if br.blockReader.FreezingCfg().KeepBlocks {
		return nil
	}
	currentProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return err
	}
	canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBlocks())
	if err := br.blockWriter.PruneBlocks(context.Background(), tx, canDeleteTo, limit); err != nil {
		return nil
	}
	if includeBor {
		canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBorBlocks())
		if err := br.blockWriter.PruneBorBlocks(context.Background(), tx, canDeleteTo, limit); err != nil {
			return nil
		}
	}
	return nil
}

func (br *BlockRetire) RetireBlocksInBackground(ctx context.Context, forwardProgress uint64, includeBor bool, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error) {
	ok := br.working.CompareAndSwap(false, true)
	if !ok {
		// go-routine is still working
		return
	}
	go func() {
		defer br.working.Store(false)

		blockFrom, blockTo, ok := CanRetire(forwardProgress, br.blockReader.FrozenBlocks())
		if ok {
			if err := br.RetireBlocks(ctx, blockFrom, blockTo, lvl, seedNewSnapshots); err != nil {
				br.logger.Warn("[snapshots] retire blocks", "err", err, "fromBlock", blockFrom, "toBlock", blockTo)
			}
		}

		if includeBor {
			blockFrom, blockTo, ok = CanRetire(forwardProgress, br.blockReader.FrozenBorBlocks())
			if ok {
				if err := br.RetireBorBlocks(ctx, blockFrom, blockTo, lvl, seedNewSnapshots); err != nil {
					br.logger.Warn("[bor snapshots] retire blocks", "err", err, "fromBlock", blockFrom, "toBlock", blockTo)
				}
			}
		}
	}()
}

func (br *BlockRetire) BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	snapshots := br.snapshots()
	snapshots.LogStat()

	// Create .idx files
	if snapshots.IndicesMax() < snapshots.SegmentsMax() {

		if !snapshots.Cfg().Produce && snapshots.IndicesMax() == 0 {
			return fmt.Errorf("please remove --snap.stop, erigon can't work without creating basic indices")
		}
		if snapshots.Cfg().Produce {
			if !snapshots.SegmentsReady() {
				return fmt.Errorf("not all snapshot segments are available")
			}

			// wait for Downloader service to download all expected snapshots
			if snapshots.IndicesMax() < snapshots.SegmentsMax() {
				indexWorkers := estimate.IndexSnapshot.Workers()
				if err := BuildMissedIndices(logPrefix, ctx, br.dirs, cc, indexWorkers, br.logger); err != nil {
					return fmt.Errorf("BuildMissedIndices: %w", err)
				}
			}

			if err := snapshots.ReopenFolder(); err != nil {
				return err
			}
			snapshots.LogStat()
			if notifier != nil {
				notifier.OnNewSnapshot()
			}
		}
	}
	if cc.Bor != nil {
		borSnapshots := br.borSnapshots()
		borSnapshots.LogStat()

		// Create .idx files
		if borSnapshots.IndicesMax() < borSnapshots.SegmentsMax() {

			if !borSnapshots.Cfg().Produce && borSnapshots.IndicesMax() == 0 {
				return fmt.Errorf("please remove --snap.stop, erigon can't work without creating basic indices")
			}
			if borSnapshots.Cfg().Produce {
				if !borSnapshots.SegmentsReady() {
					return fmt.Errorf("not all bor snapshot segments are available")
				}

				// wait for Downloader service to download all expected snapshots
				if borSnapshots.IndicesMax() < borSnapshots.SegmentsMax() {
					indexWorkers := estimate.IndexSnapshot.Workers()
					if err := BuildBorMissedIndices(logPrefix, ctx, br.dirs, cc, indexWorkers, br.logger); err != nil {
						return fmt.Errorf("BuildBorMissedIndices: %w", err)
					}
				}

				if err := borSnapshots.ReopenFolder(); err != nil {
					return err
				}
				borSnapshots.LogStat()
				if notifier != nil {
					notifier.OnNewSnapshot()
				}
			}
		}
	}
	return nil
}

func DumpBlocks(ctx context.Context, blockFrom, blockTo, blocksPerFile uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	if blocksPerFile == 0 {
		return nil
	}
	chainConfig := fromdb.ChainConfig(chainDB)

	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, blocksPerFile) {
		if err := dumpBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, blocksPerFile), tmpDir, snapDir, firstTxNum, chainDB, *chainConfig, workers, lvl, logger, blockReader); err != nil {
			return err
		}
	}
	return nil
}

func dumpBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, chainConfig chain.Config, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	{
		segName := snaptype.SegmentFileName(blockFrom, blockTo, snaptype.Headers)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot Headers", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return err
		}
		defer sn.Close()
		if err := DumpHeaders(ctx, chainDB, blockFrom, blockTo, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		}); err != nil {
			return fmt.Errorf("DumpHeaders: %w", err)
		}
		if err := sn.Compress(); err != nil {
			return fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}

	{
		segName := snaptype.SegmentFileName(blockFrom, blockTo, snaptype.Bodies)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot Bodies", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return err
		}
		defer sn.Close()
		if err := DumpBodies(ctx, chainDB, blockFrom, blockTo, firstTxNum, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		}); err != nil {
			return fmt.Errorf("DumpBodies: %w", err)
		}
		if err := sn.Compress(); err != nil {
			return fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}

	{
		segName := snaptype.SegmentFileName(blockFrom, blockTo, snaptype.Transactions)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot Txs", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return fmt.Errorf("NewCompressor: %w, %s", err, f.Path)
		}
		defer sn.Close()

		expectedCount, err := DumpTxs(ctx, chainDB, blockFrom, blockTo, &chainConfig, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		})
		if err != nil {
			return fmt.Errorf("DumpTxs: %w", err)
		}
		if expectedCount != sn.Count() {
			return fmt.Errorf("incorrect tx count: %d, expected from db: %d", sn.Count(), expectedCount)
		}
		snapDir, fileName := filepath.Split(f.Path)
		ext := filepath.Ext(fileName)
		logger.Log(lvl, "[snapshots] Compression", "ratio", sn.Ratio.String(), "file", fileName[:len(fileName)-len(ext)])

		_, expectedCount, err = txsAmountBasedOnBodiesSnapshots(snapDir, blockFrom, blockTo)
		if err != nil {
			return err
		}
		if expectedCount != sn.Count() {
			return fmt.Errorf("incorrect tx count: %d, expected from snapshots: %d", sn.Count(), expectedCount)
		}
		if err := sn.Compress(); err != nil {
			return fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}

	return nil
}

func hasIdxFile(sn snaptype.FileInfo, logger log.Logger) bool {
	dir, _ := filepath.Split(sn.Path)
	fName := snaptype.IdxFileName(sn.From, sn.To, sn.T.String())
	var result = true
	switch sn.T {
	case snaptype.Headers, snaptype.Bodies, snaptype.BorEvents, snaptype.BorSpans:
		idx, err := recsplit.OpenIndex(path.Join(dir, fName))
		if err != nil {
			return false
		}
		idx.Close()
	case snaptype.Transactions:
		idx, err := recsplit.OpenIndex(path.Join(dir, fName))
		if err != nil {
			return false
		}
		idx.Close()

		fName = snaptype.IdxFileName(sn.From, sn.To, snaptype.Transactions2Block.String())
		idx, err = recsplit.OpenIndex(path.Join(dir, fName))
		if err != nil {
			return false
		}
		idx.Close()
	}
	return result
}

// DumpTxs - [from, to)
// Format: hash[0]_1byte + sender_address_2bytes + txnRlp
func DumpTxs(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger, collect func([]byte) error) (expectedCount int, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	warmupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	numBuf := make([]byte, 8)
	parseCtx := types2.NewTxParseContext(*chainID)
	parseCtx.WithSender(false)
	slot := types2.TxSlot{}
	var sender [20]byte
	parse := func(v, valueBuf []byte, senders []common2.Address, j int) ([]byte, error) {
		if _, err := parseCtx.ParseTransaction(v, 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
			return valueBuf, err
		}
		if len(senders) > 0 {
			sender = senders[j]
		}

		valueBuf = valueBuf[:0]
		valueBuf = append(valueBuf, slot.IDHash[:1]...)
		valueBuf = append(valueBuf, sender[:]...)
		valueBuf = append(valueBuf, v...)
		return valueBuf, nil
	}
	valueBuf := make([]byte, 16*4096)
	addSystemTx := func(tx kv.Tx, txId uint64) error {
		binary.BigEndian.PutUint64(numBuf, txId)
		tv, err := tx.GetOne(kv.EthTx, numBuf)
		if err != nil {
			return err
		}
		if tv == nil {
			if err := collect(nil); err != nil {
				return err
			}
			return nil
		}

		parseCtx.WithSender(false)
		valueBuf, err = parse(tv, valueBuf, nil, 0)
		if err != nil {
			return err
		}
		if err := collect(valueBuf); err != nil {
			return err
		}
		return nil
	}

	doWarmup, warmupTxs, warmupSenders := blockTo-blockFrom >= 100_000 && workers > 4, &atomic.Bool{}, &atomic.Bool{}
	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo { // [from, to)
			return false, nil
		}

		h := common2.BytesToHash(v)
		dataRLP := rawdb.ReadStorageBodyRLP(tx, h, blockNum)
		if dataRLP == nil {
			return false, fmt.Errorf("body not found: %d, %x", blockNum, h)
		}
		var body types.BodyForStorage
		if e := rlp.DecodeBytes(dataRLP, &body); e != nil {
			return false, e
		}
		if body.TxAmount == 0 {
			return true, nil
		}
		expectedCount += int(body.TxAmount)

		if doWarmup && !warmupSenders.Load() && blockNum%1_000 == 0 {
			clean := kv.ReadAhead(warmupCtx, db, warmupSenders, kv.Senders, hexutility.EncodeTs(blockNum), 10_000)
			defer clean()
		}
		if doWarmup && !warmupTxs.Load() && blockNum%1_000 == 0 {
			clean := kv.ReadAhead(warmupCtx, db, warmupTxs, kv.EthTx, hexutility.EncodeTs(body.BaseTxId), 100*10_000)
			defer clean()
		}
		senders, err := rawdb.ReadSenders(tx, h, blockNum)
		if err != nil {
			return false, err
		}

		j := 0

		if err := addSystemTx(tx, body.BaseTxId); err != nil {
			return false, err
		}
		binary.BigEndian.PutUint64(numBuf, body.BaseTxId+1)
		if err := tx.ForAmount(kv.EthTx, numBuf, body.TxAmount-2, func(_, tv []byte) error {
			parseCtx.WithSender(len(senders) == 0)
			valueBuf, err = parse(tv, valueBuf, senders, j)
			if err != nil {
				return fmt.Errorf("%w, block: %d", err, blockNum)
			}
			// first tx byte => sender adress => tx rlp
			if err := collect(valueBuf); err != nil {
				return err
			}
			j++

			return nil
		}); err != nil {
			return false, fmt.Errorf("ForAmount: %w", err)
		}

		if err := addSystemTx(tx, body.BaseTxId+uint64(body.TxAmount)-1); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Dumping txs", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return 0, fmt.Errorf("BigChunks: %w", err)
	}
	return expectedCount, nil
}

// DumpHeaders - [from, to)
func DumpHeaders(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, workers int, lvl log.Lvl, logger log.Logger, collect func([]byte) error) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	key := make([]byte, 8+32)
	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)
		dataRLP, err := tx.GetOne(kv.Headers, key)
		if err != nil {
			return false, err
		}
		if dataRLP == nil {
			return false, fmt.Errorf("header missed in db: block_num=%d,  hash=%x", blockNum, v)
		}
		h := types.Header{}
		if err := rlp.DecodeBytes(dataRLP, &h); err != nil {
			return false, err
		}

		value := make([]byte, len(dataRLP)+1) // first_byte_of_header_hash + header_rlp
		value[0] = h.Hash()[0]
		copy(value[1:], dataRLP)
		if err := collect(value); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Dumping headers", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

// DumpBodies - [from, to)
func DumpBodies(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, firstTxNum uint64, workers int, lvl log.Lvl, logger log.Logger, collect func([]byte) error) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	blockNumByteLength := 8
	blockHashByteLength := 32
	key := make([]byte, blockNumByteLength+blockHashByteLength)
	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)

		// Important: DB does store canonical and non-canonical txs in same table. And using same body.BaseTxID
		// But snapshots using canonical TxNum in field body.BaseTxID
		// So, we manually calc this field here and serialize again.
		//
		// FYI: we also have other table to map canonical BlockNum->TxNum: kv.MaxTxNum
		body, err := rawdb.ReadBodyForStorageByKey(tx, key)
		if err != nil {
			return false, err
		}
		if body == nil {
			logger.Warn("body missed", "block_num", blockNum, "hash", hex.EncodeToString(v))
			return true, nil
		}

		body.BaseTxId = firstTxNum
		firstTxNum += uint64(body.TxAmount)

		dataRLP, err := rlp.EncodeToBytes(body)
		if err != nil {
			return false, err
		}

		if err := collect(dataRLP); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Wrote into file", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}

var EmptyTxHash = common2.Hash{}

func txsAmountBasedOnBodiesSnapshots(snapDir string, blockFrom, blockTo uint64) (firstTxID uint64, expectedCount int, err error) {
	bodySegmentPath := filepath.Join(snapDir, snaptype.SegmentFileName(blockFrom, blockTo, snaptype.Bodies))
	bodiesSegment, err := compress.NewDecompressor(bodySegmentPath)
	if err != nil {
		return
	}
	defer bodiesSegment.Close()

	gg := bodiesSegment.MakeGetter()
	buf, _ := gg.Next(nil)
	firstBody := &types.BodyForStorage{}
	if err = rlp.DecodeBytes(buf, firstBody); err != nil {
		return
	}
	firstTxID = firstBody.BaseTxId

	lastBody := new(types.BodyForStorage)
	i := uint64(0)
	for gg.HasNext() {
		i++
		if i == blockTo-blockFrom-1 {
			buf, _ = gg.Next(buf[:0])
			if err = rlp.DecodeBytes(buf, lastBody); err != nil {
				return
			}
			if gg.HasNext() {
				panic(1)
			}
		} else {
			gg.Skip()
		}
	}

	expectedCount = int(lastBody.BaseTxId+uint64(lastBody.TxAmount)) - int(firstBody.BaseTxId)
	return
}

func TransactionsIdx(ctx context.Context, chainConfig *chain.Config, blockFrom, blockTo uint64, snapDir string, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("TransactionsIdx: at=%d-%d, %v, %s", blockFrom, blockTo, rec, dbg.Stack())
		}
	}()
	firstBlockNum := blockFrom
	firstTxID, expectedCount, err := txsAmountBasedOnBodiesSnapshots(snapDir, blockFrom, blockTo)
	if err != nil {
		return err
	}
	bodySegmentPath := filepath.Join(snapDir, snaptype.SegmentFileName(blockFrom, blockTo, snaptype.Bodies))
	bodiesSegment, err := compress.NewDecompressor(bodySegmentPath)
	if err != nil {
		return
	}
	defer bodiesSegment.Close()

	segFileName := snaptype.SegmentFileName(blockFrom, blockTo, snaptype.Transactions)
	segmentFilePath := filepath.Join(snapDir, segFileName)
	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()
	if d.Count() != expectedCount {
		return fmt.Errorf("TransactionsIdx: at=%d-%d, pre index building, expect: %d, got %d", blockFrom, blockTo, expectedCount, d.Count())
	}
	p.Name.Store(&segFileName)
	p.Total.Store(uint64(d.Count() * 2))

	txnHashIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:    d.Count(),
		Enums:       true,
		BucketSize:  2000,
		LeafSize:    8,
		TmpDir:      tmpDir,
		IndexFile:   filepath.Join(snapDir, snaptype.IdxFileName(blockFrom, blockTo, snaptype.Transactions.String())),
		BaseDataID:  firstTxID,
		EtlBufLimit: etl.BufferOptimalSize / 2,
	}, logger)
	if err != nil {
		return err
	}

	txnHash2BlockNumIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:    d.Count(),
		Enums:       false,
		BucketSize:  2000,
		LeafSize:    8,
		TmpDir:      tmpDir,
		IndexFile:   filepath.Join(snapDir, snaptype.IdxFileName(blockFrom, blockTo, snaptype.Transactions2Block.String())),
		BaseDataID:  firstBlockNum,
		EtlBufLimit: etl.BufferOptimalSize / 2,
	}, logger)
	if err != nil {
		return err
	}
	txnHashIdx.LogLvl(log.LvlDebug)
	txnHash2BlockNumIdx.LogLvl(log.LvlDebug)

	chainId, _ := uint256.FromBig(chainConfig.ChainID)

	parseCtx := types2.NewTxParseContext(*chainId)
	parseCtx.WithSender(false)
	slot := types2.TxSlot{}
	bodyBuf, word := make([]byte, 0, 4096), make([]byte, 0, 4096)

	defer d.EnableMadvNormal().DisableReadAhead()
	defer bodiesSegment.EnableMadvNormal().DisableReadAhead()

RETRY:
	g, bodyGetter := d.MakeGetter(), bodiesSegment.MakeGetter()
	var i, offset, nextPos uint64
	blockNum := firstBlockNum
	body := &types.BodyForStorage{}

	bodyBuf, _ = bodyGetter.Next(bodyBuf[:0])
	if err := rlp.DecodeBytes(bodyBuf, body); err != nil {
		return err
	}

	for g.HasNext() {
		p.Processed.Add(1)
		word, nextPos = g.Next(word[:0])
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for body.BaseTxId+uint64(body.TxAmount) <= firstTxID+i { // skip empty blocks
			if !bodyGetter.HasNext() {
				return fmt.Errorf("not enough bodies")
			}

			bodyBuf, _ = bodyGetter.Next(bodyBuf[:0])
			if err := rlp.DecodeBytes(bodyBuf, body); err != nil {
				return err
			}

			blockNum++
		}

		firstTxByteAndlengthOfAddress := 21
		isSystemTx := len(word) == 0
		if isSystemTx { // system-txs hash:pad32(txnID)
			binary.BigEndian.PutUint64(slot.IDHash[:], firstTxID+i)
		} else {
			if _, err = parseCtx.ParseTransaction(word[firstTxByteAndlengthOfAddress:], 0, &slot, nil, true /* hasEnvelope */, false /* wrappedWithBlobs */, nil /* validateHash */); err != nil {
				return fmt.Errorf("ParseTransaction: %w, blockNum: %d, i: %d", err, blockNum, i)
			}
		}

		if err := txnHashIdx.AddKey(slot.IDHash[:], offset); err != nil {
			return err
		}
		if err := txnHash2BlockNumIdx.AddKey(slot.IDHash[:], blockNum); err != nil {
			return err
		}

		i++
		offset = nextPos
	}

	if int(i) != expectedCount {
		return fmt.Errorf("TransactionsIdx: at=%d-%d, post index building, expect: %d, got %d", blockFrom, blockTo, expectedCount, i)
	}

	if err := txnHashIdx.Build(ctx); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			logger.Warn("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
			txnHashIdx.ResetNextSalt()
			txnHash2BlockNumIdx.ResetNextSalt()
			goto RETRY
		}
		return fmt.Errorf("txnHashIdx: %w", err)
	}
	if err := txnHash2BlockNumIdx.Build(ctx); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			logger.Warn("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
			txnHashIdx.ResetNextSalt()
			txnHash2BlockNumIdx.ResetNextSalt()
			goto RETRY
		}
		return fmt.Errorf("txnHash2BlockNumIdx: %w", err)
	}

	return nil
}

// HeadersIdx - headerHash -> offset (analog of kv.HeaderNumber)
func HeadersIdx(ctx context.Context, chainConfig *chain.Config, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			_, fName := filepath.Split(segmentFilePath)
			err = fmt.Errorf("HeadersIdx: at=%s, %v, %s", fName, rec, dbg.Stack())
		}
	}()

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	_, fname := filepath.Split(segmentFilePath)
	p.Name.Store(&fname)
	p.Total.Store(uint64(d.Count()))

	hasher := crypto.NewKeccakState()
	defer cryptopool.ReturnToPoolKeccak256(hasher)
	var h common2.Hash
	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, log.LvlDebug, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		p.Processed.Add(1)
		headerRlp := word[1:]
		hasher.Reset()
		hasher.Write(headerRlp)
		hasher.Read(h[:])
		if err := idx.AddKey(h[:], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("HeadersIdx: %w", err)
	}
	return nil
}

func BodiesIdx(ctx context.Context, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			_, fName := filepath.Split(segmentFilePath)
			err = fmt.Errorf("BodiesIdx: at=%s, %v, %s", fName, rec, dbg.Stack())
		}
	}()

	num := make([]byte, 8)

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	_, fname := filepath.Split(segmentFilePath)
	p.Name.Store(&fname)
	p.Total.Store(uint64(d.Count()))

	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, log.LvlDebug, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		p.Processed.Add(1)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("BodyNumberIdx: %w", err)
	}
	return nil
}

// Idx - iterate over segment and building .idx file
func Idx(ctx context.Context, d *compress.Decompressor, firstDataID uint64, tmpDir string, lvl log.Lvl, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error, logger log.Logger) error {
	segmentFileName := d.FilePath()
	var extension = filepath.Ext(segmentFileName)
	var idxFilePath = segmentFileName[0:len(segmentFileName)-len(extension)] + ".idx"

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:    d.Count(),
		Enums:       true,
		BucketSize:  2000,
		LeafSize:    8,
		TmpDir:      tmpDir,
		IndexFile:   idxFilePath,
		BaseDataID:  firstDataID,
		EtlBufLimit: etl.BufferOptimalSize / 2,
	}, logger)
	if err != nil {
		return err
	}
	rs.LogLvl(log.LvlDebug)

	defer d.EnableMadvNormal().DisableReadAhead()

RETRY:
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
			goto RETRY
		}
		return err
	}
	return nil
}

func ForEachHeader(ctx context.Context, s *RoSnapshots, walker func(header *types.Header) error) error {
	r := bytes.NewReader(nil)
	word := make([]byte, 0, 2*4096)

	view := s.View()
	defer view.Close()

	for _, sn := range view.Headers() {
		if err := sn.seg.WithReadAhead(func() error {
			g := sn.seg.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				var header types.Header
				r.Reset(word[1:])
				if err := rlp.Decode(r, &header); err != nil {
					return err
				}
				if err := walker(&header); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

type Merger struct {
	lvl             log.Lvl
	compressWorkers int
	tmpDir          string
	chainConfig     *chain.Config
	chainDB         kv.RoDB
	notifier        services.DBEventNotifier
	logger          log.Logger
}

func NewMerger(tmpDir string, compressWorkers int, lvl log.Lvl, chainDB kv.RoDB, chainConfig *chain.Config, notifier services.DBEventNotifier, logger log.Logger) *Merger {
	return &Merger{tmpDir: tmpDir, compressWorkers: compressWorkers, lvl: lvl, chainDB: chainDB, chainConfig: chainConfig, notifier: notifier, logger: logger}
}

type Range struct {
	from, to uint64
}

func (r Range) From() uint64 { return r.from }
func (r Range) To() uint64   { return r.to }

func (*Merger) FindMergeRanges(currentRanges []Range) (toMerge []Range) {
	for i := len(currentRanges) - 1; i > 0; i-- {
		r := currentRanges[i]
		if r.to-r.from >= snaptype.Erigon2SegmentSize { // is complete .seg
			continue
		}

		for _, span := range []uint64{500_000, 100_000, 10_000} {
			if r.to%span != 0 {
				continue
			}
			if r.to-r.from == span {
				break
			}
			aggFrom := r.to - span
			toMerge = append(toMerge, Range{from: aggFrom, to: r.to})
			for currentRanges[i].from > aggFrom {
				i--
			}
			break
		}
	}
	slices.SortFunc(toMerge, func(i, j Range) int { return cmp.Compare(i.from, j.from) })
	return toMerge
}

type View struct {
	s      *RoSnapshots
	closed bool
}

func (s *RoSnapshots) View() *View {
	v := &View{s: s}
	v.s.Headers.lock.RLock()
	v.s.Bodies.lock.RLock()
	v.s.Txs.lock.RLock()
	return v
}

func (v *View) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.s.Headers.lock.RUnlock()
	v.s.Bodies.lock.RUnlock()
	v.s.Txs.lock.RUnlock()
}
func (v *View) Headers() []*HeaderSegment { return v.s.Headers.segments }
func (v *View) Bodies() []*BodySegment    { return v.s.Bodies.segments }
func (v *View) Txs() []*TxnSegment        { return v.s.Txs.segments }
func (v *View) HeadersSegment(blockNum uint64) (*HeaderSegment, bool) {
	for _, seg := range v.Headers() {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}
func (v *View) BodiesSegment(blockNum uint64) (*BodySegment, bool) {
	for _, seg := range v.Bodies() {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}
func (v *View) TxsSegment(blockNum uint64) (*TxnSegment, bool) {
	for _, seg := range v.Txs() {
		if !(blockNum >= seg.ranges.from && blockNum < seg.ranges.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func (m *Merger) filesByRange(snapshots *RoSnapshots, from, to uint64) (map[snaptype.Type][]string, error) {
	toMerge := map[snaptype.Type][]string{}
	view := snapshots.View()
	defer view.Close()

	hSegments := view.Headers()
	bSegments := view.Bodies()
	tSegments := view.Txs()

	for i, sn := range hSegments {
		if sn.ranges.from < from {
			continue
		}
		if sn.ranges.to > to {
			break
		}
		toMerge[snaptype.Headers] = append(toMerge[snaptype.Headers], hSegments[i].seg.FilePath())
		toMerge[snaptype.Bodies] = append(toMerge[snaptype.Bodies], bSegments[i].seg.FilePath())
		toMerge[snaptype.Transactions] = append(toMerge[snaptype.Transactions], tSegments[i].Seg.FilePath())
	}

	return toMerge, nil
}

// Merge does merge segments in given ranges
func (m *Merger) Merge(ctx context.Context, snapshots *RoSnapshots, mergeRanges []Range, snapDir string, doIndex bool) error {
	if len(mergeRanges) == 0 {
		return nil
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	for _, r := range mergeRanges {
		toMerge, err := m.filesByRange(snapshots, r.from, r.to)
		if err != nil {
			return err
		}

		for _, t := range snaptype.AllSnapshotTypes {
			segName := snaptype.SegmentFileName(r.from, r.to, t)
			f, ok := snaptype.ParseFileName(snapDir, segName)
			if !ok {
				continue
			}
			if err := m.merge(ctx, toMerge[t], f.Path, logEvery); err != nil {
				return fmt.Errorf("mergeByAppendSegments: %w", err)
			}
			if doIndex {
				p := &background.Progress{}
				if err := buildIdx(ctx, f, m.chainConfig, m.tmpDir, p, m.lvl, m.logger); err != nil {
					return err
				}
			}
		}
		if err := snapshots.ReopenFolder(); err != nil {
			return fmt.Errorf("ReopenSegments: %w", err)
		}
		snapshots.LogStat()
		if m.notifier != nil { // notify about new snapshots of any size
			m.notifier.OnNewSnapshot()
			time.Sleep(1 * time.Second) // i working on blocking API - to ensure client does not use old snapsthos - and then delete them
		}
		for _, t := range snaptype.AllSnapshotTypes {
			m.removeOldFiles(toMerge[t], snapDir)
		}
	}
	m.logger.Log(m.lvl, "[snapshots] Merge done", "from", mergeRanges[0].from)
	return nil
}

func (m *Merger) merge(ctx context.Context, toMerge []string, targetFile string, logEvery *time.Ticker) error {
	var word = make([]byte, 0, 4096)
	var expectedTotal int
	cList := make([]*compress.Decompressor, len(toMerge))
	for i, cFile := range toMerge {
		d, err := compress.NewDecompressor(cFile)
		if err != nil {
			return err
		}
		defer d.Close()
		cList[i] = d
		expectedTotal += d.Count()
	}

	f, err := compress.NewCompressor(ctx, "Snapshots merge", targetFile, m.tmpDir, compress.MinPatternScore, m.compressWorkers, log.LvlTrace, m.logger)
	if err != nil {
		return err
	}
	defer f.Close()

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
			return err
		}
	}
	if f.Count() != expectedTotal {
		return fmt.Errorf("unexpected amount after segments merge. got: %d, expected: %d", f.Count(), expectedTotal)
	}
	if err = f.Compress(); err != nil {
		return err
	}
	return nil
}

func (m *Merger) removeOldFiles(toDel []string, snapDir string) {
	for _, f := range toDel {
		_ = os.Remove(f)
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
		isTxnType := strings.HasSuffix(withoutExt, snaptype.Transactions.String())
		if isTxnType {
			_ = os.Remove(withoutExt + "-to-block.idx")
		}
	}
	tmpFiles, err := snaptype.TmpFiles(snapDir)
	if err != nil {
		return
	}
	for _, f := range tmpFiles {
		_ = os.Remove(f)
	}
}

func (sn *HeaderSegment) mappedSnapshot() *silkworm.MappedHeaderSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.seg.FilePath(), sn.seg.DataHandle(), sn.seg.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.idxHeaderHash.FilePath(), sn.idxHeaderHash.DataHandle(), sn.idxHeaderHash.Size())
	return silkworm.NewMappedHeaderSnapshot(segmentRegion, idxRegion)
}

func (sn *BodySegment) mappedSnapshot() *silkworm.MappedBodySnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.seg.FilePath(), sn.seg.DataHandle(), sn.seg.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.idxBodyNumber.FilePath(), sn.idxBodyNumber.DataHandle(), sn.idxBodyNumber.Size())
	return silkworm.NewMappedBodySnapshot(segmentRegion, idxRegion)
}

func (sn *TxnSegment) mappedSnapshot() *silkworm.MappedTxnSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.Seg.FilePath(), sn.Seg.DataHandle(), sn.Seg.Size())
	idxTxnHashRegion := silkworm.NewMemoryMappedRegion(sn.IdxTxnHash.FilePath(), sn.IdxTxnHash.DataHandle(), sn.IdxTxnHash.Size())
	idxTxnHash2BlockRegion := silkworm.NewMemoryMappedRegion(sn.IdxTxnHash2BlockNum.FilePath(), sn.IdxTxnHash2BlockNum.DataHandle(), sn.IdxTxnHash2BlockNum.Size())
	return silkworm.NewMappedTxnSnapshot(segmentRegion, idxTxnHashRegion, idxTxnHash2BlockRegion)
}
