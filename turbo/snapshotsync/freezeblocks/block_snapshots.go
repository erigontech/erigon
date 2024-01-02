package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

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
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
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
)

type HeaderSegment struct {
	seg           *compress.Decompressor // value: first_byte_of_header_hash + header_rlp
	idxHeaderHash *recsplit.Index        // header_hash       -> headers_segment_offset
	Range
	version uint8
}

type BodySegment struct {
	seg           *compress.Decompressor // value: rlp(types.BodyForStorage)
	idxBodyNumber *recsplit.Index        // block_num_u64     -> bodies_segment_offset
	Range
	version uint8
}

type TxnSegment struct {
	Seg                 *compress.Decompressor // value: first_byte_of_transaction_hash + sender_address + transaction_rlp
	IdxTxnHash          *recsplit.Index        // transaction_hash  -> transactions_segment_offset
	IdxTxnHash2BlockNum *recsplit.Index        // transaction_hash  -> block_number
	Range
	version uint8
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

func (sn *HeaderSegment) openFiles() []string {
	var files []string

	if sn.seg.IsOpen() {
		files = append(files, sn.seg.FilePath())
	}

	if sn.idxHeaderHash != nil {
		files = append(files, sn.idxHeaderHash.FilePath())
	}

	return files
}

func (sn *HeaderSegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.version, sn.from, sn.to, snaptype.Headers)
	sn.seg, err = compress.NewDecompressor(filepath.Join(dir, fileName))
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
	fileName := snaptype.IdxFileName(sn.version, sn.from, sn.to, snaptype.Headers.String())
	sn.idxHeaderHash, err = recsplit.OpenIndex(filepath.Join(dir, fileName))
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

func (sn *BodySegment) openFiles() []string {
	var files []string

	if sn.seg.IsOpen() {
		files = append(files, sn.seg.FilePath())
	}

	if sn.idxBodyNumber != nil {
		files = append(files, sn.idxBodyNumber.FilePath())
	}

	return files
}

func (sn *BodySegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.version, sn.from, sn.to, snaptype.Bodies)
	sn.seg, err = compress.NewDecompressor(filepath.Join(dir, fileName))
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
	fileName := snaptype.IdxFileName(sn.version, sn.from, sn.to, snaptype.Bodies.String())
	sn.idxBodyNumber, err = recsplit.OpenIndex(filepath.Join(dir, fileName))
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

func (sn *TxnSegment) openFiles() []string {
	var files []string

	if sn.Seg.IsOpen() {
		files = append(files, sn.Seg.FilePath())
	}

	if sn.IdxTxnHash != nil && sn.IdxTxnHash.IsOpen() {
		files = append(files, sn.IdxTxnHash.FilePath())
	}

	if sn.IdxTxnHash2BlockNum != nil && sn.IdxTxnHash2BlockNum.IsOpen() {
		files = append(files, sn.IdxTxnHash2BlockNum.FilePath())
	}

	return files
}

func (sn *TxnSegment) reopenSeg(dir string) (err error) {
	sn.closeSeg()
	fileName := snaptype.SegmentFileName(sn.version, sn.from, sn.to, snaptype.Transactions)
	sn.Seg, err = compress.NewDecompressor(filepath.Join(dir, fileName))
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
	fileName := snaptype.IdxFileName(sn.version, sn.from, sn.to, snaptype.Transactions.String())
	sn.IdxTxnHash, err = recsplit.OpenIndex(filepath.Join(dir, fileName))
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

	fileName = snaptype.IdxFileName(sn.version, sn.from, sn.to, snaptype.Transactions2Block.String())
	sn.IdxTxnHash2BlockNum, err = recsplit.OpenIndex(filepath.Join(dir, fileName))
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
		if !(blockNum >= seg.from && blockNum < seg.to) {
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
		if !(blockNum >= seg.from && blockNum < seg.to) {
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
	version     uint8
	logger      log.Logger

	// allows for pruning segments - this is the min availible segment
	segmentsMin atomic.Uint64
}

// NewRoSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, version uint8, logger log.Logger) *RoSnapshots {
	return &RoSnapshots{dir: snapDir, cfg: cfg, version: version, Headers: &headerSegments{}, Bodies: &bodySegments{}, Txs: &txnSegments{}, logger: logger}
}

func (s *RoSnapshots) Version() uint8                { return s.version }
func (s *RoSnapshots) Cfg() ethconfig.BlocksFreezing { return s.cfg }
func (s *RoSnapshots) Dir() string                   { return s.dir }
func (s *RoSnapshots) SegmentsReady() bool           { return s.segmentsReady.Load() }
func (s *RoSnapshots) IndicesReady() bool            { return s.indicesReady.Load() }
func (s *RoSnapshots) IndicesMax() uint64            { return s.idxMax.Load() }
func (s *RoSnapshots) SegmentsMax() uint64           { return s.segmentsMax.Load() }
func (s *RoSnapshots) SegmentsMin() uint64           { return s.segmentsMin.Load() }
func (s *RoSnapshots) SetSegmentsMin(min uint64)     { s.segmentsMin.Store(min) }
func (s *RoSnapshots) BlocksAvailable() uint64       { return cmp.Min(s.segmentsMax.Load(), s.idxMax.Load()) }
func (s *RoSnapshots) LogStat(label string) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Blocks Stat", label),
		"blocks", fmt.Sprintf("%dk", (s.BlocksAvailable()+1)/1000),
		"indices", fmt.Sprintf("%dk", (s.IndicesMax()+1)/1000),
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
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
		headers = seg.to - 1
	}
	for _, seg := range s.Bodies.segments {
		if seg.idxBodyNumber == nil {
			break
		}
		bodies = seg.to - 1
	}
	for _, seg := range s.Txs.segments {
		if seg.IdxTxnHash == nil || seg.IdxTxnHash2BlockNum == nil {
			break
		}
		txs = seg.to - 1
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
	maxBlockNumInFiles := s.BlocksAvailable()
	for _, seg := range s.Bodies.segments {
		if seg.seg == nil {
			continue
		}
		if seg.from > maxBlockNumInFiles {
			continue
		}
		_, fName := filepath.Split(seg.seg.FilePath())
		list = append(list, fName)
	}
	for _, seg := range s.Headers.segments {
		if seg.seg == nil {
			continue
		}
		if seg.from > maxBlockNumInFiles {
			continue
		}
		_, fName := filepath.Split(seg.seg.FilePath())
		list = append(list, fName)
	}
	for _, seg := range s.Txs.segments {
		if seg.Seg == nil {
			continue
		}
		if seg.from > maxBlockNumInFiles {
			continue
		}
		_, fName := filepath.Split(seg.Seg.FilePath())
		list = append(list, fName)
	}
	slices.Sort(list)
	return list
}

func (s *RoSnapshots) OpenFiles() (list []string) {
	s.Headers.lock.RLock()
	defer s.Headers.lock.RUnlock()
	s.Bodies.lock.RLock()
	defer s.Bodies.lock.RUnlock()
	s.Txs.lock.RLock()
	defer s.Txs.lock.RUnlock()

	for _, header := range s.Headers.segments {
		list = append(list, header.openFiles()...)
	}

	for _, body := range s.Bodies.segments {
		list = append(list, body.openFiles()...)
	}

	for _, txs := range s.Txs.segments {
		list = append(list, txs.openFiles()...)
	}

	return list
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *RoSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	return s.rebuildSegments(fileNames, true, optimistic)
}

func (s *RoSnapshots) InitSegments(fileNames []string) error {
	return s.rebuildSegments(fileNames, false, true)
}

func (s *RoSnapshots) rebuildSegments(fileNames []string, open bool, optimistic bool) error {
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
				sn = &HeaderSegment{version: f.Version, Range: Range{f.From, f.To}}
			}

			if open {
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
			}

			if !exists {
				// it's possible to iterate over .seg file even if you don't have index
				// then make segment available even if index open may fail
				s.Headers.segments = append(s.Headers.segments, sn)
			}

			if open {
				if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
					return err
				}
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
				sn = &BodySegment{version: f.Version, Range: Range{f.From, f.To}}
			}

			if open {
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
			}
			if !exists {
				s.Bodies.segments = append(s.Bodies.segments, sn)
			}

			if open {
				if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
					return err
				}
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
				sn = &TxnSegment{version: f.Version, Range: Range{f.From, f.To}}
			}

			if open {
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
			}

			if !exists {
				s.Txs.segments = append(s.Txs.segments, sn)
			}

			if open {
				if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
					return err
				}
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
		ranges = append(ranges, sn.Range)
	}
	return ranges
}

func (s *RoSnapshots) OptimisticalyReopenFolder()           { _ = s.ReopenFolder() }
func (s *RoSnapshots) OptimisticalyReopenWithDB(db kv.RoDB) { _ = s.ReopenWithDB(db) }
func (s *RoSnapshots) ReopenFolder() error {
	return s.ReopenSegments(snaptype.BlockSnapshotTypes)
}

func (s *RoSnapshots) ReopenSegments(types []snaptype.Type) error {
	files, _, err := segments(s.dir, s.version, 0, func(dir string, in []snaptype.FileInfo) (res []snaptype.FileInfo) {
		return typeOfSegmentsMustExist(dir, in, types)
	})

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
		fmt.Printf("%d,  %t\n", sn.from, sn.idxHeaderHash == nil)
	}
	fmt.Println("    == Snapshots, Body")
	for _, sn := range s.Bodies.segments {
		fmt.Printf("%d,  %t\n", sn.from, sn.idxBodyNumber == nil)
	}
	fmt.Println("    == Snapshots, Txs")
	for _, sn := range s.Txs.segments {
		fmt.Printf("%d,  %t, %t\n", sn.from, sn.IdxTxnHash == nil, sn.IdxTxnHash2BlockNum == nil)
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
		if err := HeadersIdx(ctx, sn.Path, sn.Version, sn.From, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.Bodies:
		if err := BodiesIdx(ctx, sn.Path, sn.From, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.Transactions:
		dir, _ := filepath.Split(sn.Path)
		if err := TransactionsIdx(ctx, chainConfig, sn.Version, sn.From, sn.To, dir, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.BorEvents:
		dir, _ := filepath.Split(sn.Path)
		if err := BorEventsIdx(ctx, sn.Path, sn.Version, sn.From, sn.To, dir, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	case snaptype.BorSpans:
		dir, _ := filepath.Split(sn.Path)
		if err := BorSpansIdx(ctx, sn.Path, sn.Version, sn.From, sn.To, dir, tmpDir, p, lvl, logger); err != nil {
			return err
		}
	}
	//log.Info("[snapshots] finish build idx", "file", fName)
	return nil
}

func BuildMissedIndices(logPrefix string, ctx context.Context, dirs datadir.Dirs, version uint8, minIndex uint64, chainConfig *chain.Config, workers int, logger log.Logger) error {
	dir, tmpDir := dirs.Snap, dirs.Tmp
	//log.Log(lvl, "[snapshots] Build indices", "from", min)

	segments, _, err := Segments(dir, version, minIndex)
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
				sendDiagnostics(startIndexingTime, ps.DiagnossticsData(), m.Alloc, m.Sys)
				logger.Info(fmt.Sprintf("[%s] Indexing", logPrefix), "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			case <-finish:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for _, t := range snaptype.BlockSnapshotTypes {
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
				defer notifySegmentIndexingFinished(sn.Name())
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

func BuildBorMissedIndices(logPrefix string, ctx context.Context, dirs datadir.Dirs, version uint8, minIndex uint64, chainConfig *chain.Config, workers int, logger log.Logger) error {
	dir, tmpDir := dirs.Snap, dirs.Tmp

	segments, _, err := BorSegments(dir, version, minIndex)
	if err != nil {
		return err
	}
	ps := background.NewProgressSet()
	startIndexingTime := time.Now()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, t := range snaptype.BorSnapshotTypes {
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
				defer notifySegmentIndexingFinished(sn.Name())
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
			sendDiagnostics(startIndexingTime, ps.DiagnossticsData(), m.Alloc, m.Sys)
			logger.Info(fmt.Sprintf("[%s] Indexing", logPrefix), "progress", ps.String(), "total-indexing-time", time.Since(startIndexingTime).Round(time.Second).String(), "alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
		}
	}
}

func notifySegmentIndexingFinished(name string) {
	diagnostics.Send(
		diagnostics.SnapshotSegmentIndexingFinishedUpdate{
			SegmentName: name,
		},
	)
}

func sendDiagnostics(startIndexingTime time.Time, indexPercent map[string]int, alloc uint64, sys uint64) {
	segmentsStats := make([]diagnostics.SnapshotSegmentIndexingStatistics, 0, len(indexPercent))
	for k, v := range indexPercent {
		segmentsStats = append(segmentsStats, diagnostics.SnapshotSegmentIndexingStatistics{
			SegmentName: k,
			Percent:     v,
			Alloc:       alloc,
			Sys:         sys,
		})
	}
	diagnostics.Send(diagnostics.SnapshotIndexingStatistics{
		Segments:    segmentsStats,
		TimeElapsed: time.Since(startIndexingTime).Round(time.Second).Seconds(),
	})
}

func noGaps(in []snaptype.FileInfo, from uint64) (out []snaptype.FileInfo, missingSnapshots []Range) {
	prevTo := from
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

func typeOfSegmentsMustExist(dir string, in []snaptype.FileInfo, types []snaptype.Type) (res []snaptype.FileInfo) {
MainLoop:
	for _, f := range in {
		if f.From == f.To {
			continue
		}
		for _, t := range types {
			p := filepath.Join(dir, snaptype.SegmentFileName(f.Version, f.From, f.To, t))
			if !dir2.FileExist(p) {
				continue MainLoop
			}
		}
		res = append(res, f)
	}
	return res
}

func allTypeOfSegmentsMustExist(dir string, in []snaptype.FileInfo) (res []snaptype.FileInfo) {
	return typeOfSegmentsMustExist(dir, in, snaptype.BlockSnapshotTypes)
}

func borSegmentsMustExist(dir string, in []snaptype.FileInfo) (res []snaptype.FileInfo) {
	return typeOfSegmentsMustExist(dir, in, []snaptype.Type{snaptype.BorEvents, snaptype.BorSpans})
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

func SegmentsCaplin(dir string, version uint8, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	list, err := snaptype.Segments(dir, version)
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
		l, m = noGaps(noOverlaps(l), minBlock)
		res = append(res, l...)
		missingSnapshots = append(missingSnapshots, m...)
	}
	return res, missingSnapshots, nil
}

func Segments(dir string, version uint8, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	return segments(dir, version, minBlock, allTypeOfSegmentsMustExist)
}

func segments(dir string, version uint8, minBlock uint64, segmentsTypeCheck func(dir string, in []snaptype.FileInfo) []snaptype.FileInfo) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	list, err := snaptype.Segments(dir, version)
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
		l, m = noGaps(noOverlaps(segmentsTypeCheck(dir, l)), minBlock)
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
		l, _ = noGaps(noOverlaps(segmentsTypeCheck(dir, l)), minBlock)
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
		l, _ = noGaps(noOverlaps(segmentsTypeCheck(dir, l)), minBlock)
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
	maxScheduledBlock     atomic.Uint64
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
	chainConfig *chain.Config
}

func NewBlockRetire(compressWorkers int, dirs datadir.Dirs, blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter, db kv.RoDB, chainConfig *chain.Config, notifier services.DBEventNotifier, logger log.Logger) *BlockRetire {
	return &BlockRetire{workers: compressWorkers, tmpDir: dirs.Tmp, dirs: dirs, blockReader: blockReader, blockWriter: blockWriter, db: db, chainConfig: chainConfig, notifier: notifier, logger: logger}
}

func (br *BlockRetire) SetWorkers(workers int) {
	br.workers = workers
}

func (br *BlockRetire) IO() (services.FullBlockReader, *blockio.BlockWriter) {
	return br.blockReader, br.blockWriter
}

func (br *BlockRetire) Writer() *RoSnapshots { return br.blockReader.Snapshots().(*RoSnapshots) }

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
	if blockFrom%snaptype.Erigon2MergeLimit == 0 {
		maxJump = snaptype.Erigon2MergeLimit
	} else if blockFrom%100_000 == 0 {
		maxJump = 100_000
	} else if blockFrom%10_000 == 0 {
		maxJump = 10_000
	}
	//roundedTo1K := (to / 1_000) * 1_000
	jump := cmp.Min(maxJump, roundedTo1K-blockFrom)
	switch { // only next segment sizes are allowed
	case jump >= snaptype.Erigon2MergeLimit:
		blockTo = blockFrom + snaptype.Erigon2MergeLimit
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

func (br *BlockRetire) retireBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	snapshots := br.snapshots()

	blockFrom, blockTo, ok := CanRetire(maxBlockNum, minBlockNum)

	if ok {
		logger.Log(lvl, "[snapshots] Retire Blocks", "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))
		// in future we will do it in background
		if err := DumpBlocks(ctx, snapshots.version, blockFrom, blockTo, snaptype.Erigon2MergeLimit, tmpDir, snapshots.Dir(), db, workers, lvl, logger, blockReader); err != nil {
			return ok, fmt.Errorf("DumpBlocks: %w", err)
		}
		if err := snapshots.ReopenFolder(); err != nil {
			return ok, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merger := NewMerger(tmpDir, workers, lvl, db, br.chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(), snapshots.BlocksAvailable())
	if len(rangesToMerge) == 0 {
		return ok, nil
	}
	ok = true // have something to merge
	onMerge := func(r Range) error {
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		if seedNewSnapshots != nil {
			downloadRequest := []services.DownloadRequest{
				services.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return err
			}
		}
		return nil
	}
	err := merger.Merge(ctx, snapshots, rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete)
	if err != nil {
		return ok, err
	}

	return ok, nil
}

func (br *BlockRetire) PruneAncientBlocks(tx kv.RwTx, limit int) error {
	if br.blockReader.FreezingCfg().KeepBlocks {
		return nil
	}
	currentProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return err
	}
	canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBlocks())

	br.logger.Info("[snapshots] Prune Blocks", "to", canDeleteTo, "limit", limit)
	if err := br.blockWriter.PruneBlocks(context.Background(), tx, canDeleteTo, limit); err != nil {
		return err
	}
	includeBor := br.chainConfig.Bor != nil
	if includeBor {
		canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBorBlocks())
		br.logger.Info("[snapshots] Prune Bor Blocks", "to", canDeleteTo, "limit", limit)

		if err := br.blockWriter.PruneBorBlocks(context.Background(), tx, canDeleteTo, limit, span.IDAt); err != nil {
			return err
		}
	}
	return nil
}

func (br *BlockRetire) RetireBlocksInBackground(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDeleteSnapshots func(l []string) error) {
	if maxBlockNum > br.maxScheduledBlock.Load() {
		br.maxScheduledBlock.Store(maxBlockNum)
	}

	if !br.working.CompareAndSwap(false, true) {
		return
	}

	go func() {

		defer br.working.Store(false)

		for {
			maxBlockNum := br.maxScheduledBlock.Load()

			err := br.RetireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)

			if err != nil {
				br.logger.Warn("[snapshots] retire blocks", "err", err)
				return
			}

			if maxBlockNum == br.maxScheduledBlock.Load() {
				return
			}
		}
	}()
}

func (br *BlockRetire) RetireBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDeleteSnapshots func(l []string) error) (err error) {
	includeBor := br.chainConfig.Bor != nil

	if includeBor {
		// "bor snaps" can be behind "block snaps", it's ok: for example because of `kill -9` in the middle of merge
		if frozen := br.blockReader.FrozenBlocks(); frozen > minBlockNum {
			minBlockNum = frozen
		}

		for br.blockReader.FrozenBorBlocks() < minBlockNum {
			ok, err := br.retireBorBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
		}
	}

	var ok, okBor bool
	for {
		if frozen := br.blockReader.FrozenBlocks(); frozen > minBlockNum {
			minBlockNum = frozen
		}

		ok, err = br.retireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
		if err != nil {
			return err
		}

		if includeBor {
			okBor, err = br.retireBorBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
			if err != nil {
				return err
			}
		}
		haveMore := ok || okBor
		if !haveMore {
			break
		}
	}

	return nil
}

func (br *BlockRetire) BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	if err := br.buildMissedIndicesIfNeed(ctx, logPrefix, notifier, cc); err != nil {
		return err
	}

	if err := br.buildBorMissedIndicesIfNeed(ctx, logPrefix, notifier, cc); err != nil {
		return err
	}

	return nil
}

func (br *BlockRetire) buildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	snapshots := br.snapshots()
	if snapshots.IndicesMax() >= snapshots.SegmentsMax() {
		return nil
	}
	snapshots.LogStat("missed-idx")
	if !snapshots.Cfg().Produce && snapshots.IndicesMax() == 0 {
		return fmt.Errorf("please remove --snap.stop, erigon can't work without creating basic indices")
	}
	if !snapshots.Cfg().Produce {
		return nil
	}
	if !snapshots.SegmentsReady() {
		return fmt.Errorf("not all snapshot segments are available")
	}

	// wait for Downloader service to download all expected snapshots
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err := BuildMissedIndices(logPrefix, ctx, br.dirs, snapshots.Version(), snapshots.SegmentsMin(), cc, indexWorkers, br.logger); err != nil {
		return fmt.Errorf("BuildMissedIndices: %w", err)
	}

	if err := snapshots.ReopenFolder(); err != nil {
		return err
	}
	snapshots.LogStat("missed-idx:reopen")
	if notifier != nil {
		notifier.OnNewSnapshot()
	}
	return nil
}

func (br *BlockRetire) buildBorMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	if cc.Bor == nil {
		return nil
	}

	borSnapshots := br.borSnapshots()
	if borSnapshots.IndicesMax() >= borSnapshots.SegmentsMax() {
		return nil
	}

	borSnapshots.LogStat("bor:missed-idx")
	if !borSnapshots.Cfg().Produce && borSnapshots.IndicesMax() == 0 {
		return fmt.Errorf("please remove --snap.stop, erigon can't work without creating basic indices")
	}
	if !borSnapshots.Cfg().Produce {
		return nil
	}
	if !borSnapshots.SegmentsReady() {
		return fmt.Errorf("not all bor snapshot segments are available")
	}

	// wait for Downloader service to download all expected snapshots
	indexWorkers := estimate.IndexSnapshot.Workers()
	if err := BuildBorMissedIndices(logPrefix, ctx, br.dirs, borSnapshots.Version(), borSnapshots.SegmentsMin(), cc, indexWorkers, br.logger); err != nil {
		return fmt.Errorf("BuildBorMissedIndices: %w", err)
	}

	if err := borSnapshots.ReopenFolder(); err != nil {
		return err
	}
	borSnapshots.LogStat("bor:missed-idx:reopen")
	if notifier != nil {
		notifier.OnNewSnapshot()
	}
	return nil
}

func DumpBlocks(ctx context.Context, version uint8, blockFrom, blockTo, blocksPerFile uint64, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	if blocksPerFile == 0 {
		return nil
	}
	chainConfig := fromdb.ChainConfig(chainDB)

	firstTxNum := blockReader.(*BlockReader).FirstTxNumNotInSnapshots()
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, blocksPerFile) {
		lastTxNum, err := dumpBlocksRange(ctx, version, i, chooseSegmentEnd(i, blockTo, blocksPerFile), tmpDir, snapDir, firstTxNum, chainDB, *chainConfig, workers, lvl, logger)
		if err != nil {
			return err
		}
		firstTxNum = lastTxNum + 1
	}
	return nil
}

func dumpBlocksRange(ctx context.Context, version uint8, blockFrom, blockTo uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, chainConfig chain.Config, workers int, lvl log.Lvl, logger log.Logger) (lastTxNum uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	{
		segName := snaptype.SegmentFileName(version, blockFrom, blockTo, snaptype.Headers)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot Headers", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return lastTxNum, err
		}
		defer sn.Close()
		if err := DumpHeaders(ctx, chainDB, blockFrom, blockTo, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		}); err != nil {
			return lastTxNum, fmt.Errorf("DumpHeaders: %w", err)
		}
		if err := sn.Compress(); err != nil {
			return lastTxNum, fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return lastTxNum, err
		}
	}

	{
		segName := snaptype.SegmentFileName(version, blockFrom, blockTo, snaptype.Bodies)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot Bodies", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return lastTxNum, err
		}
		defer sn.Close()
		lastTxNum, err = DumpBodies(ctx, chainDB, blockFrom, blockTo, firstTxNum, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		})
		if err != nil {
			return lastTxNum, fmt.Errorf("DumpBodies: %w", err)
		}
		if err := sn.Compress(); err != nil {
			return lastTxNum, fmt.Errorf("compress: %w", err)
		}

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return lastTxNum, err
		}
	}

	{
		segName := snaptype.SegmentFileName(version, blockFrom, blockTo, snaptype.Transactions)
		f, _ := snaptype.ParseFileName(snapDir, segName)

		sn, err := compress.NewCompressor(ctx, "Snapshot Txs", f.Path, tmpDir, compress.MinPatternScore, workers, log.LvlTrace, logger)
		if err != nil {
			return lastTxNum, fmt.Errorf("NewCompressor: %w, %s", err, f.Path)
		}
		defer sn.Close()

		expectedCount, err := DumpTxs(ctx, chainDB, blockFrom, blockTo, &chainConfig, workers, lvl, logger, func(v []byte) error {
			return sn.AddWord(v)
		})
		if err != nil {
			return lastTxNum, fmt.Errorf("DumpTxs: %w", err)
		}
		if expectedCount != sn.Count() {
			return lastTxNum, fmt.Errorf("incorrect tx count: %d, expected from db: %d", sn.Count(), expectedCount)
		}
		snapDir, fileName := filepath.Split(f.Path)
		ext := filepath.Ext(fileName)
		logger.Log(lvl, "[snapshots] Compression start", "file", fileName[:len(fileName)-len(ext)], "workers", sn.Workers())
		t := time.Now()
		_, expectedCount, err = txsAmountBasedOnBodiesSnapshots(snapDir, version, blockFrom, blockTo)
		if err != nil {
			return lastTxNum, err
		}
		if expectedCount != sn.Count() {
			return lastTxNum, fmt.Errorf("incorrect tx count: %d, expected from snapshots: %d", sn.Count(), expectedCount)
		}
		if err := sn.Compress(); err != nil {
			return lastTxNum, fmt.Errorf("compress: %w", err)
		}
		logger.Log(lvl, "[snapshots] Compression", "took", time.Since(t), "ratio", sn.Ratio.String(), "file", fileName[:len(fileName)-len(ext)])

		p := &background.Progress{}
		if err := buildIdx(ctx, f, &chainConfig, tmpDir, p, lvl, logger); err != nil {
			return lastTxNum, err
		}
	}

	return lastTxNum, nil
}

func hasIdxFile(sn snaptype.FileInfo, logger log.Logger) bool {
	dir, _ := filepath.Split(sn.Path)
	fName := snaptype.IdxFileName(sn.Version, sn.From, sn.To, sn.T.String())
	var result = true
	switch sn.T {
	case snaptype.Headers, snaptype.Bodies, snaptype.BorEvents, snaptype.BorSpans, snaptype.BeaconBlocks:
		idx, err := recsplit.OpenIndex(filepath.Join(dir, fName))
		if err != nil {
			return false
		}
		idx.Close()
	case snaptype.Transactions:
		idx, err := recsplit.OpenIndex(filepath.Join(dir, fName))
		if err != nil {
			return false
		}
		idx.Close()

		fName = snaptype.IdxFileName(sn.Version, sn.From, sn.To, snaptype.Transactions2Block.String())
		idx, err = recsplit.OpenIndex(filepath.Join(dir, fName))
		if err != nil {
			return false
		}
		idx.Close()
	}
	return result
}

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 16*4096)
	},
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

	parse := func(ctx *types2.TxParseContext, v, valueBuf []byte, senders []common2.Address, j int) ([]byte, error) {
		var sender [20]byte
		slot := types2.TxSlot{}

		if _, err := ctx.ParseTransaction(v, 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
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

	addSystemTx := func(ctx *types2.TxParseContext, tx kv.Tx, txId uint64) error {
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

		ctx.WithSender(false)

		valueBuf := bufPool.Get().([]byte)
		defer bufPool.Put(valueBuf) //nolint

		valueBuf, err = parse(ctx, tv, valueBuf, nil, 0)
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

		workers := estimate.AlmostAllCPUs()

		if workers > 3 {
			workers = workers / 3 * 2
		}

		if workers > int(body.TxAmount-2) {
			if int(body.TxAmount-2) > 1 {
				workers = int(body.TxAmount - 2)
			} else {
				workers = 1
			}
		}

		parsers := errgroup.Group{}
		parsers.SetLimit(workers)

		valueBufs := make([][]byte, workers)
		parseCtxs := make([]*types2.TxParseContext, workers)

		for i := 0; i < workers; i++ {
			valueBuf := bufPool.Get().([]byte)
			defer bufPool.Put(valueBuf) //nolint
			valueBufs[i] = valueBuf
			parseCtxs[i] = types2.NewTxParseContext(*chainID)
		}

		if err := addSystemTx(parseCtxs[0], tx, body.BaseTxId); err != nil {
			return false, err
		}

		binary.BigEndian.PutUint64(numBuf, body.BaseTxId+1)

		collected := -1
		collectorLock := sync.Mutex{}
		collections := sync.NewCond(&collectorLock)

		var j int

		if err := tx.ForAmount(kv.EthTx, numBuf, body.TxAmount-2, func(_, tv []byte) error {
			tx := j
			j++

			parsers.Go(func() error {
				parseCtx := parseCtxs[tx%workers]

				parseCtx.WithSender(len(senders) == 0)
				parseCtx.WithAllowPreEip2s(blockNum <= chainConfig.HomesteadBlock.Uint64())

				valueBuf, err := parse(parseCtx, tv, valueBufs[tx%workers], senders, tx)

				if err != nil {
					return fmt.Errorf("%w, block: %d", err, blockNum)
				}

				collectorLock.Lock()
				defer collectorLock.Unlock()

				for collected < tx-1 {
					collections.Wait()
				}

				// first tx byte => sender adress => tx rlp
				if err := collect(valueBuf); err != nil {
					return err
				}

				collected = tx
				collections.Broadcast()

				return nil
			})

			return nil
		}); err != nil {
			return false, fmt.Errorf("ForAmount: %w", err)
		}

		if err := parsers.Wait(); err != nil {
			return false, fmt.Errorf("ForAmount parser: %w", err)
		}

		if err := addSystemTx(parseCtxs[0], tx, body.BaseTxId+uint64(body.TxAmount)-1); err != nil {
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
func DumpBodies(ctx context.Context, db kv.RoDB, blockFrom, blockTo uint64, firstTxNum uint64, lvl log.Lvl, logger log.Logger, collect func([]byte) error) (uint64, error) {
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
		return firstTxNum, err
	}

	return firstTxNum, nil
}

var EmptyTxHash = common2.Hash{}

func txsAmountBasedOnBodiesSnapshots(snapDir string, version uint8, blockFrom, blockTo uint64) (firstTxID uint64, expectedCount int, err error) {
	bodySegmentPath := filepath.Join(snapDir, snaptype.SegmentFileName(version, blockFrom, blockTo, snaptype.Bodies))
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

func TransactionsIdx(ctx context.Context, chainConfig *chain.Config, version uint8, blockFrom, blockTo uint64, snapDir string, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("TransactionsIdx: at=%d-%d, %v, %s", blockFrom, blockTo, rec, dbg.Stack())
		}
	}()
	firstBlockNum := blockFrom
	firstTxID, expectedCount, err := txsAmountBasedOnBodiesSnapshots(snapDir, version, blockFrom, blockTo)
	if err != nil {
		return err
	}
	bodySegmentPath := filepath.Join(snapDir, snaptype.SegmentFileName(version, blockFrom, blockTo, snaptype.Bodies))
	bodiesSegment, err := compress.NewDecompressor(bodySegmentPath)
	if err != nil {
		return
	}
	defer bodiesSegment.Close()

	segFileName := snaptype.SegmentFileName(version, blockFrom, blockTo, snaptype.Transactions)
	segmentFilePath := filepath.Join(snapDir, segFileName)
	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()
	if d.Count() != expectedCount {
		return fmt.Errorf("TransactionsIdx: at=%d-%d, pre index building, expect: %d, got %d", blockFrom, blockTo, expectedCount, d.Count())
	}

	if p != nil {
		p.Name.Store(&segFileName)
		p.Total.Store(uint64(d.Count() * 2))
	}

	txnHashIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:    d.Count(),
		Enums:       true,
		BucketSize:  2000,
		LeafSize:    8,
		TmpDir:      tmpDir,
		IndexFile:   filepath.Join(snapDir, snaptype.IdxFileName(version, blockFrom, blockTo, snaptype.Transactions.String())),
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
		IndexFile:   filepath.Join(snapDir, snaptype.IdxFileName(version, blockFrom, blockTo, snaptype.Transactions2Block.String())),
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
		if p != nil {
			p.Processed.Add(1)
		}

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
func HeadersIdx(ctx context.Context, segmentFilePath string, version uint8, firstBlockNumInSegment uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
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

	if p != nil {
		_, fname := filepath.Split(segmentFilePath)
		p.Name.Store(&fname)
		p.Total.Store(uint64(d.Count()))
	}

	hasher := crypto.NewKeccakState()
	defer cryptopool.ReturnToPoolKeccak256(hasher)
	var h common2.Hash
	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, log.LvlDebug, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if p != nil {
			p.Processed.Add(1)
		}

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

	if p != nil {
		_, fname := filepath.Split(segmentFilePath)
		p.Name.Store(&fname)
		p.Total.Store(uint64(d.Count()))
	}

	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, log.LvlDebug, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if p != nil {
			p.Processed.Add(1)
		}
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
	logger          log.Logger
	noFsync         bool // fsync is enabled by default, but tests can manually disable
}

func NewMerger(tmpDir string, compressWorkers int, lvl log.Lvl, chainDB kv.RoDB, chainConfig *chain.Config, logger log.Logger) *Merger {
	return &Merger{tmpDir: tmpDir, compressWorkers: compressWorkers, lvl: lvl, chainDB: chainDB, chainConfig: chainConfig, logger: logger}
}
func (m *Merger) DisableFsync() { m.noFsync = true }

type Range struct {
	from, to uint64
}

func (r Range) From() uint64 { return r.from }
func (r Range) To() uint64   { return r.to }

type Ranges []Range

func (r Ranges) String() string {
	return fmt.Sprintf("%d", r)
}

func (m *Merger) FindMergeRanges(currentRanges []Range, maxBlockNum uint64) (toMerge []Range) {
	for i := len(currentRanges) - 1; i > 0; i-- {
		r := currentRanges[i]
		mergeLimit := uint64(snaptype.Erigon2MergeLimit)
		if r.to-r.from >= mergeLimit {
			continue
		}
		for _, span := range snaptype.MergeSteps {
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
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}
func (v *View) BodiesSegment(blockNum uint64) (*BodySegment, bool) {
	for _, seg := range v.Bodies() {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}
func (v *View) TxsSegment(blockNum uint64) (*TxnSegment, bool) {
	for _, seg := range v.Txs() {
		if !(blockNum >= seg.from && blockNum < seg.to) {
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
		if sn.from < from {
			continue
		}
		if sn.to > to {
			break
		}
		toMerge[snaptype.Headers] = append(toMerge[snaptype.Headers], hSegments[i].seg.FilePath())
		toMerge[snaptype.Bodies] = append(toMerge[snaptype.Bodies], bSegments[i].seg.FilePath())
		toMerge[snaptype.Transactions] = append(toMerge[snaptype.Transactions], tSegments[i].Seg.FilePath())
	}

	return toMerge, nil
}

// Merge does merge segments in given ranges
func (m *Merger) Merge(ctx context.Context, snapshots *RoSnapshots, mergeRanges []Range, snapDir string, doIndex bool, onMerge func(r Range) error, onDelete func(l []string) error) error {
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

		for _, t := range snaptype.BlockSnapshotTypes {
			segName := snaptype.SegmentFileName(snapshots.version, r.from, r.to, t)
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

		snapshots.LogStat("merge")

		if onMerge != nil {
			if err := onMerge(r); err != nil {
				return err
			}
		}

		for _, t := range snaptype.BlockSnapshotTypes {
			if len(toMerge[t]) == 0 {
				continue
			}
			if onDelete != nil {
				if err := onDelete(toMerge[t]); err != nil {
					return err
				}
			}
			m.removeOldFiles(toMerge[t], snapDir, snapshots.Version())
		}
	}
	m.logger.Log(m.lvl, "[snapshots] Merge done", "from", mergeRanges[0].from, "to", mergeRanges[0].to)
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
	if m.noFsync {
		f.DisableFsync()
	}

	_, fName := filepath.Split(targetFile)
	m.logger.Debug("[snapshots] merge", "file", fName)

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

func (m *Merger) removeOldFiles(toDel []string, snapDir string, version uint8) {
	for _, f := range toDel {
		_ = os.Remove(f)
		_ = os.Remove(f + ".torrent")
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
		isTxnType := strings.HasSuffix(withoutExt, snaptype.Transactions.String())
		if isTxnType {
			_ = os.Remove(withoutExt + "-to-block.idx")
		}
	}
	tmpFiles, err := snaptype.TmpFiles(snapDir, version)
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
