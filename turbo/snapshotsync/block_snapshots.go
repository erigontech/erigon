package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
)

type BlocksSnapshot struct {
	Bodies              *compress.Decompressor // value: rlp(types.BodyForStorage)
	Headers             *compress.Decompressor // value: first_byte_of_header_hash + header_rlp
	Transactions        *compress.Decompressor // value: first_byte_of_transaction_hash + transaction_rlp
	BodyNumberIdx       *recsplit.Index        // block_num_u64     -> bodies_segment_offset
	HeaderHashIdx       *recsplit.Index        // header_hash       -> headers_segment_offset
	TxnHashIdx          *recsplit.Index        // transaction_hash  -> transactions_segment_offset
	TxnIdsIdx           *recsplit.Index        // transaction_id    -> transactions_segment_offset
	TxnHash2BlockNumIdx *recsplit.Index        // transaction_hash  -> block_number

	From, To uint64 // [from,to)
}

type Type int

const (
	Headers Type = iota
	Bodies
	Transactions
	NumberOfTypes
)

func (ft Type) String() string {
	switch ft {
	case Headers:
		return "headers"
	case Bodies:
		return "bodies"
	case Transactions:
		return "transactions"
	default:
		panic(fmt.Sprintf("unknown file type: %d", ft))
	}
}

func ParseFileType(s string) (Type, bool) {
	switch s {
	case "headers":
		return Headers, true
	case "bodies":
		return Bodies, true
	case "transactions":
		return Transactions, true
	default:
		return NumberOfTypes, false
	}
}

type IdxType string

const (
	Transactions2Block IdxType = "transactions-to-block"
	TransactionsId     IdxType = "transactions-id"
)

func (it IdxType) String() string { return string(it) }

var AllSnapshotTypes = []Type{Headers, Bodies, Transactions}

var (
	ErrInvalidFileName = fmt.Errorf("invalid compressed file name")
)

func FileName(from, to uint64, fileType string) string {
	return fmt.Sprintf("v1-%06d-%06d-%s", from/1_000, to/1_000, fileType)
}
func SegmentFileName(from, to uint64, t Type) string   { return FileName(from, to, t.String()) + ".seg" }
func DatFileName(from, to uint64, fType string) string { return FileName(from, to, fType) + ".dat" }
func IdxFileName(from, to uint64, fType string) string { return FileName(from, to, fType) + ".idx" }

func (s BlocksSnapshot) Has(block uint64) bool { return block >= s.From && block < s.To }

type RoSnapshots struct {
	indicesReady      atomic.Bool
	segmentsReady     atomic.Bool
	blocks            []*BlocksSnapshot
	dir               string
	segmentsAvailable uint64
	idxAvailable      uint64
	cfg               ethconfig.Snapshot
}

// NewRoSnapshots - opens all snapshots. But to simplify everything:
//  - it opens snapshots only on App start and immutable after
//  - all snapshots of given blocks range must exist - to make this blocks range available
//  - gaps are not allowed
//  - segment have [from:to) semantic
func NewRoSnapshots(cfg ethconfig.Snapshot, snapshotDir string) *RoSnapshots {
	return &RoSnapshots{dir: snapshotDir, cfg: cfg}
}

func (s *RoSnapshots) Cfg() ethconfig.Snapshot  { return s.cfg }
func (s *RoSnapshots) Dir() string              { return s.dir }
func (s *RoSnapshots) SegmentsReady() bool      { return s.segmentsReady.Load() }
func (s *RoSnapshots) BlocksAvailable() uint64  { return s.segmentsAvailable }
func (s *RoSnapshots) IndicesReady() bool       { return s.indicesReady.Load() }
func (s *RoSnapshots) IndicesAvailable() uint64 { return s.idxAvailable }

func (s *RoSnapshots) EnsureExpectedBlocksAreAvailable(cfg *snapshothashes.Config) error {
	if s.BlocksAvailable() < cfg.ExpectBlocks {
		return fmt.Errorf("app must wait until all expected snapshots are available. Expected: %d, Available: %d", cfg.ExpectBlocks, s.BlocksAvailable())
	}
	return nil
}

func (s *RoSnapshots) SegmentsAvailability() (headers, bodies, txs uint64, err error) {
	if headers, err = latestSegment(s.dir, Headers); err != nil {
		return
	}
	if bodies, err = latestSegment(s.dir, Bodies); err != nil {
		return
	}
	if txs, err = latestSegment(s.dir, Transactions); err != nil {
		return
	}
	return
}
func (s *RoSnapshots) IdxAvailability() (headers, bodies, txs uint64, err error) {
	if headers, err = latestIdx(s.dir, Headers); err != nil {
		return
	}
	if bodies, err = latestIdx(s.dir, Bodies); err != nil {
		return
	}
	if txs, err = latestIdx(s.dir, Transactions); err != nil {
		return
	}
	return
}

func (s *RoSnapshots) ReopenIndices() error {
	s.closeIndices()
	return s.ReopenSomeIndices(AllSnapshotTypes...)
}

func (s *RoSnapshots) ReopenSomeIndices(types ...Type) (err error) {
	for _, bs := range s.blocks {
		for _, snapshotType := range types {
			switch snapshotType {
			case Headers:
				if bs.HeaderHashIdx != nil {
					bs.HeaderHashIdx.Close()
					bs.HeaderHashIdx = nil
				}
				bs.HeaderHashIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Headers.String())))
				if err != nil {
					return err
				}
			case Bodies:
				if bs.BodyNumberIdx != nil {
					bs.BodyNumberIdx.Close()
					bs.BodyNumberIdx = nil
				}
				bs.BodyNumberIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Bodies.String())))
				if err != nil {
					return err
				}
			case Transactions:
				if bs.TxnHashIdx != nil {
					bs.TxnHashIdx.Close()
					bs.TxnHashIdx = nil
				}
				bs.TxnHashIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Transactions.String())))
				if err != nil {
					return err
				}

				if bs.TxnIdsIdx != nil {
					bs.TxnIdsIdx.Close()
					bs.TxnIdsIdx = nil
				}
				bs.TxnIdsIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, TransactionsId.String())))
				if err != nil {
					return err
				}

				if bs.TxnHash2BlockNumIdx != nil {
					bs.TxnHash2BlockNumIdx.Close()
					bs.TxnHash2BlockNumIdx = nil
				}
				bs.TxnHash2BlockNumIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Transactions2Block.String())))
				if err != nil {
					return err
				}
			default:
				panic(fmt.Sprintf("unknown snapshot type: %s", snapshotType))
			}
		}

		if bs.To > 0 {
			s.idxAvailable = bs.To - 1
		} else {
			s.idxAvailable = 0
		}
	}
	s.indicesReady.Store(true)
	return nil
}

func (s *RoSnapshots) AsyncOpenAll(ctx context.Context) {
	go func() {
		for !s.segmentsReady.Load() || !s.indicesReady.Load() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := s.ReopenSegments(); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Error("AsyncOpenAll", "err", err)
			}
			if err := s.ReopenIndices(); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Error("AsyncOpenAll", "err", err)
			}
			time.Sleep(15 * time.Second)
		}
	}()
}

func (s *RoSnapshots) ReopenSegments() error {
	s.closeSegements()
	s.closeIndices()
	s.blocks = nil
	files, err := segmentsOfType(s.dir, Headers)
	if err != nil {
		return err
	}
	for _, f := range files {
		blocksSnapshot := &BlocksSnapshot{From: f.From, To: f.To}
		{
			fileName := SegmentFileName(f.From, f.To, Bodies)
			blocksSnapshot.Bodies, err = compress.NewDecompressor(path.Join(s.dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
		}
		{
			fileName := SegmentFileName(f.From, f.To, Headers)
			blocksSnapshot.Headers, err = compress.NewDecompressor(path.Join(s.dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
		}
		{
			fileName := SegmentFileName(f.From, f.To, Transactions)
			blocksSnapshot.Transactions, err = compress.NewDecompressor(path.Join(s.dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
		}

		s.blocks = append(s.blocks, blocksSnapshot)
		if blocksSnapshot.To > 0 {
			s.segmentsAvailable = blocksSnapshot.To - 1
		} else {
			s.segmentsAvailable = 0
		}
	}
	s.segmentsReady.Store(true)
	return nil
}

func (s *RoSnapshots) Close() {
	s.closeSegements()
	s.closeIndices()
	s.blocks = nil
}

func (s *RoSnapshots) closeSegements() {
	for _, s := range s.blocks {
		if s.Headers != nil {
			s.Headers.Close()
		}
		if s.Bodies != nil {
			s.Bodies.Close()
		}
		if s.Transactions != nil {
			s.Transactions.Close()
		}
	}
}
func (s *RoSnapshots) closeIndices() {
	for _, s := range s.blocks {
		if s.HeaderHashIdx != nil {
			s.HeaderHashIdx.Close()
		}
		if s.BodyNumberIdx != nil {
			s.BodyNumberIdx.Close()
		}
		if s.TxnHashIdx != nil {
			s.TxnHashIdx.Close()
		}
		if s.TxnIdsIdx != nil {
			s.TxnIdsIdx.Close()
		}
		if s.TxnHash2BlockNumIdx != nil {
			s.TxnHash2BlockNumIdx.Close()
		}
	}
}

func (s *RoSnapshots) Blocks(blockNumber uint64) (snapshot *BlocksSnapshot, found bool) {
	if !s.indicesReady.Load() {
		return nil, false
	}

	if blockNumber > s.segmentsAvailable {
		return snapshot, false
	}
	for _, blocksSnapshot := range s.blocks {
		if blocksSnapshot.Has(blockNumber) {
			return blocksSnapshot, true
		}
	}
	return snapshot, false
}

func BuildIndices(ctx context.Context, s *RoSnapshots, snapshotDir *dir.Rw, chainID uint256.Int, tmpDir string, from uint64, lvl log.Lvl) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	for _, sn := range s.blocks {
		if sn.From < from {
			continue
		}
		f := filepath.Join(snapshotDir.Path, SegmentFileName(sn.From, sn.To, Headers))
		if err := HeadersHashIdx(ctx, f, sn.From, tmpDir, logEvery, lvl); err != nil {
			return err
		}
	}

	for _, sn := range s.blocks {
		if sn.From < from {
			continue
		}
		f := filepath.Join(snapshotDir.Path, SegmentFileName(sn.From, sn.To, Bodies))
		if err := BodiesIdx(ctx, f, sn.From, tmpDir, logEvery, lvl); err != nil {
			return err
		}
	}
	// hack to read first block body - to get baseTxId from there
	if err := s.ReopenSomeIndices(Headers, Bodies); err != nil {
		return err
	}
	for _, sn := range s.blocks {
		if sn.From < from {
			continue
		}
		// build txs idx
		gg := sn.Bodies.MakeGetter()
		buf, _ := gg.Next(nil)
		firstBody := &types.BodyForStorage{}
		if err := rlp.DecodeBytes(buf, firstBody); err != nil {
			return err
		}

		var expectedTxsAmount uint64
		{
			off := sn.BodyNumberIdx.Lookup2(sn.To - 1 - sn.From)
			gg.Reset(off)

			buf, _ = gg.Next(buf[:0])
			lastBody := new(types.BodyForStorage)
			err := rlp.DecodeBytes(buf, lastBody)
			if err != nil {
				return err
			}
			expectedTxsAmount = lastBody.BaseTxId + uint64(lastBody.TxAmount) - firstBody.BaseTxId
		}
		f := filepath.Join(snapshotDir.Path, SegmentFileName(sn.From, sn.To, Transactions))
		if err := TransactionsHashIdx(ctx, chainID, sn, firstBody.BaseTxId, sn.From, expectedTxsAmount, f, tmpDir, logEvery, lvl); err != nil {
			return err
		}
	}

	return nil
}

func latestSegment(dir string, ofType Type) (uint64, error) {
	files, err := segmentsOfType(dir, ofType)
	if err != nil {
		return 0, err
	}
	var maxBlock uint64
	for _, f := range files {
		if maxBlock < f.To {
			maxBlock = f.To
		}
	}
	if maxBlock == 0 {
		return 0, nil
	}
	return maxBlock - 1, nil
}
func latestIdx(dir string, ofType Type) (uint64, error) {
	files, err := idxFilesOfType(dir, ofType)
	if err != nil {
		return 0, err
	}
	var maxBlock uint64
	for _, f := range files {
		if maxBlock < f.To {
			maxBlock = f.To
		}
	}
	if maxBlock == 0 {
		return 0, nil
	}
	return maxBlock - 1, nil
}

// FileInfo - parsed file metadata
type FileInfo struct {
	fs.FileInfo
	Version   uint8
	From, To  uint64
	Path, Ext string
	T         Type
}

func IdxFiles(dir string) (res []FileInfo, err error) { return filesWithExt(dir, ".idx") }
func Segments(dir string) (res []FileInfo, err error) { return filesWithExt(dir, ".seg") }
func TmpFiles(dir string) (res []string, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || len(f.Name()) < 3 {
			continue
		}
		if filepath.Ext(f.Name()) != ".tmp" {
			continue
		}
		res = append(res, filepath.Join(dir, f.Name()))
	}
	return res, nil
}

func noGaps(in []FileInfo) (out []FileInfo, err error) {
	var prevTo uint64
	for _, f := range in {
		if f.To <= prevTo {
			continue
		}
		if f.From != prevTo { // no gaps
			return nil, fmt.Errorf("[open snapshots] snapshot missed: from %d to %d", prevTo, f.From)
		}
		prevTo = f.To
		out = append(out, f)
	}
	return out, nil
}
func parseDir(dir string) (res []FileInfo, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || f.Size() == 0 || len(f.Name()) < 3 {
			continue
		}

		meta, err := ParseFileName(dir, f)
		if err != nil {
			if errors.Is(err, ErrInvalidFileName) {
				continue
			}
			return nil, err
		}
		res = append(res, meta)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Version != res[j].Version {
			return res[i].Version < res[j].Version
		}
		if res[i].From != res[j].From {
			return res[i].From < res[j].From
		}
		if res[i].To != res[j].To {
			return res[i].To < res[j].To
		}
		if res[i].T != res[j].T {
			return res[i].T < res[j].T
		}
		return res[i].Ext < res[j].Ext
	})

	return res, nil
}

// noOverlaps - keep largest ranges and avoid overlap
func noOverlaps(in []FileInfo) (res []FileInfo) {
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

func segmentsOfType(dir string, ofType Type) (res []FileInfo, err error) {
	list, err := Segments(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range list {
		if f.T != ofType {
			continue
		}
		res = append(res, f)
	}
	return noGaps(noOverlaps(res))
}

func idxFilesOfType(dir string, ofType Type) (res []FileInfo, err error) {
	files, err := IdxFiles(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.T != ofType {
			continue
		}
		res = append(res, f)
	}
	return noGaps(noOverlaps(res))
}

func filterExt(in []FileInfo, expectExt string) (out []FileInfo) {
	for _, f := range in {
		if f.Ext != expectExt { // filter out only compressed files
			continue
		}
		out = append(out, f)
	}
	return out
}
func filesWithExt(dir, expectExt string) ([]FileInfo, error) {
	files, err := parseDir(dir)
	if err != nil {
		return nil, err
	}
	return filterExt(files, expectExt), nil
}

func IsCorrectFileName(name string) bool {
	parts := strings.Split(name, "-")
	return len(parts) == 4 && parts[3] != "v1"
}

func ParseFileName(dir string, f os.FileInfo) (res FileInfo, err error) {
	fileName := f.Name()
	ext := filepath.Ext(fileName)
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.Split(onlyName, "-")
	if len(parts) < 4 {
		return res, fmt.Errorf("expected format: v1-001500-002000-bodies.seg got: %s. %w", fileName, ErrInvalidFileName)
	}
	if parts[0] != "v1" {
		return res, fmt.Errorf("version: %s. %w", parts[0], ErrInvalidFileName)
	}
	from, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return
	}
	to, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return
	}
	var snapshotType Type
	ft, ok := ParseFileType(parts[3])
	if !ok {
		return res, fmt.Errorf("unexpected snapshot suffix: %s,%w", parts[2], ErrInvalidFileName)
	}
	switch ft {
	case Headers:
		snapshotType = Headers
	case Bodies:
		snapshotType = Bodies
	case Transactions:
		snapshotType = Transactions
	default:
		return res, fmt.Errorf("unexpected snapshot suffix: %s,%w", parts[2], ErrInvalidFileName)
	}
	return FileInfo{From: from * 1_000, To: to * 1_000, Path: filepath.Join(dir, fileName), T: snapshotType, FileInfo: f, Ext: ext}, nil
}

const MERGE_THRESHOLD = 2 // don't trigger merge if have too small amount of partial segments
const DEFAULT_SEGMENT_SIZE = 500_000
const MIN_SEGMENT_SIZE = 1_000

func chooseSegmentEnd(from, to, blocksPerFile uint64) uint64 {
	next := (from/blocksPerFile + 1) * blocksPerFile
	to = min(next, to)
	return to - (to % MIN_SEGMENT_SIZE) // round down to the nearest 1k
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func RetireBlocks(ctx context.Context, blockFrom, blockTo uint64, chainID uint256.Int, tmpDir string, snapshots *RoSnapshots, db kv.RoDB, workers int, lvl log.Lvl) error {
	log.Log(lvl, "[snapshots] Retire Blocks", "range", fmt.Sprintf("%dk-%dk", blockFrom/1000, blockTo/1000))
	// in future we will do it in background
	if err := DumpBlocks(ctx, blockFrom, blockTo, DEFAULT_SEGMENT_SIZE, tmpDir, snapshots.Dir(), db, workers, lvl); err != nil {
		return fmt.Errorf("DumpBlocks: %w", err)
	}
	if err := snapshots.ReopenSegments(); err != nil {
		return fmt.Errorf("ReopenSegments: %w", err)
	}
	merger := NewMerger(tmpDir, workers, lvl)
	ranges := merger.FindMergeRanges(snapshots)
	if len(ranges) == 0 {
		return nil
	}
	if err := merger.Merge(ctx, snapshots, ranges, &dir.Rw{Path: snapshots.Dir()}); err != nil {
		return err
	}
	log.Log(lvl, "[snapshots] Merge done. Indexing new segments", "from", ranges[0].from)
	if err := BuildIndices(ctx, snapshots, &dir.Rw{Path: snapshots.Dir()}, chainID, tmpDir, ranges[0].from, lvl); err != nil {
		return fmt.Errorf("BuildIndices: %w", err)
	}
	if err := snapshots.ReopenIndices(); err != nil {
		return fmt.Errorf("ReopenIndices: %w", err)
	}

	return nil
}

func DumpBlocks(ctx context.Context, blockFrom, blockTo, blocksPerFile uint64, tmpDir, snapshotDir string, chainDB kv.RoDB, workers int, lvl log.Lvl) error {
	if blocksPerFile == 0 {
		return nil
	}
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, blocksPerFile) {
		if err := dumpBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, blocksPerFile), tmpDir, snapshotDir, chainDB, workers, lvl); err != nil {
			return err
		}
	}
	return nil
}
func dumpBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapshotDir string, chainDB kv.RoDB, workers int, lvl log.Lvl) error {
	segmentFile := filepath.Join(snapshotDir, SegmentFileName(blockFrom, blockTo, Transactions))
	if _, err := DumpTxs(ctx, chainDB, segmentFile, tmpDir, blockFrom, blockTo, workers, lvl); err != nil {
		return fmt.Errorf("DumpTxs: %w", err)
	}

	segmentFile = filepath.Join(snapshotDir, SegmentFileName(blockFrom, blockTo, Bodies))
	if err := DumpBodies(ctx, chainDB, segmentFile, tmpDir, blockFrom, blockTo, workers, lvl); err != nil {
		return fmt.Errorf("DumpBodies: %w", err)
	}

	segmentFile = filepath.Join(snapshotDir, SegmentFileName(blockFrom, blockTo, Headers))
	if err := DumpHeaders(ctx, chainDB, segmentFile, tmpDir, blockFrom, blockTo, workers, lvl); err != nil {
		return fmt.Errorf("DumpHeaders: %w", err)
	}

	return nil
}

// DumpTxs - [from, to)
// Format: hash[0]_1byte + sender_address_2bytes + txnRlp
func DumpTxs(ctx context.Context, db kv.RoDB, segmentFile, tmpDir string, blockFrom, blockTo uint64, workers int, lvl log.Lvl) (firstTxID uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	chainConfig := tool.ChainConfigFromDB(db)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	f, err := compress.NewCompressor(ctx, "Transactions", segmentFile, tmpDir, compress.MinPatternScore, workers)
	if err != nil {
		return 0, fmt.Errorf("NewCompressor: %w, %s", err, segmentFile)
	}
	defer f.Close()

	var count, prevTxID uint64
	numBuf := make([]byte, binary.MaxVarintLen64)
	parseCtx := txpool.NewTxParseContext(*chainID)
	parseCtx.WithSender(false)
	slot := txpool.TxSlot{}
	var sender [20]byte
	parse := func(v, valueBuf []byte, senders []common.Address, j int) ([]byte, error) {
		if _, err := parseCtx.ParseTransaction(v, 0, &slot, sender[:], false /* hasEnvelope */); err != nil {
			return valueBuf, err
		}
		if len(senders) > 0 {
			sender = senders[j]
		}

		valueBuf = valueBuf[:0]
		valueBuf = append(valueBuf, slot.IdHash[:1]...)
		valueBuf = append(valueBuf, sender[:]...)
		valueBuf = append(valueBuf, v...)
		return valueBuf, nil
	}
	valueBuf := make([]byte, 16*4096)
	addSystemTx := func(tx kv.Tx, txId uint64) error {
		binary.BigEndian.PutUint64(numBuf, txId)
		tv, err := tx.GetOne(kv.EthTx, numBuf[:8])
		if err != nil {
			return err
		}
		if tv == nil {
			if err := f.AddWord(nil); err != nil {
				return fmt.Errorf("AddWord1: %d", err)
			}
			return nil
		}

		parseCtx.WithSender(false)
		valueBuf, err = parse(tv, valueBuf, nil, 0)
		if err != nil {
			return err
		}
		if err := f.AddWord(valueBuf); err != nil {
			return fmt.Errorf("AddWord2: %d", err)
		}
		return nil
	}

	firstIDSaved := false

	from := dbutils.EncodeBlockNumber(blockFrom)
	var lastBody types.BodyForStorage
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}

		h := common.BytesToHash(v)
		dataRLP := rawdb.ReadStorageBodyRLP(tx, h, blockNum)
		if dataRLP == nil {
			return false, fmt.Errorf("body not found: %d, %x", blockNum, h)
		}
		var body types.BodyForStorage
		if e := rlp.DecodeBytes(dataRLP, &body); e != nil {
			return false, e
		}
		lastBody = body
		if body.TxAmount == 0 {
			return true, nil
		}
		senders, err := rawdb.ReadSenders(tx, h, blockNum)
		if err != nil {
			return false, err
		}

		if !firstIDSaved {
			firstIDSaved = true
			firstTxID = body.BaseTxId
		}
		j := 0

		if err := addSystemTx(tx, body.BaseTxId); err != nil {
			return false, err
		}
		count++
		if prevTxID > 0 {
			prevTxID++
		} else {
			prevTxID = body.BaseTxId
		}
		binary.BigEndian.PutUint64(numBuf, body.BaseTxId+1)
		if err := tx.ForAmount(kv.EthTx, numBuf[:8], body.TxAmount-2, func(tk, tv []byte) error {
			id := binary.BigEndian.Uint64(tk)
			if prevTxID != 0 && id != prevTxID+1 {
				panic(fmt.Sprintf("no gaps in tx ids are allowed: block %d does jump from %d to %d", blockNum, prevTxID, id))
			}
			prevTxID = id
			parseCtx.WithSender(len(senders) == 0)
			valueBuf, err = parse(tv, valueBuf, senders, j)
			if err != nil {
				return err
			}
			if err := f.AddWord(valueBuf); err != nil {
				return err
			}
			count++
			j++

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Log(lvl, "[snapshots] Dumping txs", "block num", blockNum,
					"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
				)
			default:
			}
			return nil
		}); err != nil {
			return false, fmt.Errorf("ForAmount: %w", err)
		}

		if err := addSystemTx(tx, body.BaseTxId+uint64(body.TxAmount)-1); err != nil {
			return false, err
		}
		prevTxID++
		count++
		return true, nil
	}); err != nil {
		return 0, fmt.Errorf("BigChunks: %w", err)
	}
	if lastBody.BaseTxId+uint64(lastBody.TxAmount)-firstTxID != count {
		return 0, fmt.Errorf("incorrect tx count: %d, expected: %d", count, lastBody.BaseTxId+uint64(lastBody.TxAmount)-firstTxID)
	}
	if err := f.Compress(); err != nil {
		return 0, fmt.Errorf("compress: %w", err)
	}

	_, fileName := filepath.Split(segmentFile)
	ext := filepath.Ext(fileName)
	log.Log(lvl, "[snapshots] Compression", "ratio", f.Ratio.String(), "file", fileName[:len(fileName)-len(ext)])

	return firstTxID, nil
}

// DumpHeaders - [from, to)
func DumpHeaders(ctx context.Context, db kv.RoDB, segmentFilePath, tmpDir string, blockFrom, blockTo uint64, workers int, lvl log.Lvl) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	f, err := compress.NewCompressor(ctx, "Headers", segmentFilePath, tmpDir, compress.MinPatternScore, workers)
	if err != nil {
		return err
	}
	defer f.Close()

	key := make([]byte, 8+32)
	from := dbutils.EncodeBlockNumber(blockFrom)
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
		if err := f.AddWord(value); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Log(lvl, "[snapshots] Dumping headers", "block num", blockNum,
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := f.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}

	return nil
}

// DumpBodies - [from, to)
func DumpBodies(ctx context.Context, db kv.RoDB, segmentFilePath, tmpDir string, blockFrom, blockTo uint64, workers int, lvl log.Lvl) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	f, err := compress.NewCompressor(ctx, "Bodies", segmentFilePath, tmpDir, compress.MinPatternScore, workers)
	if err != nil {
		return err
	}
	defer f.Close()

	key := make([]byte, 8+32)
	from := dbutils.EncodeBlockNumber(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)
		dataRLP, err := tx.GetOne(kv.BlockBody, key)
		if err != nil {
			return false, err
		}
		if dataRLP == nil {
			log.Warn("header missed", "block_num", blockNum, "hash", fmt.Sprintf("%x", v))
			return true, nil
		}

		if err := f.AddWord(dataRLP); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Log(lvl, "[snapshots] Wrote into file", "block num", blockNum,
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := f.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}

	return nil
}

var EmptyTxHash = common.Hash{}

func TransactionsHashIdx(ctx context.Context, chainID uint256.Int, sn *BlocksSnapshot, firstTxID, firstBlockNum, expectedCount uint64, segmentFilePath, tmpDir string, logEvery *time.Ticker, lvl log.Lvl) error {
	dir, _ := filepath.Split(segmentFilePath)

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	buf := make([]byte, 1024)

	txnHashIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count() - d.EmptyWordsCount(),
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(dir, IdxFileName(sn.From, sn.To, Transactions.String())),
		BaseDataID: firstTxID,
	})
	if err != nil {
		return err
	}
	txnIdIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(dir, IdxFileName(sn.From, sn.To, TransactionsId.String())),
		BaseDataID: firstTxID,
	})
	if err != nil {
		return err
	}
	txnHash2BlockNumIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count() - d.EmptyWordsCount(),
		Enums:      false,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(dir, IdxFileName(sn.From, sn.To, Transactions2Block.String())),
		BaseDataID: firstBlockNum,
	})
	if err != nil {
		return err
	}

RETRY:
	txnHashIdx.NoLogs(true)
	txnIdIdx.NoLogs(true)
	txnHash2BlockNumIdx.NoLogs(true)

	ch := forEachAsync(ctx, d)
	type txHashWithOffet struct {
		txnHash   [32]byte
		i, offset uint64
		empty     bool // block may have empty txn in the beginning or end of block. such txs have no hash, but have ID
		err       error
	}
	txsCh := make(chan txHashWithOffet, 1024)
	txsCh2 := make(chan txHashWithOffet, 1024)
	go func() { //TODO: can't spawn multiple goroutines, because consumer expecting right order of txWithOffet.i
		defer close(txsCh)
		defer close(txsCh2)
		parseCtx := txpool.NewTxParseContext(chainID)
		parseCtx.WithSender(false)
		slot := txpool.TxSlot{}
		var sender [20]byte
		for it := range ch {
			if it.err != nil {
				txsCh <- txHashWithOffet{err: it.err}
				txsCh2 <- txHashWithOffet{err: it.err}
				return
			}
			if len(it.word) == 0 {
				txsCh <- txHashWithOffet{empty: true, i: it.i, offset: it.offset}
				txsCh2 <- txHashWithOffet{empty: true, i: it.i, offset: it.offset}
				continue
			}

			if _, err := parseCtx.ParseTransaction(it.word[1+20:], 0, &slot, sender[:], true /* hasEnvelope */); err != nil {
				txsCh <- txHashWithOffet{err: it.err}
				txsCh2 <- txHashWithOffet{err: it.err}
				return
			}
			txsCh <- txHashWithOffet{txnHash: slot.IdHash, i: it.i, offset: it.offset}
			txsCh2 <- txHashWithOffet{txnHash: slot.IdHash, i: it.i, offset: it.offset}
		}
	}()

	wg := sync.WaitGroup{}
	errCh := make(chan error, 3)
	defer close(errCh)
	num := make([]byte, 8)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var j uint64
		for it := range txsCh {
			if it.err != nil {
				errCh <- it.err
				return
			}
			j++
			binary.BigEndian.PutUint64(num, it.i)
			if err := txnIdIdx.AddKey(num, it.offset); err != nil {
				errCh <- it.err
				return
			}
			if it.empty {
				continue
			}
			if err := txnHashIdx.AddKey(it.txnHash[:], it.offset); err != nil {
				errCh <- it.err
				return
			}

			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}
		}

		if j != expectedCount {
			panic(fmt.Errorf("expect: %d, got %d\n", expectedCount, j))
		}

		if err := txnHashIdx.Build(); err != nil {
			errCh <- fmt.Errorf("txnHashIdx: %w", err)
		} else {
			errCh <- nil
		}
		if err := txnIdIdx.Build(); err != nil {
			errCh <- fmt.Errorf("txnIdIdx: %w", err)
		} else {
			errCh <- nil
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		blockNum := firstBlockNum
		body := &types.BodyForStorage{}
		if err := sn.Bodies.WithReadAhead(func() error {
			bodyGetter := sn.Bodies.MakeGetter()
			bodyGetter.Reset(0)
			buf, _ = bodyGetter.Next(buf[:0])
			if err := rlp.DecodeBytes(buf, body); err != nil {
				return err
			}

			for it := range txsCh2 {
				if it.err != nil {
					return it.err
				}
				for body.BaseTxId+uint64(body.TxAmount) <= firstTxID+it.i { // skip empty blocks
					if !bodyGetter.HasNext() {
						return fmt.Errorf("not enough bodies")
					}
					buf, _ = bodyGetter.Next(buf[:0])
					if err := rlp.DecodeBytes(buf, body); err != nil {
						return err
					}
					blockNum++
				}

				if it.empty {
					continue
				}

				if err := txnHash2BlockNumIdx.AddKey(it.txnHash[:], blockNum); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					log.Log(lvl, "[Snapshots Indexing] TransactionsHashIdx", "blockNum", blockNum,
						"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
				default:
				}
			}
			return nil
		}); err != nil {
			errCh <- err
			return
		}
		if err := txnHash2BlockNumIdx.Build(); err != nil {
			errCh <- fmt.Errorf("txnHash2BlockNumIdx: %w", err)
		} else {
			errCh <- nil
		}
	}()

	wg.Wait()

	for i := 0; i < 3; i++ {
		err = <-errCh
		if err != nil {
			if errors.Is(err, recsplit.ErrCollision) {
				log.Warn("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
				txnHashIdx.ResetNextSalt()
				txnIdIdx.ResetNextSalt()
				txnHash2BlockNumIdx.ResetNextSalt()
				goto RETRY
			}
			return err
		}
	}

	return nil
}

// HeadersHashIdx - headerHash -> offset (analog of kv.HeaderNumber)
func HeadersHashIdx(ctx context.Context, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, logEvery *time.Ticker, lvl log.Lvl) error {
	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		h := types.Header{}
		if err := rlp.DecodeBytes(word[1:], &h); err != nil {
			return err
		}
		if err := idx.AddKey(h.Hash().Bytes(), offset); err != nil {
			return err
		}
		//TODO: optimize by - types.RawRlpHash(word).Bytes()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Log(lvl, "[Snapshots Indexing] HeadersHashIdx", "blockNum", h.Number.Uint64(),
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
		default:
		}
		return nil
	}); err != nil {
		return fmt.Errorf("HeadersHashIdx: %w", err)
	}
	return nil
}

func BodiesIdx(ctx context.Context, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, logEvery *time.Ticker, lvl log.Lvl) error {
	num := make([]byte, 8)

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := Idx(ctx, d, firstBlockNumInSegment, tmpDir, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Log(lvl, "[Snapshots Indexing] BodyNumberIdx", "blockNum", firstBlockNumInSegment+i,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
		default:
		}
		return nil
	}); err != nil {
		return fmt.Errorf("BodyNumberIdx: %w", err)
	}
	return nil
}

type decompressItem struct {
	i, offset uint64
	word      []byte
	err       error
}

func forEachAsync(ctx context.Context, d *compress.Decompressor) chan decompressItem {
	ch := make(chan decompressItem, 1024)
	go func() {
		defer close(ch)
		if err := d.WithReadAhead(func() error {
			g := d.MakeGetter()
			var wc, pos, nextPos uint64
			word := make([]byte, 0, 4096)
			for g.HasNext() {
				word, nextPos = g.Next(word[:0])
				select {
				case <-ctx.Done():
					return nil
				case ch <- decompressItem{i: wc, offset: pos, word: common2.Copy(word)}:
				}
				wc++
				pos = nextPos
			}
			return nil
		}); err != nil {
			ch <- decompressItem{err: err}
		}
	}()
	return ch
}

// Idx - iterate over segment and building .idx file
func Idx(ctx context.Context, d *compress.Decompressor, firstDataID uint64, tmpDir string, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error) error {
	segmentFileName := d.FilePath()
	var extension = filepath.Ext(segmentFileName)
	var idxFilePath = segmentFileName[0:len(segmentFileName)-len(extension)] + ".idx"

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      true,
		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxFilePath,
		BaseDataID: firstDataID,
	})
	if err != nil {
		return err
	}

RETRY:
	ch := forEachAsync(ctx, d)
	for it := range ch {
		if it.err != nil {
			return it.err
		}
		if err := walker(rs, it.i, it.offset, it.word); err != nil {
			return err
		}
	}

	if err = rs.Build(); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			log.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
			rs.ResetNextSalt()
			goto RETRY
		}
		return err
	}

	return nil
}

func ForEachHeader(ctx context.Context, s *RoSnapshots, walker func(header *types.Header) error) error {
	r := bytes.NewReader(nil)
	for _, sn := range s.blocks {
		ch := forEachAsync(ctx, sn.Headers)
		for it := range ch {
			if it.err != nil {
				return nil
			}

			header := new(types.Header)
			r.Reset(it.word[1:])
			if err := rlp.Decode(r, header); err != nil {
				return err
			}
			if err := walker(header); err != nil {
				return err
			}
		}
	}
	return nil
}

type Merger struct {
	lvl     log.Lvl
	workers int
	tmpDir  string
}

func NewMerger(tmpDir string, workers int, lvl log.Lvl) *Merger {
	return &Merger{tmpDir: tmpDir, workers: workers, lvl: lvl}
}

/*
	a.fileLocks[fType].RLock()
	defer a.fileLocks[fType].RUnlock()
	var maxEndBlock uint64
	a.files[fType].Ascend(func(i btree.Item) bool {
		item := i.(*byEndBlockItem)
		if item.decompressor == nil {
			return true // Skip B-tree based items
		}
		pre = append(pre, item)
		if aggTo == 0 {
			var doubleEnd uint64
			nextDouble := item.endBlock
			for nextDouble <= maxEndBlock && nextDouble-item.startBlock < maxSpan {
				doubleEnd = nextDouble
				nextDouble = doubleEnd + (doubleEnd - item.startBlock) + 1
			}
			if doubleEnd != item.endBlock {
				aggFrom = item.startBlock
				aggTo = doubleEnd
			} else {
				post = append(post, item)
				return true
			}
		}
		toAggregate = append(toAggregate, item)
		return item.endBlock < aggTo
	})
*/
type mergeRange struct {
	from, to uint64
}

func (r mergeRange) String() string { return fmt.Sprintf("%dk-%dk", r.from/1000, r.to/1000) }

func (*Merger) FindMergeRanges(snapshots *RoSnapshots) (res []mergeRange) {
	for i := len(snapshots.blocks) - 1; i > 0; i-- {
		sn := snapshots.blocks[i]
		if sn.To-sn.From >= DEFAULT_SEGMENT_SIZE { // is complete .seg
			continue
		}

		for _, span := range []uint64{500_000, 100_000, 10_000} {
			if sn.To%span != 0 {
				continue
			}
			if sn.To-sn.From == span {
				break
			}
			aggFrom := sn.To - span
			res = append(res, mergeRange{from: aggFrom, to: sn.To})
			for snapshots.blocks[i].From > aggFrom {
				i--
			}
			break
		}
	}
	sort.Slice(res, func(i, j int) bool { return res[i].from < res[j].from })
	return res
}
func (m *Merger) filesByRange(snapshots *RoSnapshots, from, to uint64) (toMergeHeaders, toMergeBodies, toMergeTxs []string) {
	for _, sn := range snapshots.blocks {
		if sn.From < from {
			continue
		}
		if sn.To > to {
			break
		}

		toMergeBodies = append(toMergeBodies, sn.Bodies.FilePath())
		toMergeHeaders = append(toMergeHeaders, sn.Headers.FilePath())
		toMergeTxs = append(toMergeTxs, sn.Transactions.FilePath())
	}
	return
}

func (m *Merger) Merge(ctx context.Context, snapshots *RoSnapshots, mergeRanges []mergeRange, snapshotDir *dir.Rw) error {
	log.Log(m.lvl, "[snapshots] Merge segments", "ranges", fmt.Sprintf("%v", mergeRanges))
	for _, r := range mergeRanges {
		toMergeHeaders, toMergeBodies, toMergeTxs := m.filesByRange(snapshots, r.from, r.to)
		if err := m.merge(ctx, toMergeBodies, filepath.Join(snapshotDir.Path, SegmentFileName(r.from, r.to, Bodies))); err != nil {
			return fmt.Errorf("mergeByAppendSegments: %w", err)
		}
		if err := m.merge(ctx, toMergeHeaders, filepath.Join(snapshotDir.Path, SegmentFileName(r.from, r.to, Headers))); err != nil {
			return fmt.Errorf("mergeByAppendSegments: %w", err)
		}
		if err := m.merge(ctx, toMergeTxs, filepath.Join(snapshotDir.Path, SegmentFileName(r.from, r.to, Transactions))); err != nil {
			return fmt.Errorf("mergeByAppendSegments: %w", err)
		}
		snapshots.Close()
		if err := m.RemoveOldFiles(toMergeHeaders, toMergeBodies, toMergeTxs, &dir.Rw{Path: snapshots.Dir()}); err != nil {
			return err
		}
		if err := snapshots.ReopenSegments(); err != nil {
			return fmt.Errorf("ReopenSegments: %w", err)
		}
	}
	return nil
}

func (m *Merger) merge(ctx context.Context, toMerge []string, targetFile string) error {
	fileNames := make([]string, len(toMerge))
	for i, f := range toMerge {
		_, fName := filepath.Split(f)
		fileNames[i] = fName
	}
	f, err := compress.NewCompressor(ctx, "merge", targetFile, m.tmpDir, compress.MinPatternScore, m.workers)
	if err != nil {
		return err
	}
	defer f.Close()
	var word = make([]byte, 0, 4096)
	for _, cFile := range toMerge {
		d, err := compress.NewDecompressor(cFile)
		if err != nil {
			return err
		}
		defer d.Close()
		if err := d.WithReadAhead(func() error {
			g := d.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				if err := f.AddWord(word); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			return nil
		}); err != nil {
			return err
		}
		d.Close()
	}
	if err = f.Compress(); err != nil {
		return err
	}
	return nil
}

func (m *Merger) RemoveOldFiles(toMergeHeaders, toMergeBodies, toMergeTxs []string, snapshotsDir *dir.Rw) error {
	for _, f := range toMergeBodies {
		_ = os.Remove(f)
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
	}
	for _, f := range toMergeHeaders {
		_ = os.Remove(f)
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
	}
	for _, f := range toMergeTxs {
		_ = os.Remove(f)
		ext := filepath.Ext(f)
		withoutExt := f[:len(f)-len(ext)]
		_ = os.Remove(withoutExt + ".idx")
		_ = os.Remove(withoutExt + "-to-block.idx")
	}
	tmpFiles, err := TmpFiles(snapshotsDir.Path)
	if err != nil {
		return err
	}
	for _, f := range tmpFiles {
		_ = os.Remove(f)
	}
	return nil
}

func RecompressSegments(ctx context.Context, snapshotDir *dir.Rw, tmpDir string) error {
	allFiles, err := Segments(snapshotDir.Path)
	if err != nil {
		return err
	}
	for _, f := range allFiles {
		outFile := snapshotDir.Path + ".tmp2"
		if err := cpSegmentByWords(ctx, f.Path, outFile, tmpDir); err != nil {
			return err
		}
		if err = os.Remove(f.Path); err != nil {
			return err
		}
		if err = os.Rename(outFile, f.Path); err != nil {
			return err
		}
	}
	return nil
}

func cpSegmentByWords(ctx context.Context, srcF, dstF, tmpDir string) error {
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	workers := runtime.NumCPU() - 1
	if workers < 1 {
		workers = 1
	}
	buf := make([]byte, 4096)
	d, err := compress.NewDecompressor(srcF)
	if err != nil {
		return err
	}
	defer d.Close()
	out, err := compress.NewCompressor(ctx, "", dstF, tmpDir, compress.MinPatternScore, workers)
	if err != nil {
		return err
	}
	defer out.Close()

	i := 0
	if err := d.WithReadAhead(func() error {
		g := d.MakeGetter()
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
			if err := out.AddWord(buf); err != nil {
				return err
			}

			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info("[snapshots] Recompress", "file", srcF, "progress", fmt.Sprintf("%.2f%%", 100*float64(i)/float64(d.Count())))
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := out.Compress(); err != nil {
		return err
	}
	return nil
}

//nolint
func assertAllSegments(blocks []*BlocksSnapshot, root string) {
	wg := sync.WaitGroup{}
	for _, sn := range blocks {
		wg.Add(1)
		go func(sn *BlocksSnapshot) {
			defer wg.Done()
			f := filepath.Join(root, SegmentFileName(sn.From, sn.To, Headers))
			assertSegment(f)
			f = filepath.Join(root, SegmentFileName(sn.From, sn.To, Bodies))
			assertSegment(f)
			f = filepath.Join(root, SegmentFileName(sn.From, sn.To, Transactions))
			assertSegment(f)
			fmt.Printf("done:%s\n", f)
		}(sn)
	}
	wg.Wait()
	panic("success")
}

//nolint
func assertSegment(segmentFile string) {
	d, err := compress.NewDecompressor(segmentFile)
	if err != nil {
		panic(err)
	}
	defer d.Close()
	var buf []byte
	if err := d.WithReadAhead(func() error {
		g := d.MakeGetter()
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
		}
		return nil
	}); err != nil {
		panic(err)
	}
}
