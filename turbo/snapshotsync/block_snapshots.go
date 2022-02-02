package snapshotsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
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
	TxnHash2BlockNumIdx *recsplit.Index        // transaction_hash  -> block_number

	From, To uint64 // [from,to)
}

type SnapshotType string

const (
	Headers      SnapshotType = "headers"
	Bodies       SnapshotType = "bodies"
	Transactions SnapshotType = "transactions"
)

const (
	Transactions2Block SnapshotType = "transactions-to-block"
)

var AllSnapshotTypes = []SnapshotType{Headers, Bodies, Transactions}
var AllIdxTypes = []SnapshotType{Headers, Bodies, Transactions, Transactions2Block}

var (
	ErrInvalidCompressedFileName = fmt.Errorf("invalid compressed file name")
)

func FileName(from, to uint64, t SnapshotType) string {
	return fmt.Sprintf("v1-%06d-%06d-%s", from/1_000, to/1_000, t)
}
func SegmentFileName(from, to uint64, t SnapshotType) string { return FileName(from, to, t) + ".seg" }
func DatFileName(from, to uint64, t SnapshotType) string     { return FileName(from, to, t) + ".dat" }
func IdxFileName(from, to uint64, t SnapshotType) string     { return FileName(from, to, t) + ".idx" }

func (s BlocksSnapshot) Has(block uint64) bool { return block >= s.From && block < s.To }

type AllSnapshots struct {
	indicesReady      atomic.Bool
	segmentsReady     atomic.Bool
	blocks            []*BlocksSnapshot
	dir               string
	segmentsAvailable uint64
	idxAvailable      uint64
	cfg               ethconfig.Snapshot
}

// NewAllSnapshots - opens all snapshots. But to simplify everything:
//  - it opens snapshots only on App start and immutable after
//  - all snapshots of given blocks range must exist - to make this blocks range available
//  - gaps are not allowed
//  - segment have [from:to) semantic
func NewAllSnapshots(cfg ethconfig.Snapshot, snapshotDir string) *AllSnapshots {
	common2.MustExist(snapshotDir)
	return &AllSnapshots{dir: snapshotDir, cfg: cfg}
}

func (s *AllSnapshots) Cfg() ethconfig.Snapshot  { return s.cfg }
func (s *AllSnapshots) Dir() string              { return s.dir }
func (s *AllSnapshots) SegmentsReady() bool      { return s.segmentsReady.Load() }
func (s *AllSnapshots) BlocksAvailable() uint64  { return s.segmentsAvailable }
func (s *AllSnapshots) IndicesReady() bool       { return s.indicesReady.Load() }
func (s *AllSnapshots) IndicesAvailable() uint64 { return s.idxAvailable }

func (s *AllSnapshots) EnsureExpectedBlocksAreAvailable(cfg *snapshothashes.Config) error {
	if s.BlocksAvailable() < cfg.ExpectBlocks {
		return fmt.Errorf("app must wait until all expected snapshots are available. Expected: %d, Available: %d", cfg.ExpectBlocks, s.BlocksAvailable())
	}
	return nil
}

func (s *AllSnapshots) SegmentsAvailability() (headers, bodies, txs uint64, err error) {
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
func (s *AllSnapshots) IdxAvailability() (headers, bodies, txs uint64, err error) {
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

func (s *AllSnapshots) ReopenIndices() error { return s.ReopenSomeIndices(AllSnapshotTypes...) }

func (s *AllSnapshots) ReopenSomeIndices(types ...SnapshotType) (err error) {
	for _, bs := range s.blocks {
		for _, snapshotType := range types {
			switch snapshotType {
			case Headers:
				if bs.HeaderHashIdx != nil {
					bs.HeaderHashIdx.Close()
					bs.HeaderHashIdx = nil
				}
				bs.HeaderHashIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Headers)))
				if err != nil {
					return err
				}
			case Bodies:
				if bs.BodyNumberIdx != nil {
					bs.BodyNumberIdx.Close()
					bs.BodyNumberIdx = nil
				}
				bs.BodyNumberIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Bodies)))
				if err != nil {
					return err
				}
			case Transactions:
				if bs.TxnHashIdx != nil {
					bs.TxnHashIdx.Close()
					bs.TxnHashIdx = nil
				}
				bs.TxnHashIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Transactions)))
				if err != nil {
					return err
				}

				if bs.TxnHash2BlockNumIdx != nil {
					bs.TxnHash2BlockNumIdx.Close()
					bs.TxnHash2BlockNumIdx = nil
				}
				bs.TxnHash2BlockNumIdx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.From, bs.To, Transactions2Block)))
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

func (s *AllSnapshots) AsyncOpenAll(ctx context.Context) {
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

func (s *AllSnapshots) ReopenSegments() error {
	dir := s.dir
	files, err := segmentsOfType(dir, Headers)
	if err != nil {
		return err
	}
	var prevTo uint64
	for _, f := range files {
		from, to, _, err := ParseFileName(f, ".seg")
		if err != nil {
			if errors.Is(ErrInvalidCompressedFileName, err) {
				continue
			}
			return err
		}
		if to == prevTo {
			continue
		}
		if from != prevTo { // no gaps
			return fmt.Errorf("[open snapshots] snapshot missed: from %d to %d", prevTo, from)
			//log.Debug("[open snapshots] snapshot missed before", "file", f)
			//break
		}
		prevTo = to

		blocksSnapshot := &BlocksSnapshot{From: from, To: to}
		{
			fileName := SegmentFileName(from, to, Bodies)
			blocksSnapshot.Bodies, err = compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
		}
		{
			fileName := SegmentFileName(from, to, Headers)
			blocksSnapshot.Headers, err = compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
		}
		{
			fileName := SegmentFileName(from, to, Transactions)
			blocksSnapshot.Transactions, err = compress.NewDecompressor(path.Join(dir, fileName))
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

func (s *AllSnapshots) Close() {
	for _, s := range s.blocks {
		if s.HeaderHashIdx != nil {
			s.HeaderHashIdx.Close()
		}
		if s.Headers != nil {
			s.Headers.Close()
		}
		if s.BodyNumberIdx != nil {
			s.BodyNumberIdx.Close()
		}
		if s.Bodies != nil {
			s.Bodies.Close()
		}
		if s.TxnHashIdx != nil {
			s.TxnHashIdx.Close()
		}
		if s.Transactions != nil {
			s.Transactions.Close()
		}
	}
}

func (s *AllSnapshots) Blocks(blockNumber uint64) (snapshot *BlocksSnapshot, found bool) {
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

func (s *AllSnapshots) BuildIndices(ctx context.Context, chainID uint256.Int, tmpDir string) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	for _, sn := range s.blocks {
		f := path.Join(s.dir, SegmentFileName(sn.From, sn.To, Headers))
		if err := HeadersHashIdx(ctx, f, sn.From, tmpDir, logEvery); err != nil {
			return err
		}
	}

	for _, sn := range s.blocks {
		f := path.Join(s.dir, SegmentFileName(sn.From, sn.To, Bodies))
		if err := BodiesIdx(ctx, f, sn.From, tmpDir, logEvery); err != nil {
			return err
		}
	}
	// hack to read first block body - to get baseTxId from there
	if err := s.ReopenSomeIndices(Headers, Bodies); err != nil {
		return err
	}
	for _, sn := range s.blocks {
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
		f := path.Join(s.dir, SegmentFileName(sn.From, sn.To, Transactions))
		if err := TransactionsHashIdx(ctx, chainID, sn, firstBody.BaseTxId, sn.From, expectedTxsAmount, f, tmpDir, logEvery); err != nil {
			return err
		}
	}

	return nil
}

func latestSegment(dir string, ofType SnapshotType) (uint64, error) {
	files, err := segmentsOfType(dir, ofType)
	if err != nil {
		return 0, err
	}
	var maxBlock, prevTo uint64
	for _, f := range files {
		from, to, _, err := ParseFileName(f, ".seg")
		if err != nil {
			if errors.Is(ErrInvalidCompressedFileName, err) {
				continue
			}
			return 0, err
		}
		if from != prevTo { // no gaps
			log.Warn("[open snapshots] snapshot missed", "type", ofType, "from", prevTo, "to", from)
			break
		}
		prevTo = to
		if maxBlock < to {
			maxBlock = to
		}
	}
	if maxBlock == 0 {
		return 0, nil
	}
	return maxBlock - 1, nil
}
func latestIdx(dir string, ofType SnapshotType) (uint64, error) {
	files, err := idxFilesOfType(dir, ofType)
	if err != nil {
		return 0, err
	}
	var maxBlock, prevTo uint64
	for _, f := range files {
		from, to, _, err := ParseFileName(f, ".idx")
		if err != nil {
			if errors.Is(ErrInvalidCompressedFileName, err) {
				continue
			}
			return 0, err
		}
		if from != prevTo { // no gaps
			log.Warn("[open snapshots] snapshot missed", "type", ofType, "from", prevTo, "to", from)
			break
		}
		prevTo = to
		if maxBlock < to {
			maxBlock = to
		}
	}
	if maxBlock == 0 {
		return 0, nil
	}
	return maxBlock - 1, nil
}

func IdxFiles(dir string) ([]string, error) { return filesWithExt(dir, ".idx") }
func Segments(dir string) ([]string, error) { return filesWithExt(dir, ".seg") }

func segmentsOfType(dir string, ofType SnapshotType) ([]string, error) {
	list, err := Segments(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range list {
		if !strings.Contains(f, string(ofType)) {
			continue
		}
		res = append(res, f)
	}
	return res, nil
}

func idxFilesOfType(dir string, ofType SnapshotType) ([]string, error) {
	files, err := IdxFiles(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !strings.Contains(f, string(ofType)) {
			continue
		}
		res = append(res, f)
	}
	return res, nil
}

func filesWithExt(dir, ext string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, f := range files {
		if !IsCorrectFileName(f.Name()) {
			continue
		}
		if f.Size() == 0 {
			continue
		}
		if filepath.Ext(f.Name()) != ext { // filter out only compressed files
			continue
		}
		res = append(res, f.Name())
	}
	sort.Strings(res)
	return res, nil
}

func IsCorrectFileName(name string) bool {
	parts := strings.Split(name, "-")
	return len(parts) == 4 && parts[3] != "v1"
}

func ParseFileName(name, expectedExt string) (from, to uint64, snapshotType SnapshotType, err error) {
	_, fileName := filepath.Split(name)
	ext := filepath.Ext(fileName)
	if ext != expectedExt {
		return 0, 0, "", fmt.Errorf("%w. Ext: %s", ErrInvalidCompressedFileName, ext)
	}
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.Split(onlyName, "-")
	if len(parts) != 4 {
		return 0, 0, "", fmt.Errorf("%w. Expected format: 001500-002000-bodies-v1.seg got: %s", ErrInvalidCompressedFileName, fileName)
	}
	if parts[0] != "v1" {
		return 0, 0, "", fmt.Errorf("%w. Version: %s", ErrInvalidCompressedFileName, parts[0])
	}
	from, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return
	}
	to, err = strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return
	}
	switch SnapshotType(parts[3]) {
	case Headers:
		snapshotType = Headers
	case Bodies:
		snapshotType = Bodies
	case Transactions:
		snapshotType = Transactions
	default:
		return 0, 0, "", fmt.Errorf("%w, unexpected snapshot suffix: %s", ErrInvalidCompressedFileName, parts[2])
	}
	return from * 1_000, to * 1_000, snapshotType, nil
}

const DEFAULT_SEGMENT_SIZE = 500_000

func DumpBlocks(ctx context.Context, blockFrom, blockTo, blocksPerFile uint64, tmpDir, snapshotDir string, chainDB kv.RoDB, workers int) error {
	for i := blockFrom; i < blockTo; i += blocksPerFile {
		if err := dumpBlocksRange(ctx, i, i+blocksPerFile, tmpDir, snapshotDir, chainDB, workers); err != nil {
			return err
		}
	}
	return nil
}
func dumpBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapshotDir string, chainDB kv.RoDB, workers int) error {
	segmentFile := filepath.Join(snapshotDir, SegmentFileName(blockFrom, blockTo, Bodies))
	if err := DumpBodies(ctx, chainDB, segmentFile, tmpDir, blockFrom, blockTo, workers); err != nil {
		return err
	}

	segmentFile = filepath.Join(snapshotDir, SegmentFileName(blockFrom, blockTo, Headers))
	if err := DumpHeaders(ctx, chainDB, segmentFile, tmpDir, blockFrom, blockTo, workers); err != nil {
		return err
	}

	segmentFile = filepath.Join(snapshotDir, SegmentFileName(blockFrom, blockTo, Transactions))
	if _, err := DumpTxs(ctx, chainDB, segmentFile, tmpDir, blockFrom, blockTo, workers); err != nil {
		return err
	}
	return nil
}

// DumpTxs -
// Format: hash[0]_1byte + sender_address_2bytes + txnRlp
func DumpTxs(ctx context.Context, db kv.RoDB, segmentFile, tmpDir string, blockFrom, blockTo uint64, workers int) (firstTxID uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	chainConfig := tool.ChainConfigFromDB(db)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	f, err := compress.NewCompressor(ctx, "Transactions", segmentFile, tmpDir, compress.MinPatternScore, workers)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var count, prevTxID uint64
	numBuf := make([]byte, binary.MaxVarintLen64)
	parseCtx := txpool.NewTxParseContext(*chainID)
	parseCtx.WithSender(false)
	slot := txpool.TxSlot{}
	valueBuf := make([]byte, 16*4096)

	firstIDSaved := false

	from := dbutils.EncodeBlockNumber(blockFrom)
	var lastBody types.BodyForStorage
	var sender [20]byte
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}

		h := common.BytesToHash(v)
		dataRLP := rawdb.ReadStorageBodyRLP(tx, h, blockNum)
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

		binary.BigEndian.PutUint64(numBuf, body.BaseTxId)

		if !firstIDSaved {
			firstIDSaved = true
			firstTxID = body.BaseTxId
		}
		j := 0
		if err := tx.ForAmount(kv.EthTx, numBuf[:8], body.TxAmount, func(tk, tv []byte) error {
			id := binary.BigEndian.Uint64(tk)
			if prevTxID != 0 && id != prevTxID+1 {
				panic(fmt.Sprintf("no gaps in tx ids are allowed: block %d does jump from %d to %d", blockNum, prevTxID, id))
			}
			prevTxID = id
			parseCtx.WithSender(len(senders) == 0)
			if _, err := parseCtx.ParseTransaction(tv, 0, &slot, sender[:], false /* hasEnvelope */); err != nil {
				return err
			}
			if len(senders) > 0 {
				sender = senders[j]
			}

			valueBuf = valueBuf[:0]
			valueBuf = append(valueBuf, slot.IdHash[:1]...)
			valueBuf = append(valueBuf, sender[:]...)
			valueBuf = append(valueBuf, tv...)
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
				log.Info("Dumping txs", "block num", blockNum,
					"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
				)
			default:
			}
			return nil
		}); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return 0, err
	}
	if lastBody.BaseTxId+uint64(lastBody.TxAmount)-firstTxID != count {
		fmt.Printf("prevTxID: %d\n", prevTxID)
		return 0, fmt.Errorf("incorrect tx count: %d, expected: %d", count, lastBody.BaseTxId+uint64(lastBody.TxAmount)-firstTxID)
	}
	if err := f.Compress(); err != nil {
		return 0, err
	}

	_, fileName := filepath.Split(segmentFile)
	log.Info("[Transactions] Compression", "ratio", f.Ratio.String(), "file", fileName)
	return firstTxID, nil
}

func DumpHeaders(ctx context.Context, db kv.RoDB, segmentFilePath, tmpDir string, blockFrom, blockTo uint64, workers int) error {
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
			log.Info("Dumping headers", "block num", blockNum,
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := f.Compress(); err != nil {
		return err
	}

	return nil
}

func DumpBodies(ctx context.Context, db kv.RoDB, segmentFilePath, tmpDir string, blockFrom, blockTo uint64, workers int) error {
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
			log.Info("Wrote into file", "block num", blockNum,
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := f.Compress(); err != nil {
		return err
	}
	return nil
}

func TransactionsHashIdx(ctx context.Context, chainID uint256.Int, sn *BlocksSnapshot, firstTxID, firstBlockNum, expectedCount uint64, segmentFilePath, tmpDir string, logEvery *time.Ticker) error {
	dir, _ := filepath.Split(segmentFilePath)

	parseCtx := txpool.NewTxParseContext(chainID)
	parseCtx.WithSender(false)
	slot := txpool.TxSlot{}
	var sender [20]byte
	var j uint64

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	buf := make([]byte, 1024)

	txnHashIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      true,
		BucketSize: 2000,
		Salt:       0,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  path.Join(dir, IdxFileName(sn.From, sn.To, Transactions)),
		BaseDataID: firstTxID,
	})
	if err != nil {
		return err
	}
	txnHash2BlockNumIdx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      true,
		BucketSize: 2000,
		Salt:       0,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  path.Join(dir, IdxFileName(sn.From, sn.To, Transactions2Block)),
		BaseDataID: firstBlockNum,
	})
	if err != nil {
		return err
	}

RETRY:
	blockNum := firstBlockNum
	body := &types.BodyForStorage{}
	if err := sn.Bodies.WithReadAhead(func() error {
		bodyGetter := sn.Bodies.MakeGetter()
		bodyGetter.Reset(0)
		buf, _ = bodyGetter.Next(buf[:0])
		if err := rlp.DecodeBytes(buf, body); err != nil {
			return err
		}

		if err := forEach(d, func(i, offset uint64, word []byte) error {
			if _, err := parseCtx.ParseTransaction(word[1+20:], 0, &slot, sender[:], true /* hasEnvelope */); err != nil {
				return err
			}
			if err := txnHashIdx.AddKey(slot.IdHash[:], offset); err != nil {
				return err
			}

			for body.BaseTxId+uint64(body.TxAmount) <= firstTxID+i { // skip empty blocks
				if !bodyGetter.HasNext() {
					return fmt.Errorf("not enough bodies")
				}
				buf, _ = bodyGetter.Next(buf[:0])
				if err := rlp.DecodeBytes(buf, body); err != nil {
					return err
				}
				blockNum++
			}

			if err := txnHash2BlockNumIdx.AddKey(slot.IdHash[:], blockNum); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info("[Snapshots Indexing] TransactionsHashIdx", "blockNum", blockNum)
			default:
			}
			j++
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	if err = txnHashIdx.Build(); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			log.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
			txnHashIdx.ResetNextSalt()
			txnHash2BlockNumIdx.ResetNextSalt()
			goto RETRY
		}
		return err
	}
	if err = txnHash2BlockNumIdx.Build(); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			log.Info("Building recsplit. Collision happened. It's ok. Restarting with another salt...", "err", err)
			txnHashIdx.ResetNextSalt()
			txnHash2BlockNumIdx.ResetNextSalt()
			goto RETRY
		}
		return err
	}

	if j != expectedCount {
		panic(fmt.Errorf("expect: %d, got %d\n", expectedCount, j))
	}

	return nil
}

// HeadersHashIdx - headerHash -> offset (analog of kv.HeaderNumber)
func HeadersHashIdx(ctx context.Context, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, logEvery *time.Ticker) error {
	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := Idx(d, firstBlockNumInSegment, tmpDir, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
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
			log.Info("[Snapshots Indexing] HeadersHashIdx", "blockNumber", h.Number.Uint64())
		default:
		}
		return nil
	}); err != nil {
		return fmt.Errorf("HeadersHashIdx: %w", err)
	}
	return nil
}

func BodiesIdx(ctx context.Context, segmentFilePath string, firstBlockNumInSegment uint64, tmpDir string, logEvery *time.Ticker) error {
	num := make([]byte, 8)

	d, err := compress.NewDecompressor(segmentFilePath)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := Idx(d, firstBlockNumInSegment, tmpDir, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("[Snapshots Indexing] BodyNumberIdx", "blockNumber", firstBlockNumInSegment+i)
		default:
		}
		return nil
	}); err != nil {
		return fmt.Errorf("BodyNumberIdx: %w", err)
	}
	return nil
}

//forEach - only reason why this func exists - is that .Next returns "nextPos" instead of "pos". If fix this in future - then can remove this func
func forEach(d *compress.Decompressor, walker func(i, offset uint64, word []byte) error) error {
	if err := d.WithReadAhead(func() error {
		g := d.MakeGetter()
		var wc, pos, nextPos uint64
		word := make([]byte, 0, 4096)
		for g.HasNext() {
			word, nextPos = g.Next(word[:0])
			if err := walker(wc, pos, word); err != nil {
				return err
			}
			wc++
			pos = nextPos
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// Idx - iterate over segment and building .idx file
func Idx(d *compress.Decompressor, firstDataID uint64, tmpDir string, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error) error {
	segmentFileName := d.FilePath()
	var extension = filepath.Ext(segmentFileName)
	var idxFilePath = segmentFileName[0:len(segmentFileName)-len(extension)] + ".idx"

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      true,
		BucketSize: 2000,
		Salt:       0,
		LeafSize:   8,
		TmpDir:     tmpDir,
		IndexFile:  idxFilePath,
		BaseDataID: firstDataID,
	})
	if err != nil {
		return err
	}

RETRY:

	if err = d.WithReadAhead(func() error {
		g := d.MakeGetter()
		var wc, pos, nextPos uint64
		word := make([]byte, 0, 4096)
		for g.HasNext() {
			word, nextPos = g.Next(word[:0])
			if err := walker(rs, wc, pos, word); err != nil {
				return err
			}
			wc++
			pos = nextPos
		}
		return nil
	}); err != nil {
		return err
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

func ForEachHeader(s *AllSnapshots, walker func(header *types.Header) error) error {
	word := make([]byte, 0, 4096)
	r := bytes.NewReader(nil)
	for _, sn := range s.blocks {
		d := sn.Headers
		if err := d.WithReadAhead(func() error {
			g := d.MakeGetter()
			for g.HasNext() {
				header := new(types.Header)
				word, _ = g.Next(word[:0])
				r.Reset(word[1:])
				if err := rlp.Decode(r, header); err != nil {
					return err
				}
				if err := walker(header); err != nil {
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
