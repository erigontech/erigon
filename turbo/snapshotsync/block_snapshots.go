package snapshotsync

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

type SnapshotType string

const (
	Headers      SnapshotType = "headers"
	Bodies       SnapshotType = "bodies"
	Transactions SnapshotType = "transactions"
)

var (
	ErrInvalidCompressedFileName = fmt.Errorf("invalid compressed file name")
)

func FileName(from, to uint64, name SnapshotType) string {
	return fmt.Sprintf("v1-%06d-%06d-%s", from/1_000, to/1_000, name)
}

func SegmentFileName(from, to uint64, name SnapshotType) string {
	return FileName(from, to, name) + ".seg"
}

func TmpFileName(from, to uint64, name SnapshotType) string {
	return FileName(from, to, name) + ".dat"
}

func IdxFileName(from, to uint64, name SnapshotType) string {
	return FileName(from, to, name) + ".idx"
}

type Snapshot struct {
	File    string
	Idx     *recsplit.Index
	Segment *compress.Decompressor
	From    uint64 // included
	To      uint64 // excluded
}

func (s Snapshot) Has(block uint64) bool { return block >= s.From && block < s.To }

type BlocksSnapshot struct {
	Bodies       *Snapshot
	Headers      *Snapshot
	Transactions *Snapshot
	From         uint64 // included
	To           uint64 // excluded
}

func (s BlocksSnapshot) Has(block uint64) bool { return block >= s.From && block < s.To }

type AllSnapshots struct {
	dir                  string
	allSegmentsAvailable bool
	allIdxAvailable      bool
	blocksAvailable      uint64
	blocks               []*BlocksSnapshot
	cfg                  *params.SnapshotsConfig
}

// NewAllSnapshots - opens all snapshots. But to simplify everything:
//  - it opens snapshots only on App start and immutable after
//  - all snapshots of given blocks range must exist - to make this blocks range available
//  - gaps are not allowed
//  - segment have [from:to) semantic
func NewAllSnapshots(dir string, cfg *params.SnapshotsConfig) *AllSnapshots {
	return &AllSnapshots{dir: dir, cfg: cfg}
}

func (s *AllSnapshots) ChainSnapshotConfig() *params.SnapshotsConfig {
	return s.cfg
}

func (s *AllSnapshots) AllSegmentsAvailable() bool     { return s.allSegmentsAvailable }
func (s *AllSnapshots) SetAllSegmentsAvailable(v bool) { s.allSegmentsAvailable = v }
func (s *AllSnapshots) BlocksAvailable() uint64        { return s.blocksAvailable }
func (s *AllSnapshots) AllIdxAvailable() bool          { return s.allIdxAvailable }
func (s *AllSnapshots) SetAllIdxAvailable(v bool)      { s.allIdxAvailable = v }
func (s *AllSnapshots) IndicesAvailable() uint64       { return s.blocks[len(s.blocks)-1].Transactions.To }

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
func (s *AllSnapshots) ReopenIndices() error {
	for _, bs := range s.blocks {
		if bs.Headers.Idx != nil {
			bs.Headers.Idx.Close()
			bs.Headers.Idx = nil
		}
		idx, err := recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.Headers.From, bs.Headers.To, Headers)))
		if err != nil {
			return err
		}
		bs.Headers.Idx = idx

		if bs.Bodies.Idx != nil {
			bs.Bodies.Idx.Close()
			bs.Bodies.Idx = nil
		}
		idx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.Bodies.From, bs.Bodies.To, Bodies)))
		if err != nil {
			return err
		}
		bs.Bodies.Idx = idx

		if bs.Transactions.Idx != nil {
			bs.Transactions.Idx.Close()
			bs.Transactions.Idx = nil
		}
		idx, err = recsplit.OpenIndex(path.Join(s.dir, IdxFileName(bs.Transactions.From, bs.Transactions.To, Transactions)))
		if err != nil {
			return err
		}
		bs.Transactions.Idx = idx
	}
	return nil
}

func (s *AllSnapshots) ReopenSegments() error {
	dir := s.dir
	files, err := segments(dir, Headers)
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
			log.Debug("[open snapshots] snapshot missed before", "file", f)
			break
		}
		prevTo = to

		blocksSnapshot := &BlocksSnapshot{From: from, To: to}
		{
			fileName := SegmentFileName(from, to, Bodies)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
			blocksSnapshot.Bodies = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Segment: d}
		}
		{
			fileName := SegmentFileName(from, to, Headers)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
			blocksSnapshot.Headers = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Segment: d}
		}
		{
			fileName := SegmentFileName(from, to, Transactions)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
			blocksSnapshot.Transactions = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Segment: d}
		}

		s.blocks = append(s.blocks, blocksSnapshot)
		s.blocksAvailable = blocksSnapshot.To
	}
	return nil
}

func (s *AllSnapshots) Close() {
	for _, s := range s.blocks {
		if s.Headers.Idx != nil {
			s.Headers.Idx.Close()
		}
		if s.Headers.Segment != nil {
			s.Headers.Segment.Close()
		}
		if s.Bodies.Idx != nil {
			s.Bodies.Idx.Close()
		}
		if s.Bodies.Segment != nil {
			s.Bodies.Segment.Close()
		}
		if s.Transactions.Idx != nil {
			s.Transactions.Idx.Close()
		}
		if s.Transactions.Segment != nil {
			s.Transactions.Segment.Close()
		}
	}
}

func (s *AllSnapshots) Blocks(blockNumber uint64) (snapshot *BlocksSnapshot, found bool) {
	if blockNumber > s.blocksAvailable {
		return snapshot, false
	}
	for _, blocksSnapshot := range s.blocks {
		if blocksSnapshot.Has(blockNumber) {
			return blocksSnapshot, true
		}
	}
	return snapshot, false
}

func (s *AllSnapshots) BuildIndices(chainID uint256.Int) error {
	for _, sn := range s.blocks {
		f := path.Join(s.dir, SegmentFileName(sn.Headers.From, sn.Headers.To, Headers))
		if err := HeadersHashIdx(f, sn.Headers.From); err != nil {
			return err
		}

		f = path.Join(s.dir, SegmentFileName(sn.Bodies.From, sn.Bodies.To, Bodies))
		if err := BodiesIdx(f, sn.Bodies.From); err != nil {
			return err
		}

		f = path.Join(s.dir, SegmentFileName(sn.Transactions.From, sn.Transactions.To, Transactions))
		if err := TransactionsHashIdx(chainID, sn.Transactions.From*1_000_000, f); err != nil {
			return err
		}
	}
	return nil
}

func latestSegment(dir string, ofType SnapshotType) (uint64, error) {
	files, err := segments(dir, ofType)
	if err != nil {
		return 0, err
	}
	maxBlock := uint64(0)
	for _, f := range files {
		_, to, _, err := ParseFileName(f, ".seg")
		if err != nil {
			if errors.Is(ErrInvalidCompressedFileName, err) {
				continue
			}
			return 0, err
		}
		if maxBlock < to {
			maxBlock = to
		}
	}
	return maxBlock, nil
}
func latestIdx(dir string, ofType SnapshotType) (uint64, error) {
	files, err := idxFiles(dir, ofType)
	if err != nil {
		return 0, err
	}
	maxBlock := uint64(0)
	for _, f := range files {
		_, to, _, err := ParseFileName(f, ".idx")
		if err != nil {
			if errors.Is(ErrInvalidCompressedFileName, err) {
				continue
			}
			return 0, err
		}
		if maxBlock < to {
			maxBlock = to
		}
	}
	return maxBlock, nil
}

func segments(dir string, ofType SnapshotType) ([]string, error) {
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
		if filepath.Ext(f.Name()) != ".seg" { // filter out only compressed files
			continue
		}
		if !strings.Contains(f.Name(), string(ofType)) {
			continue
		}
		res = append(res, f.Name())
	}
	sort.Strings(res)
	return res, nil
}
func idxFiles(dir string, ofType SnapshotType) ([]string, error) {
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
		if filepath.Ext(f.Name()) != ".idx" { // filter out only compressed files
			continue
		}
		if !strings.Contains(f.Name(), string(ofType)) {
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

// DumpTxs -
// Format: hash[0]_1byte + sender_address_2bytes + txnRlp
func DumpTxs(db kv.RoDB, tmpdir string, fromBlock uint64, blocksAmount int) (firstTxID uint64, err error) {
	tmpFileName := TmpFileName(fromBlock, fromBlock+uint64(blocksAmount), Transactions)
	tmpFileName = path.Join(tmpdir, tmpFileName)

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	chainConfig := tool.ChainConfigFromDB(db)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	f, err := NewSimpleFile(tmpFileName)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	i := 0
	numBuf := make([]byte, binary.MaxVarintLen64)
	parseCtx := txpool.NewTxParseContext(*chainID)
	parseCtx.WithSender(false)
	slot := txpool.TxSlot{}
	valueBuf := make([]byte, 16*4096)

	firstIDSaved := false

	from := dbutils.EncodeBlockNumber(fromBlock)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= fromBlock+uint64(blocksAmount) {
			return false, nil
		}

		h := common.BytesToHash(v)
		dataRLP := rawdb.ReadStorageBodyRLP(tx, h, blockNum)
		var body types.BodyForStorage
		if e := rlp.DecodeBytes(dataRLP, &body); e != nil {
			return false, e
		}
		if body.TxAmount == 0 {
			return true, nil
		}
		senders, err := rawdb.ReadSenders(tx, h, blockNum)
		if err != nil {
			return false, err
		}

		binary.BigEndian.PutUint64(numBuf, body.BaseTxId)
		j := 0

		if !firstIDSaved {
			firstIDSaved = true
			firstTxID = body.BaseTxId
		}
		if err := tx.ForAmount(kv.EthTx, numBuf[:8], body.TxAmount, func(tk, tv []byte) error {
			if _, err := parseCtx.ParseTransaction(tv, 0, &slot, nil); err != nil {
				return err
			}
			var sender []byte
			if len(senders) > 0 {
				sender = senders[j][:]
			} else {
				sender = make([]byte, 20) // TODO: return error here
				//panic("not implemented")
			}
			_ = sender
			valueBuf = valueBuf[:0]
			valueBuf = append(valueBuf, slot.IdHash[:1]...)
			valueBuf = append(valueBuf, sender...)
			valueBuf = append(valueBuf, tv...)
			if err := f.Append(valueBuf); err != nil {
				return err
			}
			i++
			j++

			select {
			default:
			case <-logEvery.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info("Dumping txs", "million txs", i/1_000_000, "block num", blockNum,
					"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
				)
			}
			return nil
		}); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return 0, err
	}

	return firstTxID, nil
}

func DumpHeaders(db kv.RoDB, tmpdir string, fromBlock uint64, blocksAmount int) error {
	tmpFileName := TmpFileName(fromBlock, fromBlock+uint64(blocksAmount), Headers)
	tmpFileName = path.Join(tmpdir, tmpFileName)

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	f, err := NewSimpleFile(tmpFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	key := make([]byte, 8+32)
	from := dbutils.EncodeBlockNumber(fromBlock)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= fromBlock+uint64(blocksAmount) {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)
		dataRLP, err := tx.GetOne(kv.Headers, key)
		if err != nil {
			return false, err
		}
		if dataRLP == nil {
			log.Warn("header missed", "block_num", blockNum, "hash", fmt.Sprintf("%x", v))
			return true, nil
		}
		if err := f.Append(dataRLP); err != nil {
			return false, err
		}

		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Dumping headers", "block num", blockNum,
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
			)
		}
		return true, nil
	}); err != nil {
		return err
	}

	return nil
}

func DumpBodies(db kv.RoDB, tmpdir string, fromBlock uint64, blocksAmount int) error {
	tmpFileName := TmpFileName(fromBlock, fromBlock+uint64(blocksAmount), Bodies)
	tmpFileName = path.Join(tmpdir, tmpFileName)

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	f, err := os.Create(tmpFileName)
	if err != nil {
		return err
	}
	defer f.Sync()
	defer f.Close()
	w := bufio.NewWriterSize(f, etl.BufIOSize)
	defer w.Flush()

	key := make([]byte, 8+32)
	from := dbutils.EncodeBlockNumber(fromBlock)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= fromBlock+uint64(blocksAmount) {
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

		numBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(numBuf, uint64(len(dataRLP)))
		if _, e := w.Write(numBuf[:n]); e != nil {
			return false, e
		}
		if len(dataRLP) > 0 {
			if _, e := w.Write(dataRLP); e != nil {
				return false, e
			}
		}

		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Wrote into file", "block num", blockNum,
				"alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys),
			)
		}
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func TransactionsHashIdx(chainID uint256.Int, firstTxID uint64, segmentFileName string) error {
	parseCtx := txpool.NewTxParseContext(chainID)
	parseCtx.WithSender(false)
	slot := txpool.TxSlot{}
	var sender [20]byte
	if err := Idx(segmentFileName, firstTxID, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if _, err := parseCtx.ParseTransaction(word[1+20:], 0, &slot, sender[:]); err != nil {
			return err
		}
		if err := idx.AddKey(slot.IdHash[:], offset); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("TransactionsHashIdx: %w", err)
	}
	return nil
}

// HeadersHashIdx - headerHash -> offset (analog of kv.HeaderNumber)
func HeadersHashIdx(segmentFileName string, firstBlockNumInSegment uint64) error {
	if err := Idx(segmentFileName, firstBlockNumInSegment, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		h := types.Header{}
		if err := rlp.DecodeBytes(word, &h); err != nil {
			return err
		}
		return idx.AddKey(h.Hash().Bytes(), offset)
	}); err != nil {
		return fmt.Errorf("HeadersHashIdx: %w", err)
	}
	return nil
}

func BodiesIdx(segmentFileName string, firstBlockNumInSegment uint64) error {
	num := make([]byte, 8)
	if err := Idx(segmentFileName, firstBlockNumInSegment, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		n := binary.PutUvarint(num, i)
		return idx.AddKey(num[:n], offset)
	}); err != nil {
		return fmt.Errorf("BodiesIdx: %w", err)
	}
	return nil
}

// Idx - iterate over segment and building .idx file
func Idx(segmentFileName string, firstDataID uint64, walker func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error) error {
	var extension = filepath.Ext(segmentFileName)
	var idxFileName = segmentFileName[0:len(segmentFileName)-len(extension)] + ".idx"

	d, err := compress.NewDecompressor(segmentFileName)
	if err != nil {
		return err
	}
	defer d.Close()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   d.Count(),
		Enums:      true,
		BucketSize: 2000,
		Salt:       0,
		LeafSize:   8,
		TmpDir:     "",
		IndexFile:  idxFileName,
		BaseDataID: firstDataID,
	})
	if err != nil {
		return err
	}

RETRY:

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
		select {
		default:
		case <-logEvery.C:
			log.Info("[Filling recsplit] Processed", "millions", wc/1_000_000)
		}
	}

	if err = rs.Build(); err != nil {
		if errors.Is(err, recsplit.ErrCollision) {
			log.Info("Building recsplit. Collision happened. It's ok. Restarting...", "err", err)
			rs.ResetNextSalt()
			goto RETRY
		}
		return err
	}

	return nil
}

type SimpleFile struct {
	name  string
	f     *os.File
	w     *bufio.Writer
	count uint64
	buf   []byte
}

func NewSimpleFile(name string) (*SimpleFile, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, etl.BufIOSize)
	return &SimpleFile{name: name, f: f, w: w, buf: make([]byte, 128)}, nil
}
func (f *SimpleFile) Close() {
	f.w.Flush()
	f.f.Sync()
	//TODO: write f.count to begin of the file after sync
	f.f.Close()
}
func (f *SimpleFile) Append(v []byte) error {
	f.count++
	n := binary.PutUvarint(f.buf, uint64(len(v)))
	if _, e := f.w.Write(f.buf[:n]); e != nil {
		return e
	}
	if len(v) > 0 {
		if _, e := f.w.Write(v); e != nil {
			return e
		}
	}
	return nil
}
func ReadSimpleFile(fileName string, walker func(v []byte) error) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, etl.BufIOSize)
	var buf []byte
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		if len(buf) < int(l) {
			buf = make([]byte, l)
		}
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		if err := walker(buf[:l]); err != nil {
			return err
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	return nil
}

type SnapshotInfo struct {
	MinimumAvailability uint64
}

func FileCheckSum(file string) (common.Hash, error) {
	f, err := os.Open(file)
	if err != nil {
		return common.Hash{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return common.Hash{}, err
	}

	hash := h.Sum(nil)
	return common.BytesToHash(hash), nil
}
