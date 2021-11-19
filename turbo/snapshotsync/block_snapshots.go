package snapshotsync

import (
	"bufio"
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
	"github.com/ledgerwatch/erigon/core/types"
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
	File         string
	Idx          *recsplit.Index
	Decompressor *compress.Decompressor
	From         uint64 // included
	To           uint64 // excluded
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
	dir             string
	blocksAvailable uint64
	blocks          []*BlocksSnapshot
}

func MustOpenAll(dir string) *AllSnapshots {
	res, err := OpenAll(dir)
	if err != nil {
		panic(err)
	}
	return res
}

// OpenAll - opens all snapshots. But to simplify everything:
//  - it opens snapshots only on App start and immutable after
//  - all snapshots of given blocks range must exist - to make this blocks range available
//  - gaps are not allowed
//  - segment have [from:to) semantic
func OpenAll(dir string) (*AllSnapshots, error) {
	all := &AllSnapshots{dir: dir}
	files, err := onlyCompressedFilesList(dir)
	if err != nil {
		return nil, err
	}
	var prevTo uint64
	for _, f := range files {
		from, to, _, err := ParseCompressedFileName(f)
		if err != nil {
			if errors.Is(ErrInvalidCompressedFileName, err) {
				continue
			}
			return nil, err
		}
		if to == prevTo {
			continue
		}
		if from != prevTo { // no gaps
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
				return nil, err
			}

			idx, err := recsplit.OpenIndex(path.Join(dir, IdxFileName(from, to, Bodies)))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			blocksSnapshot.Bodies = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Decompressor: d, Idx: idx}
		}
		{
			fileName := SegmentFileName(from, to, Headers)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			idx, err := recsplit.OpenIndex(path.Join(dir, IdxFileName(from, to, Headers)))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}

			blocksSnapshot.Headers = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Decompressor: d, Idx: idx}
		}
		{
			fileName := SegmentFileName(from, to, Transactions)
			d, err := compress.NewDecompressor(path.Join(dir, fileName))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			idx, err := recsplit.OpenIndex(path.Join(dir, IdxFileName(from, to, Transactions)))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, err
			}
			blocksSnapshot.Transactions = &Snapshot{From: from, To: to, File: path.Join(dir, fileName), Decompressor: d, Idx: idx}
		}

		all.blocks = append(all.blocks, blocksSnapshot)
		all.blocksAvailable = blocksSnapshot.To
	}
	return all, nil
}

func (s AllSnapshots) Close() {
	for _, s := range s.blocks {
		s.Headers.Idx.Close()
		s.Headers.Decompressor.Close()
		s.Bodies.Idx.Close()
		s.Bodies.Decompressor.Close()
		s.Transactions.Idx.Close()
		s.Transactions.Decompressor.Close()
	}
}

func (s AllSnapshots) Blocks(blockNumber uint64) (snapshot *BlocksSnapshot, found bool) {
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

func onlyCompressedFilesList(dir string) ([]string, error) {
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
		res = append(res, f.Name())
	}
	sort.Strings(res)
	return res, nil
}

func IsCorrectFileName(name string) bool {
	parts := strings.Split(name, "-")
	return len(parts) == 4 && parts[3] != "v1"
}

func ParseCompressedFileName(name string) (from, to uint64, snapshotType SnapshotType, err error) {
	_, fileName := filepath.Split(name)
	ext := filepath.Ext(fileName)
	if ext != ".seg" {
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

func DumpTxs(db kv.RoDB, tmpdir string, fromBlock uint64, blocksAmount int) error {
	tmpFileName := TmpFileName(fromBlock, fromBlock+uint64(blocksAmount), Transactions)
	tmpFileName = path.Join(tmpdir, tmpFileName)

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	chainConfig := tool.ChainConfigFromDB(db)
	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	f, err := NewSimpleFile(tmpFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	i := 0
	numBuf := make([]byte, binary.MaxVarintLen64)
	parseCtx := txpool.NewTxParseContext(*chainID)
	parseCtx.WithSender(false)
	slot := txpool.TxSlot{}
	valueBuf := make([]byte, 16*4096)
	from := dbutils.EncodeBlockNumber(fromBlock)
	if err := kv.BigChunks(db, kv.BlockBody, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= fromBlock+uint64(blocksAmount) {
			return false, nil
		}

		var body types.BodyForStorage
		if e := rlp.DecodeBytes(v, &body); e != nil {
			return false, e
		}
		if body.TxAmount == 0 {
			return true, nil
		}

		binary.BigEndian.PutUint64(numBuf, body.BaseTxId)
		if err := tx.ForAmount(kv.EthTx, numBuf[:8], body.TxAmount, func(tk, tv []byte) error {
			if _, err := parseCtx.ParseTransaction(tv, 0, &slot, nil); err != nil {
				return err
			}
			valueBuf = valueBuf[:0]
			valueBuf = append(append(valueBuf, slot.IdHash[:1]...), tv...)
			if err := f.Append(valueBuf); err != nil {
				return err
			}
			i++

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
		return err
	}

	return nil
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
			log.Warn("header missed", "block_num", blockNum, "hash", fmt.Sprintf("v"))
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
			log.Warn("header missed", "block_num", blockNum, "hash", fmt.Sprintf("v"))
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

	w.Flush()
	s, _ := f.Stat()
	fmt.Printf("%dKb\n", s.Size()/1024)
	return nil
}

func TransactionsIdx(chainID uint256.Int, dir, name string) error {
	d, err := compress.NewDecompressor(path.Join(dir, name+".seg"))
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
		IndexFile:  path.Join(dir, name+".idx"),
	})
	if err != nil {
		return err
	}

RETRY:

	g := d.MakeGetter()
	wc := 0
	var pos uint64
	parseCtx := txpool.NewTxParseContext(chainID)
	parseCtx.WithSender(false)
	slot := txpool.TxSlot{}
	var sender [20]byte
	word := make([]byte, 0, 4*1024)

	for g.HasNext() {
		word, pos = g.Next(word[:0])
		if _, err := parseCtx.ParseTransaction(word[1:], 0, &slot, sender[:]); err != nil {
			return err
		}
		if err := rs.AddKey(slot.IdHash[:], pos); err != nil {
			return err
		}
		wc++
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

func BodiesIdx(dir, name string) error {
	d, err := compress.NewDecompressor(path.Join(dir, name+".seg"))
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
		IndexFile:  path.Join(dir, name+".idx"),
	})
	if err != nil {
		return err
	}

RETRY:

	g := d.MakeGetter()
	wc := 0
	var pos uint64
	word := make([]byte, 0, 4096)
	num := make([]byte, 8)
	for g.HasNext() {
		word, pos = g.Next(word[:0])
		binary.BigEndian.PutUint64(num, uint64(wc))
		if err := rs.AddKey(num, pos); err != nil {
			return err
		}
		wc++
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
