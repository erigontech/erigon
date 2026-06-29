package state

import (
	"bytes"
	"encoding/binary"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

// Raw, non-merged dumps of a single key across history/domain files and the DB.
// "Non-merged" means overlapping files are all reported as-is, so the caller can
// see exactly which file (and the DB) holds which value for the key.

type DebugKeyHistoryEntry struct {
	TxNum uint64
	Val   []byte
}

type DebugKeyHistoryFile struct {
	FileName   string
	StartTxNum uint64
	EndTxNum   uint64
	Entries    []DebugKeyHistoryEntry
}

type DebugKeyDomainFile struct {
	FileName   string
	StartTxNum uint64
	EndTxNum   uint64
	Val        []byte
	Found      bool
}

type DebugKeyDomainDBEntry struct {
	Step kv.Step
	Val  []byte
}

func (at *AggregatorRoTx) DebugDumpKeyHistory(domain kv.Domain, key []byte, roTx kv.Tx) (files []DebugKeyHistoryFile, db []DebugKeyHistoryEntry, err error) {
	ht := at.d[domain].ht
	files, err = ht.debugKeyHistoryFiles(key)
	if err != nil {
		return nil, nil, err
	}
	db, err = ht.debugKeyHistoryDB(key, roTx)
	if err != nil {
		return nil, nil, err
	}
	return files, db, nil
}

func (at *AggregatorRoTx) DebugDumpKeyDomain(domain kv.Domain, key []byte, roTx kv.Tx) (files []DebugKeyDomainFile, db []DebugKeyDomainDBEntry, err error) {
	dt := at.d[domain]
	files, err = dt.debugKeyDomainFiles(key)
	if err != nil {
		return nil, nil, err
	}
	db, err = dt.debugKeyDomainDB(key, roTx)
	if err != nil {
		return nil, nil, err
	}
	return files, db, nil
}

func (ht *HistoryRoTx) debugKeyHistoryFiles(key []byte) (res []DebugKeyHistoryFile, err error) {
	iit := ht.iit
	hi, lo := iit.hashKey(key)
	for i := 0; i < len(iit.files); i++ {
		item := iit.files[i].src
		if item == nil || item.decompressor == nil || item.index == nil {
			continue
		}
		offset, ok := iit.statelessIdxReader(i).TwoLayerLookupByHash(hi, lo)
		if !ok {
			continue
		}
		g := iit.statelessGetter(i)
		g.Reset(offset)
		k, _ := g.Next(nil)
		if !bytes.Equal(key, k) {
			continue
		}
		encodedSeq, _ := g.Next(nil)
		seq := multiencseq.ReadMultiEncSeq(iit.files[i].startTxNum, encodedSeq)

		f := DebugKeyHistoryFile{
			FileName:   item.decompressor.FileName(),
			StartTxNum: iit.files[i].startTxNum,
			EndTxNum:   iit.files[i].endTxNum,
		}
		it := seq.Iterator(0)
		for it.HasNext() {
			txNum, err := it.Next()
			if err != nil {
				return nil, err
			}
			v, _, err := ht.histValueAt(txNum, key)
			if err != nil {
				return nil, err
			}
			f.Entries = append(f.Entries, DebugKeyHistoryEntry{TxNum: txNum, Val: bytes.Clone(v)})
		}
		res = append(res, f)
	}
	return res, nil
}

func (ht *HistoryRoTx) histValueAt(histTxNum uint64, key []byte) ([]byte, bool, error) {
	historyItem, ok := ht.getFile(histTxNum)
	if !ok {
		return nil, false, nil
	}
	reader := ht.statelessIdxReader(historyItem.i)
	if reader.Empty() {
		return nil, false, nil
	}
	hKey := ht.encodeTs(histTxNum, key)
	offset, ok := reader.Lookup(hKey)
	if !ok {
		return nil, false, nil
	}
	g := ht.statelessGetter(historyItem.i)
	g.Reset(offset)
	v, _ := g.Next(nil)

	compressedPageValuesCount := historyItem.src.decompressor.CompressedPageValuesCount()
	if historyItem.src.decompressor.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
		compressedPageValuesCount = ht.h.HistoryValuesOnCompressedPage
	}
	if compressedPageValuesCount > 1 {
		v, ht.snappyReadBuffer = seg.GetFromPage(hKey, v, ht.snappyReadBuffer, true)
	}
	return v, true, nil
}

func (ht *HistoryRoTx) debugKeyHistoryDB(key []byte, roTx kv.Tx) (res []DebugKeyHistoryEntry, err error) {
	if ht.h.HistoryLargeValues {
		c, err := roTx.Cursor(ht.h.ValuesTable)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		k, v, err := c.Seek(key)
		for ; k != nil; k, v, err = c.Next() {
			if err != nil {
				return nil, err
			}
			if len(k) < 8 || !bytes.Equal(k[:len(k)-8], key) {
				break
			}
			txNum := binary.BigEndian.Uint64(k[len(k)-8:])
			res = append(res, DebugKeyHistoryEntry{TxNum: txNum, Val: bytes.Clone(v)})
		}
		return res, nil
	}

	c, err := roTx.CursorDupSort(ht.h.ValuesTable)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	k, v, err := c.SeekExact(key)
	for ; k != nil; k, v, err = c.NextDup() {
		if err != nil {
			return nil, err
		}
		if len(v) < 8 {
			continue
		}
		txNum := binary.BigEndian.Uint64(v[:8])
		res = append(res, DebugKeyHistoryEntry{TxNum: txNum, Val: bytes.Clone(v[8:])})
	}
	return res, nil
}

func (dt *DomainRoTx) debugKeyDomainFiles(key []byte) (res []DebugKeyDomainFile, err error) {
	hi, lo := dt.ht.iit.hashKey(key)
	for i := 0; i < len(dt.files); i++ {
		if dt.files[i].src == nil || dt.files[i].src.decompressor == nil {
			continue
		}
		v, ok, _, err := dt.getLatestFromFile(i, key, hi, lo)
		if err != nil {
			return nil, err
		}
		res = append(res, DebugKeyDomainFile{
			FileName:   dt.files[i].src.decompressor.FileName(),
			StartTxNum: dt.files[i].startTxNum,
			EndTxNum:   dt.files[i].endTxNum,
			Val:        bytes.Clone(v),
			Found:      ok,
		})
	}
	return res, nil
}

func (dt *DomainRoTx) debugKeyDomainDB(key []byte, roTx kv.Tx) (res []DebugKeyDomainDBEntry, err error) {
	if dt.d.LargeValues {
		c, err := roTx.Cursor(dt.d.ValuesTable)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		k, v, err := c.Seek(key)
		for ; k != nil; k, v, err = c.Next() {
			if err != nil {
				return nil, err
			}
			if len(k) < 8 || !bytes.Equal(k[:len(k)-8], key) {
				break
			}
			step := kv.Step(^binary.BigEndian.Uint64(k[len(k)-8:]))
			res = append(res, DebugKeyDomainDBEntry{Step: step, Val: bytes.Clone(v)})
		}
		return res, nil
	}

	c, err := roTx.CursorDupSort(dt.d.ValuesTable)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	k, v, err := c.SeekExact(key)
	for ; k != nil; k, v, err = c.NextDup() {
		if err != nil {
			return nil, err
		}
		if len(v) < 8 {
			continue
		}
		step := kv.Step(^binary.BigEndian.Uint64(v[:8]))
		res = append(res, DebugKeyDomainDBEntry{Step: step, Val: bytes.Clone(v[8:])})
	}
	return res, nil
}
