// Copyright 2024 The Erigon Authors
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

package state

import (
	"context"
	"encoding/binary"
	"path/filepath"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// B-tree accessor for a page-compressed history .v file.
//
// A paged .v stores its keys inline (each page holds sorted (txNum||key, value)
// pairs) in key-major physical order. We build a sparse index over it: one
// anchor per page, `page's last key -> page's seg-word offset`, held in a small
// anchor seg file with a stock .bt on top. Because pages partition the key space
// in order, Seek(probe) on the anchors lands directly on the page whose range
// contains the probe, and GetFromPage pulls the value out — identical to how the
// recsplit .vi read works, minus the MPHF.
//
// The stored key is txNum||key but physical order is key-major, so the bt is
// keyed on the re-packed key||txNum form.

func vibtRepackAnchorKey(storedKey, buf []byte) []byte {
	buf = append(buf[:0], storedKey[8:]...)
	return append(buf, storedKey[:8]...)
}

func vibtProbeKey(key []byte, txNum uint64, buf []byte) []byte {
	buf = append(buf[:0], key...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], txNum)
	return append(buf, ts[:]...)
}

func vibtStoredKey(key []byte, txNum uint64, buf []byte) []byte {
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], txNum)
	buf = append(buf[:0], ts[:]...)
	return append(buf, key...)
}

// buildVIBt writes the anchor seg file and its .bt over a paged history .v.
func buildVIBt(ctx context.Context, vDecomp *seg.Decompressor, pageCompressed bool, anchorPath, btPath, tmpDir string, salt uint32, ps *background.ProgressSet, logger log.Logger, noFsync bool) error {
	comp, err := seg.NewCompressor(ctx, "vibt", anchorPath, tmpDir, seg.DefaultCfg, log.LvlTrace, logger)
	if err != nil {
		return err
	}
	defer comp.Close()
	if noFsync {
		comp.DisableFsync()
	}

	// CompressNone Writer stores words via AddUncompressedWord, which pairs with
	// the NextUncompressed read the anchor .bt uses. Writing via comp.AddWord
	// directly would apply pattern compression and misparse on read.
	w := seg.NewWriter(comp, seg.CompressNone)

	_, anchorName := filepath.Split(anchorPath)
	p := ps.AddNew(anchorName, uint64(vDecomp.Count()))
	defer ps.Delete(p)

	g := seg.NewReader(vDecomp.MakeGetter(), seg.CompressNone)
	g.Reset(0)
	var pageOff uint64
	var anchorKey, lastKey []byte
	var offBuf [8]byte
	for g.HasNext() {
		word, nextOff := g.Next(nil)
		page := seg.FromBytes(word, pageCompressed)
		lastKey = lastKey[:0]
		hasEntry := false
		for page.HasNext() {
			k, _ := page.Next()
			lastKey = append(lastKey[:0], k...)
			hasEntry = true
		}
		if hasEntry {
			anchorKey = vibtRepackAnchorKey(lastKey, anchorKey)
			if _, err := w.Write(anchorKey); err != nil {
				return err
			}
			binary.BigEndian.PutUint64(offBuf[:], pageOff)
			if _, err := w.Write(offBuf[:]); err != nil {
				return err
			}
		}
		pageOff = nextOff
		p.Processed.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := comp.Compress(); err != nil {
		return err
	}
	comp.Close()

	ad, err := seg.NewDecompressor(anchorPath)
	if err != nil {
		return err
	}
	defer ad.Close()
	return btindex.BuildBtreeIndexWithDecompressor(btPath, "", seg.NewReader(ad.MakeGetter(), seg.CompressNone), ps, tmpDir, salt, logger, noFsync, statecfg.AccessorBTree)
}

// vibtSeek resolves (key, txNum) to its value via the anchor .bt and GetFromPage.
func vibtSeek(bt *btindex.BtIndex, anchorGetter, vGetter *seg.Reader, key []byte, txNum uint64, pageCompressed bool, probeBuf, storedBuf, pageBuf []byte) (val, probeBufOut, storedBufOut, pageBufOut []byte, found bool, err error) {
	probeBuf = vibtProbeKey(key, txNum, probeBuf)
	cur, err := bt.Seek(anchorGetter, probeBuf)
	if err != nil {
		return nil, probeBuf, storedBuf, pageBuf, false, err
	}
	if cur == nil {
		return nil, probeBuf, storedBuf, pageBuf, false, nil
	}
	pageOff := binary.BigEndian.Uint64(cur.Value())
	vGetter.Reset(pageOff)
	if !vGetter.HasNext() {
		return nil, probeBuf, storedBuf, pageBuf, false, nil
	}
	word, _ := vGetter.Next(nil)
	storedBuf = vibtStoredKey(key, txNum, storedBuf)
	val, pageBuf = seg.GetFromPage(storedBuf, word, pageBuf, pageCompressed)
	return val, probeBuf, storedBuf, pageBuf, val != nil, nil
}
