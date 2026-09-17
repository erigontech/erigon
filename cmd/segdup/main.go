// Command segdup reproduces the transactions-index key derivation over a pair of block snapshot
// segments and reports duplicated keys.
//
// The index builder only ever reports a collision as recsplit's salted 64-bit hash, which changes on
// every retry and so never names the key that cannot be indexed. This walks the same two segments in
// the same order with the same key rules as snaptype2's transactions index builder and prints the
// offending key together with every record that carries it.
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// firstTxByteAndlengthOfAddress is the dump's per-record prefix: first tx byte, then the sender.
const firstTxByteAndlengthOfAddress = 21

func main() {
	if len(os.Args) < 4 {
		fmt.Println("usage: segdup <transactions.seg> <bodies.seg> <firstBlockNum>")
		os.Exit(2)
	}
	txPath, bodiesPath := os.Args[1], os.Args[2]
	firstBlockNum, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Println("firstBlockNum:", err)
		os.Exit(2)
	}

	bodiesSegment, err := seg.NewDecompressor(bodiesPath)
	if err != nil {
		fmt.Println("open bodies:", err)
		os.Exit(1)
	}
	defer bodiesSegment.Close()

	d, err := seg.NewDecompressor(txPath)
	if err != nil {
		fmt.Println("open transactions:", err)
		os.Exit(1)
	}
	defer d.Close()

	// Read every body up front so each record can be placed by its txn-id RANGE. The index builder
	// walks blocks lazily with a comparison it carries a "TODO review this code" against, so its own
	// attribution cannot be trusted to say which block a duplicated record belongs to.
	type blockSpan struct {
		num   uint64
		first uint64 // first txn id owned by the block, system slot included
		last  uint64 // last txn id owned by the block
	}
	var spans []blockSpan
	{
		bg := bodiesSegment.MakeGetter()
		b := &types.BodyForStorage{}
		buf := make([]byte, 0, 4096)
		num := firstBlockNum
		for bg.HasNext() {
			buf, _ = bg.Next(buf[:0])
			if err := rlp.DecodeBytes(buf, b); err != nil {
				fmt.Println("decode body:", err)
				os.Exit(1)
			}
			spans = append(spans, blockSpan{
				num:   num,
				first: b.BaseTxnID.U64(),
				last:  b.BaseTxnID.LastSystemTx(b.TxCount),
			})
			num++
		}
	}
	// SEGDUP_SPANS=1 dumps every block's id range, so it can be joined against the txNum the executor
	// actually ran the block at. Both counters accumulate TxCount per block from zero, so they must agree.
	if os.Getenv("SEGDUP_SPANS") != "" {
		for _, s := range spans {
			fmt.Printf("SPAN %d %d %d\n", s.num, s.first, s.last)
		}
	}

	blockOf := func(txnID uint64) uint64 {
		lo, hi := 0, len(spans)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			switch {
			case txnID < spans[mid].first:
				hi = mid - 1
			case txnID > spans[mid].last:
				lo = mid + 1
			default:
				return spans[mid].num
			}
		}
		return ^uint64(0) // owned by no block
	}

	g, bodyGetter := d.MakeGetter(), bodiesSegment.MakeGetter()
	body := &types.BodyForStorage{}
	bodyBuf, word := make([]byte, 0, 4096), make([]byte, 0, 4096)

	bodyBuf, _ = bodyGetter.Next(bodyBuf[:0])
	if err := rlp.DecodeBytes(bodyBuf, body); err != nil {
		fmt.Println("decode first body:", err)
		os.Exit(1)
	}
	baseTxnID := body.BaseTxnID

	type where struct {
		ti       uint64
		blockNum uint64 // the builder's lazy attribution
		exact    uint64 // attribution by txn-id range
		wordLen  int
		system   bool
	}
	seen := make(map[common.Hash][]where, d.Count())

	blockNum := firstBlockNum
	var ti uint64
	var realCount, systemCount, decodeErrs int

	for g.HasNext() {
		word, _ = g.Next(word[:0])

		for body.BaseTxnID.LastSystemTx(body.TxCount) < baseTxnID.U64()+ti {
			if !bodyGetter.HasNext() {
				fmt.Printf("ran out of bodies at ti=%d blockNum=%d\n", ti, blockNum)
				break
			}
			bodyBuf, _ = bodyGetter.Next(bodyBuf[:0])
			if err := rlp.DecodeBytes(bodyBuf, body); err != nil {
				fmt.Println("decode body:", err)
				os.Exit(1)
			}
			blockNum++
		}

		var txnHash common.Hash
		isSystemTx := len(word) == 0
		if isSystemTx {
			binary.BigEndian.PutUint64(txnHash[:], baseTxnID.U64()+ti)
			systemCount++
		} else {
			txn, derr := types.DecodeTransaction(word[firstTxByteAndlengthOfAddress:])
			if derr != nil {
				decodeErrs++
				ti++
				continue
			}
			txnHash = txn.Hash()
			realCount++
		}

		seen[txnHash] = append(seen[txnHash], where{
			ti: ti, blockNum: blockNum, exact: blockOf(baseTxnID.U64() + ti),
			wordLen: len(word), system: isSystemTx,
		})
		ti++
	}

	fmt.Printf("segment records=%d (expected %d)  real=%d  system=%d  decodeErrors=%d\n",
		ti, d.Count(), realCount, systemCount, decodeErrs)

	// A system slot must be empty: real transactions only ever occupy At(i) = Base+1+i. A slot holding a
	// real record is a row left behind by a DIFFERENT id allocation for the same block. Count them, so the
	// duplicate keys can be told apart from the wider stale-row condition they are only the visible tip of.
	occupiedFirst, occupiedLast, blocksAffected := 0, 0, 0
	for _, s := range spans {
		firstBad, lastBad := false, false
		for _, ws := range seen {
			for _, w := range ws {
				if w.system || w.exact != s.num {
					continue
				}
				switch baseTxnID.U64() + w.ti {
				case s.first:
					firstBad = true
				case s.last:
					lastBad = true
				}
			}
		}
		if firstBad {
			occupiedFirst++
		}
		if lastBad {
			occupiedLast++
		}
		if firstBad || lastBad {
			blocksAffected++
		}
	}
	fmt.Printf("system slots holding a REAL transaction: first=%d last=%d  blocks affected=%d of %d\n",
		occupiedFirst, occupiedLast, blocksAffected, len(spans))

	// Every block's range must start exactly one past its predecessor's. A boundary that does not is the
	// id sequence having moved, which is what lets a superseded allocation overlap the live one.
	gaps := 0
	for i := 1; i < len(spans); i++ {
		if spans[i].first != spans[i-1].last+1 {
			gaps++
			if gaps <= 10 {
				fmt.Printf("NON-CONTIGUOUS at block %d: prev %d..%d then %d..%d (delta %+d)\n",
					spans[i].num, spans[i-1].first, spans[i-1].last, spans[i].first, spans[i].last,
					int64(spans[i].first)-int64(spans[i-1].last+1))
			}
		}
	}
	fmt.Printf("non-contiguous span boundaries: %d of %d\n", gaps, len(spans)-1)

	// And the immediate neighbourhood of each block that stranded a row.
	for h, ws := range seen {
		if len(ws) < 2 {
			continue
		}
		_ = h
		for i, s := range spans {
			if s.num != ws[0].exact {
				continue
			}
			for j := max(0, i-1); j <= min(len(spans)-1, i+1); j++ {
				mark := ""
				if j == i {
					mark = "  <-- stranded"
				}
				fmt.Printf("neighbourhood block %-8d span %d..%d  txs=%d%s\n",
					spans[j].num, spans[j].first, spans[j].last, spans[j].last-spans[j].first+1-2, mark)
			}
			fmt.Println()
		}
	}
	fmt.Printf("baseTxnID=%d  firstBlockNum=%d  distinctKeys=%d\n", baseTxnID.U64(), firstBlockNum, len(seen))

	dups := 0
	for h, ws := range seen {
		if len(ws) < 2 {
			continue
		}
		dups++
		if dups <= 25 {
			fmt.Printf("DUPLICATE KEY %x  occurrences=%d  system=%v\n", h, len(ws), ws[0].system)
			for _, w := range ws {
				// A real transaction may only occupy At(i) = Base+1+i. Landing on a block's system
				// slot means the row is left over from a different, overlapping id allocation.
				slot := "real-range"
				for _, s := range spans {
					if s.num != w.exact {
						continue
					}
					switch baseTxnID.U64() + w.ti {
					case s.first:
						slot = "FIRST-SYSTEM-SLOT"
					case s.last:
						slot = "LAST-SYSTEM-SLOT"
					}
				}
				fmt.Printf("    ti=%-8d txnID=%-10d lazyBlock=%-8d exactBlock=%-8d wordLen=%-5d %s\n",
					w.ti, baseTxnID.U64()+w.ti, w.blockNum, w.exact, w.wordLen, slot)
			}
			// The sealed body's own width, to compare against what the canonical view serves.
			for _, s := range spans {
				if s.num == ws[0].exact {
					fmt.Printf("    block %d span: txnIDs %d..%d  => %d real txs in the SEALED body\n",
						s.num, s.first, s.last, s.last-s.first+1-2)
				}
			}
		}
	}
	fmt.Printf("DUPLICATE KEYS TOTAL: %d\n", dups)
}
