package commands

import (
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/node/debug"
)

func init() {
	withChain(debugHistoryCmd)
	withDataDir(debugHistoryCmd)
	withConfig(debugHistoryCmd)
	withBlock(debugHistoryCmd)
	commitmentCmd.AddCommand(debugHistoryCmd)
}

var debugHistoryCmd = &cobra.Command{
	Use:   "debug_history",
	Short: "Investigate phantom history keys at a specific block",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		if block == 0 {
			logger.Error("--block is required")
			return
		}

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		tx, err := chainDb.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			logger.Error("Failed to begin temporal tx", "error", err)
			return
		}
		defer tx.Rollback()

		br, _ := blocksIO(chainDb, logger)
		txNumsReader := br.TxnumReader()

		// Step 1: resolve txNum ranges for target block and neighbors
		prevMin, err := txNumsReader.Min(ctx, tx, block-1)
		if err != nil {
			logger.Error("txNumsReader.Min(block-1)", "error", err)
			return
		}
		prevMax, err := txNumsReader.Max(ctx, tx, block-1)
		if err != nil {
			logger.Error("txNumsReader.Max(block-1)", "error", err)
			return
		}
		blockMin, err := txNumsReader.Min(ctx, tx, block)
		if err != nil {
			logger.Error("txNumsReader.Min(block)", "error", err)
			return
		}
		blockMax, err := txNumsReader.Max(ctx, tx, block)
		if err != nil {
			logger.Error("txNumsReader.Max(block)", "error", err)
			return
		}
		nextMin, err := txNumsReader.Min(ctx, tx, block+1)
		if err != nil {
			logger.Error("txNumsReader.Min(block+1)", "error", err)
			return
		}
		nextMax, err := txNumsReader.Max(ctx, tx, block+1)
		if err != nil {
			logger.Error("txNumsReader.Max(block+1)", "error", err)
			return
		}

		fmt.Printf("block=%d  txNums=[%d, %d]\n", block, blockMin, blockMax)
		fmt.Printf("prevBlock=%d  txNums=[%d, %d]\n", block-1, prevMin, prevMax)
		fmt.Printf("nextBlock=%d  txNums=[%d, %d]\n", block+1, nextMin, nextMax)
		fmt.Println()

		// Step 2: collect all history keys attributed to the target block
		// Use a 3-block window so we can detect misattribution to/from neighbors
		windowFrom := block - 1
		windowEnd := block + 1
		numBlocks := int(windowEnd - windowFrom + 1)
		maxTxNums := make([]uint64, numBlocks)
		for i := range numBlocks {
			maxTxNums[i], err = txNumsReader.Max(ctx, tx, windowFrom+uint64(i))
			if err != nil {
				logger.Error("txNumsReader.Max", "block", windowFrom+uint64(i), "error", err)
				return
			}
		}
		windowMinTxNum, err := txNumsReader.Min(ctx, tx, windowFrom)
		if err != nil {
			logger.Error("txNumsReader.Min(windowFrom)", "error", err)
			return
		}
		windowMaxTxNum := maxTxNums[numBlocks-1]

		type historyEntry struct {
			key      []byte
			txNum    uint64
			blockNum uint64
			domain   kv.Domain
			domLabel string
		}

		var entries []historyEntry

		for _, dom := range []struct {
			d     kv.Domain
			label string
		}{
			{kv.AccountsDomain, "AccountsDomain"},
			{kv.StorageDomain, "StorageDomain"},
		} {
			it, err := tx.Debug().HistoryKeyTxNumRange(dom.d, int(windowMinTxNum), int(windowMaxTxNum+1), order.Asc, -1)
			if err != nil {
				logger.Error("HistoryKeyTxNumRange", "domain", dom.label, "error", err)
				return
			}
			for it.HasNext() {
				k, txNum, err := it.Next()
				if err != nil {
					it.Close()
					logger.Error("iterator.Next", "domain", dom.label, "error", err)
					return
				}
				blockIdx := sort.Search(numBlocks, func(i int) bool { return maxTxNums[i] >= txNum })
				if blockIdx >= numBlocks {
					continue
				}
				atBlock := windowFrom + uint64(blockIdx)
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				entries = append(entries, historyEntry{
					key:      keyCopy,
					txNum:    txNum,
					blockNum: atBlock,
					domain:   dom.d,
					domLabel: dom.label,
				})
			}
			it.Close()
		}

		// Filter to only keys attributed to the target block
		var targetEntries []historyEntry
		for _, e := range entries {
			if e.blockNum == block {
				targetEntries = append(targetEntries, e)
			}
		}

		fmt.Printf("Total history entries in window [%d, %d]: %d\n", windowFrom, windowEnd, len(entries))
		fmt.Printf("Entries attributed to block %d: %d\n", block, len(targetEntries))
		fmt.Println()

		// Step 3 & 4: for each key, query values before and after
		fmt.Println("---")
		for _, e := range targetEntries {
			keyHex := hex.EncodeToString(e.key)
			keyLabel := "account"
			if len(e.key) > 20 {
				keyLabel = "storage"
				keyHex = hex.EncodeToString(e.key[:20]) + ":" + hex.EncodeToString(e.key[20:])
			}

			fmt.Printf("KEY: %s:%s  txNum=%d  domain=%s  keyLen=%d (%s)\n",
				keyLabel, keyHex, e.txNum, e.domLabel, len(e.key), keyLabel)

			// Value at end of previous block
			valPrevEnd, okPrev, err := tx.GetAsOf(e.domain, e.key, blockMin)
			if err != nil {
				fmt.Printf("  ERROR GetAsOf(prevBlockEnd): %v\n", err)
				continue
			}

			// Value at start of block (same as end of previous block)
			valBlockStart, okStart, err := tx.GetAsOf(e.domain, e.key, blockMin)
			if err != nil {
				fmt.Printf("  ERROR GetAsOf(blockStart): %v\n", err)
				continue
			}

			// Value at end of block
			valBlockEnd, okEnd, err := tx.GetAsOf(e.domain, e.key, blockMax+1)
			if err != nil {
				fmt.Printf("  ERROR GetAsOf(blockEnd): %v\n", err)
				continue
			}

			// Value at end of previous block (one before)
			valPrevPrev, okPrevPrev, err := tx.GetAsOf(e.domain, e.key, prevMax+1)
			if err != nil {
				fmt.Printf("  ERROR GetAsOf(prevBlockEnd-1): %v\n", err)
				continue
			}

			fmt.Printf("  val@prevBlockEnd(txNum=%d):  ok=%t hex=%s\n", blockMin, okPrev, hex.EncodeToString(valPrevEnd))
			fmt.Printf("  val@blockStart(txNum=%d):    ok=%t hex=%s\n", blockMin, okStart, hex.EncodeToString(valBlockStart))
			fmt.Printf("  val@blockEnd(txNum=%d):      ok=%t hex=%s\n", blockMax+1, okEnd, hex.EncodeToString(valBlockEnd))
			fmt.Printf("  val@prevPrevEnd(txNum=%d):   ok=%t hex=%s\n", prevMax+1, okPrevPrev, hex.EncodeToString(valPrevPrev))

			// Determine verdict
			verdict := "real (value changed)"
			if !okEnd && !okStart {
				verdict = "phantom (key never existed)"
			} else if hex.EncodeToString(valBlockStart) == hex.EncodeToString(valBlockEnd) {
				verdict = "phantom (value unchanged start→end)"
			}

			// Check for leak from previous block
			if okPrevPrev && hex.EncodeToString(valPrevPrev) != hex.EncodeToString(valBlockStart) {
				verdict += " + prev block leak (prevPrevEnd != blockStart)"
			}

			// Step 4: for storage keys, check if the account was deleted/created
			if e.domain == kv.StorageDomain && len(e.key) >= 20 {
				addr := e.key[:20]
				acctStart, acctOkStart, err := tx.GetAsOf(kv.AccountsDomain, addr, blockMin)
				if err != nil {
					fmt.Printf("  ERROR GetAsOf(account,blockStart): %v\n", err)
				}
				acctEnd, acctOkEnd, err := tx.GetAsOf(kv.AccountsDomain, addr, blockMax+1)
				if err != nil {
					fmt.Printf("  ERROR GetAsOf(account,blockEnd): %v\n", err)
				}

				acctStartEmpty := !acctOkStart || len(acctStart) == 0
				acctEndEmpty := !acctOkEnd || len(acctEnd) == 0

				if acctStartEmpty != acctEndEmpty {
					if acctStartEmpty {
						verdict += " + account CREATED this block"
					} else {
						verdict += " + account DELETED this block (storage cleared)"
					}
				}

				fmt.Printf("  account %s: start=%t(%d bytes) end=%t(%d bytes)\n",
					hex.EncodeToString(addr), acctOkStart, len(acctStart), acctOkEnd, len(acctEnd))
			}

			fmt.Printf("  VERDICT: %s\n", verdict)

			// For phantom keys, check HistorySeek and TraceKey
			if !okEnd && !okStart {
				hsVal, hsOk, hsErr := tx.HistorySeek(e.domain, e.key, e.txNum)
				if hsErr != nil {
					fmt.Printf("  HistorySeek(txNum=%d) ERROR: %v\n", e.txNum, hsErr)
				} else {
					fmt.Printf("  HistorySeek(txNum=%d): ok=%t hex=%s\n", e.txNum, hsOk, hex.EncodeToString(hsVal))
				}

				// TraceKey: full history timeline for this key
				traceIt, trErr := tx.Debug().TraceKey(e.domain, e.key, 0, ^uint64(0))
				if trErr != nil {
					fmt.Printf("  TraceKey ERROR: %v\n", trErr)
				} else {
					fmt.Printf("  --- TraceKey (full history) ---\n")
					count := 0
					for traceIt.HasNext() {
						txn, val, err := traceIt.Next()
						if err != nil {
							fmt.Printf("    TraceKey.Next ERROR: %v\n", err)
							break
						}
						fmt.Printf("    txNum=%d val=%s\n", txn, hex.EncodeToString(val))
						count++
						if count >= 50 {
							fmt.Printf("    ... (truncated at 50 entries)\n")
							break
						}
					}
					traceIt.Close()
					if count == 0 {
						fmt.Printf("    (no entries)\n")
					}
					fmt.Println("  --- end TraceKey ---")
				}

				fmt.Printf("  --- per-txNum scan [%d..%d] ---\n", blockMin, blockMax)
				for txn := blockMin; txn <= blockMax; txn++ {
					v, ok, err := tx.GetAsOf(e.domain, e.key, txn+1)
					if err != nil {
						fmt.Printf("    txNum=%d ERROR: %v\n", txn, err)
						continue
					}
					if ok && len(v) > 0 {
						fmt.Printf("    txNum=%d VALUE FOUND: %s\n", txn, hex.EncodeToString(v))
					}
				}
				// Also scan account for storage phantoms
				if e.domain == kv.StorageDomain && len(e.key) >= 20 {
					addr := e.key[:20]
					fmt.Printf("  --- account %s per-txNum scan ---\n", hex.EncodeToString(addr))
					var prevAcct string
					for txn := blockMin; txn <= blockMax; txn++ {
						v, ok, err := tx.GetAsOf(kv.AccountsDomain, addr, txn+1)
						if err != nil {
							fmt.Printf("    txNum=%d ERROR: %v\n", txn, err)
							continue
						}
						cur := ""
						if ok && len(v) > 0 {
							cur = hex.EncodeToString(v)
						}
						if cur != prevAcct {
							if cur == "" {
								fmt.Printf("    txNum=%d account GONE\n", txn)
							} else {
								fmt.Printf("    txNum=%d account APPEARED: %s\n", txn, cur)
							}
							prevAcct = cur
						}
					}
				}
				fmt.Println("  --- end scan ---")
			}

			fmt.Println()
		}

		// Summary: one line per unique key, same format as rebuild log for easy diff
		seen := make(map[string]bool)
		fmt.Println("=== DIFF SUMMARY (domain key dbVal) ===")
		for _, e := range targetEntries {
			keyHex := hex.EncodeToString(e.key)
			domLabel := "accounts"
			if e.domain == kv.StorageDomain {
				domLabel = "storage"
			}
			combo := domLabel + " " + keyHex
			if seen[combo] {
				continue
			}
			seen[combo] = true

			val, _, err := tx.GetAsOf(e.domain, e.key, blockMax+1)
			if err != nil {
				fmt.Printf("%s key=%s dbVal=ERROR:%v\n", domLabel, keyHex, err)
				continue
			}
			fmt.Printf("%s key=%s dbVal=%s\n", domLabel, keyHex, hex.EncodeToString(val))
		}
	},
}
