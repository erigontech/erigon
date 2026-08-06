// Copyright 2026 The Erigon Authors
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

package commands

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/node/debug"
)

// verify-boundary-file iterates an emitted mode-C boundary-step .kv file
// and cross-checks each (key, v4_value) pair against tx.HistorySeek at
// lastTxN+1. A mismatch localises a wrong-value bug in the emit to a
// specific key.
//
// Only keys with a post-lastTxN history entry are checked via
// HistorySeek — those return the value BEFORE the first post-lastTxN
// change, which is the value AT lastTxN. Keys without such an entry
// leave HistorySeek "not-found" and can't be cross-checked without
// walking to older files (skipped in this pass — the emit-wrongness
// investigation cares about mismatch cases, not coverage cases).
var (
	vbfPath      string
	vbfDomainStr string
	vbfLastTxN   uint64
	vbfLimit     uint64
	vbfShowMatch bool
)

var cmdVerifyBoundaryFile = &cobra.Command{
	Use:   "verify-boundary-file",
	Short: "Cross-check every (key, value) in an emitted v4 .kv file against tx.HistorySeek at lastTxN+1. Diagnostic for mode-C emit-wrongness.",
	Long: `Given --path to a v4-emitted domain .kv file and its --lasttxn,
open the datadir read-only and iterate every (key, value) pair. For each
pair, call tx.HistorySeek(domain, key, lastTxN+1) which returns the
value BEFORE the first change at txN >= lastTxN+1 — i.e. the correct
as-of-lastTxN value — read directly from history .v/.ef files without
falling through to the v4 file itself. Print any mismatch.

Reports counts of: total pairs, HistorySeek-found matches, HistorySeek-
found mismatches, HistorySeek-not-found-can't-verify.

Zero-length values in the v4 file are tombstones and are checked the
same way — a mismatch where HistorySeek returns a non-empty value while
the v4 file stored empty is exactly the "falsely tombstoned live slot"
pattern that produces SET-vs-RESET gas mismatches on forward re-exec.

`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx := cmd.Context()

		domain, ok := parseDomainName(vbfDomainStr)
		if !ok {
			logger.Error("invalid --domain; want one of accounts|storage|code|commitment|receipt", "got", vbfDomainStr)
			return
		}
		if vbfLastTxN == 0 {
			logger.Error("--lasttxn is required (typically the file's endTxN - 1)")
			return
		}
		if vbfPath == "" {
			logger.Error("--path is required")
			return
		}

		dirs := datadir.New(datadirCli)
		db, err := openDB(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("open DB", "err", err)
			return
		}
		defer db.Close()

		tx, err := db.BeginTemporalRo(ctx)
		if err != nil {
			logger.Error("begin temporal ro", "err", err)
			return
		}
		defer tx.Rollback()

		dec, err := seg.NewDecompressor(vbfPath)
		if err != nil {
			logger.Error("open v4 file", "path", vbfPath, "err", err)
			return
		}
		defer dec.Close()
		g := dec.MakeGetter()

		var (
			total         uint64
			matched       uint64
			mismatched    uint64
			notFound      uint64
			tombstonesV4  uint64
			mismatchPrint uint64
		)

		fmt.Printf("verify-boundary-file: domain=%s path=%s lastTxN=%d ts=%d (=lastTxN+1)\n",
			domain, vbfPath, vbfLastTxN, vbfLastTxN+1)
		fmt.Println("Mismatches (v4 file stored X, history says Y at lastTxN+1):")

		var kbuf, vbuf []byte
		for g.HasNext() {
			kbuf, _ = g.Next(kbuf[:0])
			if !g.HasNext() {
				logger.Error("odd word count in v4 file", "at_idx", total)
				return
			}
			vbuf, _ = g.Next(vbuf[:0])
			total++
			if len(vbuf) == 0 {
				tombstonesV4++
			}

			hv, hok, herr := tx.HistorySeek(domain, kbuf, vbfLastTxN+1)
			if herr != nil {
				fmt.Printf("  key=%x HistorySeek err=%v\n", kbuf, herr)
				continue
			}
			if !hok {
				notFound++
				continue
			}
			if bytes.Equal(hv, vbuf) {
				matched++
				if vbfShowMatch && mismatchPrint < 5 {
					fmt.Printf("  MATCH key=%x len=%d\n", kbuf, len(vbuf))
					mismatchPrint++
				}
				continue
			}
			mismatched++
			if mismatched <= vbfLimit || vbfLimit == 0 {
				fmt.Printf("  MISMATCH key=%x  v4=(len=%d)0x%x  history=(len=%d)0x%x\n",
					kbuf, len(vbuf), vbuf, len(hv), hv)
			}
		}

		fmt.Println("--------------------------------------------------------------------")
		fmt.Printf("SUMMARY: total=%d matched=%d mismatched=%d notFound(=history-silent-past-lastTxN)=%d tombstonesInV4=%d\n",
			total, matched, mismatched, notFound, tombstonesV4)
		if mismatched > 0 {
			fmt.Printf("BUG SIGNATURE: %d keys' v4-stored value differs from history's as-of-lastTxN value.\n", mismatched)
		} else if matched == 0 {
			fmt.Println("INCONCLUSIVE: no keys had post-lastTxN history entries to cross-check against.")
		} else {
			fmt.Println("OK: every key with a post-lastTxN history entry matched the v4-stored value.")
		}
	},
}

func init() {
	withDataDir(cmdVerifyBoundaryFile)
	withChain(cmdVerifyBoundaryFile)
	cmdVerifyBoundaryFile.Flags().StringVar(&vbfPath, "path", "", "path to the v4 .kv file to verify")
	cmdVerifyBoundaryFile.Flags().StringVar(&vbfDomainStr, "domain", "storage", "accounts | storage | code | commitment | receipt")
	cmdVerifyBoundaryFile.Flags().Uint64Var(&vbfLastTxN, "lasttxn", 0, "lastTxN the emit was for (file's endTxN - 1)")
	cmdVerifyBoundaryFile.Flags().Uint64Var(&vbfLimit, "print-limit", 20, "max mismatches to print (0=all)")
	cmdVerifyBoundaryFile.Flags().BoolVar(&vbfShowMatch, "show-first-matches", false, "also print first few matched keys for sanity")
	must(cmdVerifyBoundaryFile.MarkFlagRequired("path"))
	must(cmdVerifyBoundaryFile.MarkFlagRequired("lasttxn"))

	rootCmd.AddCommand(cmdVerifyBoundaryFile)
}
