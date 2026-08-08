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
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/node/debug"
)

// history-walk-dump replays the v4-emit walker
// (Debug.HistoryKeyTxNumRange with adjacent-duplicate suppression)
// against a datadir for a specific (domain, fromTxN, toTxN] window
// and prints every yielded key. Used to diagnose the state-v4
// non-determinism class where addresses touched in-window on the
// real chain are absent from the emitted v4 .kv.
//
// When --key is set, prints ONLY yield events for that key +
// summary. Otherwise prints EVERY yielded key (verbose — pipe to
// less/grep).
var (
	hwdDomainStr string
	hwdFromTxN   uint64
	hwdToTxN     uint64
	hwdKeyHex    string
)

var cmdHistoryWalkDump = &cobra.Command{
	Use:   "history-walk-dump",
	Short: "Replay the v4-emit walker on a datadir; print yielded keys for a (fromTxN, toTxN] window.",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx := cmd.Context()

		domain, ok := parseDomainName(hwdDomainStr)
		if !ok {
			logger.Error("invalid --domain", "got", hwdDomainStr)
			return
		}
		if hwdToTxN <= hwdFromTxN {
			logger.Error("--to-txN must be > --from-txN", "from", hwdFromTxN, "to", hwdToTxN)
			return
		}
		var focusKey []byte
		if hwdKeyHex != "" {
			var err error
			focusKey, err = hex.DecodeString(strings.TrimPrefix(hwdKeyHex, "0x"))
			if err != nil {
				logger.Error("invalid --key hex", "err", err)
				return
			}
		}

		dirs := datadir.New(datadirCli)
		db, err := openDB(cmd.Context(), dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
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

		fmt.Printf("history-walk-dump: domain=%s range=(%d, %d] focusKey=0x%x\n",
			domain, hwdFromTxN, hwdToTxN, focusKey)
		fmt.Println(strings.Repeat("-", 78))

		it, err := tx.Debug().HistoryKeyTxNumRange(domain, int(hwdFromTxN), int(hwdToTxN+1), order.Asc, -1)
		if err != nil {
			logger.Error("HistoryKeyTxNumRange", "err", err)
			return
		}
		defer it.Close()

		var (
			prevKey      []byte
			raw          uint64
			yielded      uint64
			focusYielded uint64
			focusHits    uint64
			focusLastTxN uint64
		)
		for it.HasNext() {
			k, txN, ierr := it.Next()
			if ierr != nil {
				logger.Error("HistoryKeyTxNumRange.Next", "err", ierr)
				return
			}
			raw++
			isFocus := focusKey != nil && bytes.Equal(k, focusKey)
			if isFocus {
				focusHits++
				focusLastTxN = txN
			}
			isDup := prevKey != nil && bytes.Equal(k, prevKey)
			if !isDup {
				yielded++
				if isFocus {
					focusYielded++
				}
				if focusKey == nil {
					fmt.Printf("yield key=0x%x txN=%d\n", k, txN)
				}
			}
			prevKey = append(prevKey[:0], k...)
			if isFocus {
				fmt.Printf("focus%s key=0x%x txN=%d\n", func() string {
					if isDup {
						return ".dup"
					}
					return ".yield"
				}(), k, txN)
			}
		}
		fmt.Println(strings.Repeat("-", 78))
		fmt.Printf("summary: raw=%d yielded=%d\n", raw, yielded)
		if focusKey != nil {
			fmt.Printf("focus: 0x%x hits=%d yielded=%d lastTxN=%d\n",
				focusKey, focusHits, focusYielded, focusLastTxN)
			if focusHits == 0 {
				fmt.Printf("focus: NOT in walker output for range (%d, %d] — v4 emit would miss this key\n",
					hwdFromTxN, hwdToTxN)
			}
		}
	},
}

func init() {
	withDataDir(cmdHistoryWalkDump)
	withChain(cmdHistoryWalkDump)
	cmdHistoryWalkDump.Flags().StringVar(&hwdDomainStr, "domain", "accounts", "accounts | storage | code | commitment | receipt")
	cmdHistoryWalkDump.Flags().Uint64Var(&hwdFromTxN, "from-txN", 0, "walker range start (exclusive)")
	cmdHistoryWalkDump.Flags().Uint64Var(&hwdToTxN, "to-txN", 0, "walker range end (inclusive)")
	cmdHistoryWalkDump.Flags().StringVar(&hwdKeyHex, "key", "", "optional: focus on a single key; report whether it was yielded")
	must(cmdHistoryWalkDump.MarkFlagRequired("from-txN"))
	must(cmdHistoryWalkDump.MarkFlagRequired("to-txN"))

	rootCmd.AddCommand(cmdHistoryWalkDump)
}
