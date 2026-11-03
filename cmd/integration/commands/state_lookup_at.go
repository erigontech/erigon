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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/debug"
)

// state-lookup-at prints how each read layer resolves a (domain, key, ts)
// query. It exists to diagnose mode-B unwind bugs where different layers
// (MDBX vs history vs files vs GetAsOf-with-fallthrough) disagree on the
// as-of-lastTxN value for a specific key — the exact shape of the
// 2026-07-11 cleanrun5 iter-2 recovery-timeout (nonce state=450 vs
// tx=417 on 0xa40DEe...4B6).
//
// The subcommand does no writes; datadir is opened read-only.
var (
	sluDomainStr string
	sluKeyHex    string
	sluTsFlag    uint64
	sluFromBlock uint64
	sluToBlock   uint64
	sluBlockStep uint64
)

var cmdStateLookupAt = &cobra.Command{
	Use:   "state-lookup-at",
	Short: "Print each read-layer's answer for (domain, key, ts). Diagnostic for unwind-reconstruction bugs.",
	Long: `Given --domain (accounts|storage|code|commitment), --key (hex), and
--ts (txNum), print what each layer returns:
  - HistorySeek(domain, key, ts)               — history-only, no fallthrough
  - GetAsOf(domain, key, ts)                   — history then GetLatest fallthrough
  - Debug.GetLatestFromDB(domain, key)         — MDBX only (with step)
  - Debug.GetLatestFromFiles(domain, key, ts)  — files only, capped at ts
  - Debug.GetLatestFromFiles(domain, key, max) — files only, no cap
For accounts, values are decoded into nonce/balance/codeHash.

Answers should MATCH if the reconstruction pipeline is consistent. If they
diverge, the divergence localises the bug.`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx := cmd.Context()

		domain, ok := parseDomainName(sluDomainStr)
		if !ok {
			logger.Error("invalid --domain; want one of accounts|storage|code|commitment", "got", sluDomainStr)
			return
		}
		key, err := hex.DecodeString(strings.TrimPrefix(sluKeyHex, "0x"))
		if err != nil {
			logger.Error("invalid --key hex", "err", err)
			return
		}
		if sluTsFlag == 0 {
			logger.Error("--ts (txNum) is required and must be > 0")
			return
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

		stepSize := tx.Debug().StepSize()
		tsStep := uint64(0)
		if stepSize > 0 {
			tsStep = sluTsFlag / stepSize
		}

		fmt.Printf("state-lookup-at: domain=%s key=0x%x ts=%d stepSize=%d ts_step=%d\n",
			domain, key, sluTsFlag, stepSize, tsStep)
		fmt.Println(strings.Repeat("-", 78))

		printResult := func(label string, v []byte, ok bool, extra string) {
			present := "not-found"
			if ok {
				present = fmt.Sprintf("len=%d v=0x%x", len(v), v)
			}
			line := fmt.Sprintf("%-38s | %s", label, present)
			if extra != "" {
				line += " | " + extra
			}
			fmt.Println(line)
			// Decode accounts value for readability.
			if ok && domain == kv.AccountsDomain && len(v) > 0 {
				acc := accounts.Account{}
				if err := accounts.DeserialiseV3(&acc, v); err == nil {
					fmt.Printf("%-38s |   -> nonce=%d balance=%s codeHash=0x%x\n",
						"", acc.Nonce, acc.Balance.String(), acc.CodeHash)
				}
			}
		}

		// 1) HistorySeek — pure history, no state fallthrough.
		hv, hok, herr := tx.HistorySeek(domain, key, sluTsFlag)
		extra := ""
		if herr != nil {
			extra = "err=" + herr.Error()
		}
		printResult("HistorySeek(ts)", hv, hok, extra)

		// 2) GetAsOf — history then GetLatest fallthrough for domains that
		// return not-found from history.
		gv, gok, gerr := tx.GetAsOf(domain, key, sluTsFlag)
		extra = ""
		if gerr != nil {
			extra = "err=" + gerr.Error()
		}
		printResult("GetAsOf(ts)", gv, gok, extra)

		// 3) MDBX only — writable-shadow value, no file fallthrough
		// beyond the filter's own logic.
		dv, dstep, dfound, derr := tx.Debug().GetLatestFromDB(domain, key)
		extra = fmt.Sprintf("step=%d", dstep)
		if derr != nil {
			extra += " err=" + derr.Error()
		}
		printResult("Debug.GetLatestFromDB()", dv, dfound, extra)

		// 4) Files only, no cap — the file view without ts constraint.
		fv, ffound, fStart, fEnd, ferr := tx.Debug().GetLatestFromFiles(domain, key, math.MaxUint64)
		extra = fmt.Sprintf("fileStart=%d fileEnd=%d", fStart, fEnd)
		if ferr != nil {
			extra += " err=" + ferr.Error()
		}
		printResult("Debug.GetLatestFromFiles(maxTxN=max)", fv, ffound, extra)

		// 5) Files only, capped at ts+1 — mirrors what recompute paths
		// use when they want "files as of ts".
		fv2, ffound2, fStart2, fEnd2, ferr2 := tx.Debug().GetLatestFromFiles(domain, key, sluTsFlag+1)
		extra = fmt.Sprintf("fileStart=%d fileEnd=%d", fStart2, fEnd2)
		if ferr2 != nil {
			extra += " err=" + ferr2.Error()
		}
		printResult(fmt.Sprintf("Debug.GetLatestFromFiles(maxTxN=%d)", sluTsFlag+1), fv2, ffound2, extra)

		fmt.Println(strings.Repeat("-", 78))
		fmt.Println("MDBX ValuesTable rows for this key (all steps):")
		// Enumerate MDBX ValuesTable rows for this key across steps. The
		// domain's ValuesTable stores rows keyed by (key, invStep) in
		// LargeValues mode or (key -> stepBytes+value) in DupSort mode.
		// getLatestFromDb returns only the newest — for unwind diagnostics
		// we want the full set to see whether wipe left post-boundary
		// stragglers.
		if err := printMDBXStepsForKey(tx, domain, key); err != nil {
			fmt.Printf("  (MDBX step scan failed: %v)\n", err)
		}

		fmt.Println(strings.Repeat("-", 78))
		fmt.Println("History TraceKey trajectory (all changes over history):")
		// TraceKey walks the inverted index + history to yield every
		// (txNum, value_after_change) pair. For nonce diagnostics this
		// gives the exact trajectory across the unwind window.
		if err := printTraceKeyTrajectory(cmd.Context(), tx, domain, key); err != nil {
			fmt.Printf("  (TraceKey failed: %v)\n", err)
		}

		// Canonical-trajectory-by-block query: given --from-block, --to-block, --block-step,
		// query HistorySeek at each block's lastTxN+1. This is the "ground truth" trajectory
		// from history — independent of any writable-shadow state. Used to see what nonce
		// canonical chain has at each block, so we can compare against the state forward
		// re-exec produced.
		if sluFromBlock > 0 && sluToBlock >= sluFromBlock {
			step := sluBlockStep
			if step == 0 {
				step = 1
			}
			fmt.Println(strings.Repeat("-", 78))
			fmt.Printf("Canonical-trajectory-by-block (HistorySeek at each block's lastTxN+1):\n")
			fmt.Printf("  from=%d to=%d step=%d\n", sluFromBlock, sluToBlock, step)
			for blk := sluFromBlock; blk <= sluToBlock; blk += step {
				lastTxN, terr := rawdbv3.TxNums.Max(cmd.Context(), tx, blk)
				if terr != nil {
					fmt.Printf("  block=%d TxNums.Max err=%v\n", blk, terr)
					continue
				}
				bv, bok, berr := tx.HistorySeek(domain, key, lastTxN+1)
				if berr != nil {
					fmt.Printf("  block=%d lastTxN=%d err=%v\n", blk, lastTxN, berr)
					continue
				}
				if !bok {
					fmt.Printf("  block=%d lastTxN=%d HistorySeek not-found (no history record ≤ ts)\n", blk, lastTxN)
					continue
				}
				if domain == kv.AccountsDomain && len(bv) > 0 {
					acc := accounts.Account{}
					if derr := accounts.DeserialiseV3(&acc, bv); derr == nil {
						fmt.Printf("  block=%d lastTxN=%d nonce=%d balance=%s\n",
							blk, lastTxN, acc.Nonce, acc.Balance.String())
						continue
					}
				}
				fmt.Printf("  block=%d lastTxN=%d v=0x%x\n", blk, lastTxN, bv)
			}
			// Also print raw common.Address rendering of the key when relevant, to
			// make cross-referencing easier in shell.
			if domain == kv.AccountsDomain && len(key) == 20 {
				var addr common.Address
				copy(addr[:], key)
				fmt.Printf("  (address = %s)\n", addr.String())
			}
		}

		fmt.Println(strings.Repeat("-", 78))
		fmt.Println("Interpretation:")
		fmt.Println("  If HistorySeek differs from GetAsOf → GetAsOf fell through to GetLatest.")
		fmt.Println("  If GetAsOf == Debug.GetLatestFromDB → GetLatest was served by MDBX.")
		fmt.Println("  If GetAsOf == Debug.GetLatestFromFiles → GetLatest was served by files.")
		fmt.Println("  If HistorySeek matches ts's expected value but everything else differs → the")
		fmt.Println("    fallthrough is what baked the wrong value into unwind's reconstruction.")
	},
}

// printMDBXStepsForKey scans the domain's ValuesTable for every row
// keyed by `key`, printing each (step, value) pair. The domain's
// LargeValues bit governs the encoding: with LargeValues=true rows
// are (fullKey=key||invStep -> value); with LargeValues=false rows
// are (key -> invStep||value) DupSort. For diagnostic purposes we
// probe both by opening the raw table cursor.
func printMDBXStepsForKey(tx kv.TemporalTx, domain kv.Domain, key []byte) error {
	valsTable := domainValsTable(domain)
	if valsTable == "" {
		return fmt.Errorf("no values table known for domain %s", domain)
	}
	// LargeValues probe: iterate all rows whose prefix matches `key`.
	c, err := tx.Cursor(valsTable)
	if err != nil {
		return fmt.Errorf("open cursor(%s): %w", valsTable, err)
	}
	defer c.Close()
	rows := 0
	for k, v, err := c.Seek(key); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if len(k) < len(key) || !hexEq(k[:len(key)], key) {
			break
		}
		var step uint64
		var val []byte
		switch {
		case len(k) == len(key)+8: // LargeValues layout
			step = ^binary.BigEndian.Uint64(k[len(key):])
			val = v
		case len(k) == len(key) && len(v) >= 8: // DupSort layout
			step = ^binary.BigEndian.Uint64(v[:8])
			val = v[8:]
		default:
			continue
		}
		rows++
		fmt.Printf("  step=%d len=%d v=0x%x\n", step, len(val), val)
		if domain == kv.AccountsDomain && len(val) > 0 {
			acc := accounts.Account{}
			if derr := accounts.DeserialiseV3(&acc, val); derr == nil {
				fmt.Printf("    -> nonce=%d balance=%s codeHash=0x%x\n",
					acc.Nonce, acc.Balance.String(), acc.CodeHash)
			}
		}
	}
	if rows == 0 {
		fmt.Println("  (no MDBX rows for this key)")
	}
	return nil
}

// printTraceKeyTrajectory prints (txNum, value_after_change) pairs for
// every history-recorded change of the key. HistoryDisabled domains
// (commitment) can't be traced this way and return an error.
func printTraceKeyTrajectory(ctx interface{ Done() <-chan struct{} }, tx kv.TemporalTx, domain kv.Domain, key []byte) error {
	// We use the aggregator's Debug interface for TraceKey.
	trace, err := tx.Debug().TraceKey(domain, key, 0, math.MaxUint64)
	if err != nil {
		return err
	}
	rows := 0
	for trace.HasNext() {
		txNum, val, verr := trace.Next()
		if verr != nil {
			return verr
		}
		rows++
		fmt.Printf("  txNum=%d len=%d v=0x%x\n", txNum, len(val), val)
		if domain == kv.AccountsDomain && len(val) > 0 {
			acc := accounts.Account{}
			if derr := accounts.DeserialiseV3(&acc, val); derr == nil {
				fmt.Printf("    -> nonce=%d balance=%s codeHash=0x%x\n",
					acc.Nonce, acc.Balance.String(), acc.CodeHash)
			}
		}
	}
	if rows == 0 {
		fmt.Println("  (no history changes recorded)")
	}
	return nil
}

// hexEq reports whether two byte slices are equal without importing
// bytes just for one use.
func hexEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// domainValsTable returns the raw ValuesTable name for a domain. The
// mapping is stable per Erigon 3 schema.
func domainValsTable(d kv.Domain) string {
	switch d {
	case kv.AccountsDomain:
		return kv.TblAccountVals
	case kv.StorageDomain:
		return kv.TblStorageVals
	case kv.CodeDomain:
		return kv.TblCodeVals
	case kv.CommitmentDomain:
		return kv.TblCommitmentVals
	case kv.ReceiptDomain:
		return kv.TblReceiptVals
	}
	return ""
}

func parseDomainName(s string) (kv.Domain, bool) {
	switch strings.ToLower(s) {
	case "accounts", "account":
		return kv.AccountsDomain, true
	case "storage":
		return kv.StorageDomain, true
	case "code":
		return kv.CodeDomain, true
	case "commitment":
		return kv.CommitmentDomain, true
	case "receipt":
		return kv.ReceiptDomain, true
	}
	return 0, false
}

func init() {
	withDataDir(cmdStateLookupAt)
	withChain(cmdStateLookupAt)
	cmdStateLookupAt.Flags().StringVar(&sluDomainStr, "domain", "accounts", "accounts | storage | code | commitment | receipt")
	cmdStateLookupAt.Flags().StringVar(&sluKeyHex, "key", "", "hex-encoded key (accounts: 20-byte address)")
	cmdStateLookupAt.Flags().Uint64Var(&sluTsFlag, "ts", 0, "txNum to query as-of (typically lastTxN+1 for unwind-time reads)")
	cmdStateLookupAt.Flags().Uint64Var(&sluFromBlock, "from-block", 0, "if set with --to-block, print canonical HistorySeek nonce per block in the range")
	cmdStateLookupAt.Flags().Uint64Var(&sluToBlock, "to-block", 0, "upper bound (inclusive) for --from-block iteration")
	cmdStateLookupAt.Flags().Uint64Var(&sluBlockStep, "block-step", 1, "block step for --from-block/--to-block range")
	must(cmdStateLookupAt.MarkFlagRequired("key"))
	must(cmdStateLookupAt.MarkFlagRequired("ts"))

	rootCmd.AddCommand(cmdStateLookupAt)
}
