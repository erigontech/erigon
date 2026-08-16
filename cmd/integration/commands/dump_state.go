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

package commands

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/holiman/uint256"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/debug"
)

var (
	dumpStateOutput        string
	dumpStateDryRun        bool
	dumpStateExpectedTotal uint64
	dumpStateCompress      string
	dumpStateMinBalance    string
	dumpStateLatest        bool
)

const (
	dumpStateProgressEvery    = 10_000_000
	dumpStateWriteBufferBytes = 4 << 20
)

// dumpStateColumns documents the TSV layout, and is recorded in the sidecar metadata so a
// consumer can tell which schema produced a file.
var dumpStateColumns = []string{"address", "balance_wei", "nonce", "has_code"}

func init() {
	withDataDir(dumpState)
	withChain(dumpState)
	withBlock(dumpState)
	dumpState.Flags().StringVar(&dumpStateOutput, "output", "", "output TSV file path (required unless --dry-run)")
	dumpState.Flags().BoolVar(&dumpStateDryRun, "dry-run", false, "count accounts without writing a TSV file")
	dumpState.Flags().Uint64Var(&dumpStateExpectedTotal, "expected-total", 0, "expected account count, used to log an ETA (e.g. from a prior --dry-run)")
	dumpState.Flags().StringVar(&dumpStateCompress, "compress", "none", "compress output: none, gzip or zstd")
	dumpState.Flags().StringVar(&dumpStateMinBalance, "min-balance", "", "skip accounts with balance below this (wei, decimal)")
	dumpState.Flags().BoolVar(&dumpStateLatest, "latest", false, "dump at current execution progress; on a syncing node this is the only way to get the faster latest-state scan")
	dumpState.MarkFlagsMutuallyExclusive("block", "latest")
	dumpState.MarkFlagsOneRequired("block", "latest")

	rootCmd.AddCommand(dumpState)
}

var dumpState = &cobra.Command{
	Use:   "dump_state",
	Short: "Dump every account's address, balance, nonce and has_code flag at a block to a TSV file",
	Example: `  integration dump_state --datadir=... --chain=mainnet --latest --output=/tmp/state.tsv --compress=zstd
  integration dump_state --datadir=... --chain=mainnet --block=18000000 --output=/tmp/state.tsv
  integration dump_state --datadir=... --chain=mainnet --latest --dry-run   # just count accounts`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger, ctx := debug.SetupCobra(cmd, "integration"), cmd.Context()

		if !dumpStateDryRun && dumpStateOutput == "" {
			return errors.New("--output is required unless --dry-run is set")
		}

		minBalance, err := parseMinBalance(dumpStateMinBalance)
		if err != nil {
			return err
		}

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			return fmt.Errorf("opening db: %w", err)
		}
		defer chainDb.Close()

		// A full-domain scan reads the files front to back, so readahead pays off. Only safe
		// in CLI tools: app-level code serves random reads and would thrash it.
		defer chainDb.Debug().EnableReadAhead().DisableReadAhead()

		tx, err := chainDb.BeginTemporalRo(ctx)
		if err != nil {
			return fmt.Errorf("beginning temporal tx: %w", err)
		}
		defer tx.Rollback()

		// Resolving --latest inside the read transaction is what makes the latest-state path
		// reachable: a block number chosen before the scan starts is already stale on a
		// syncing node, so it always falls back to the historical iterator.
		blockNumber := block
		if dumpStateLatest {
			if blockNumber, err = executionProgress(tx); err != nil {
				return err
			}
			logger.Info("dump_state: resolved --latest", "block", blockNumber)
		}

		w := io.Writer(io.Discard)
		var out *dumpOutput
		if !dumpStateDryRun {
			if warning := compressionExtensionMismatch(dumpStateOutput, dumpStateCompress); warning != "" {
				logger.Warn("dump_state: " + warning)
			}
			out, err = newDumpOutput(dumpStateOutput, dumpStateCompress)
			if err != nil {
				return err
			}
			defer out.Abort() // no-op once committed; discards the temp file on any failure
			w = out.Writer()
		} else {
			// Validate the codec even though nothing is written, so a typo surfaces on the
			// cheap run rather than after a multi-hour dump.
			if _, err := newCompressor(dumpStateCompress, io.Discard); err != nil {
				return err
			}
			if dumpStateOutput != "" {
				logger.Warn("dump_state: --output is ignored with --dry-run; no file will be written", "output", dumpStateOutput)
			}
		}

		res, err := dumpStateToTSV(ctx, tx, blockNumber, dumpStateDryRun, dumpStateExpectedTotal, minBalance, w, logger)
		if err != nil {
			return err
		}

		if out != nil {
			// Commit before reporting success: a truncated codec footer, a full disk, or a
			// failed rename all surface here rather than being logged as a completed dump.
			if err := out.Commit(); err != nil {
				return err
			}
			if err := writeDumpMetadata(dumpStateOutput+".meta.json", buildDumpMetadata(ctx, chainDb, tx, blockNumber, logger, res)); err != nil {
				return fmt.Errorf("writing metadata sidecar: %w", err)
			}
		}

		logger.Info("dump_state done",
			"block", blockNumber,
			"accounts", res.Matched,
			"scanned", res.Scanned,
			"took", res.Elapsed.Round(time.Millisecond),
			"accounts/s", fmt.Sprintf("%.0f", float64(res.Scanned)/max(res.Elapsed.Seconds(), 1e-9)),
			"source", dumpSource(res.AtTip),
			"dryRun", dumpStateDryRun)
		return nil
	},
}

// buildDumpMetadata gathers sidecar fields. Block hash and state root need the block reader,
// and are omitted rather than failing the dump if the header cannot be read.
func buildDumpMetadata(ctx context.Context, db kv.RoDB, tx kv.TemporalTx, blockNumber uint64, logger log.Logger, res dumpResult) dumpMetadata {
	meta := dumpMetadata{
		Chain:          chain,
		Block:          blockNumber,
		AccountsInFile: res.Matched,
		AccountsInVal:  res.Scanned,
		MinBalanceWei:  dumpStateMinBalance,
		Columns:        dumpStateColumns,
		Compression:    dumpStateCompress,
		Source:         dumpSource(res.AtTip),
		ElapsedSeconds: res.Elapsed.Seconds(),
		ErigonVersion:  version.VersionWithMeta,
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	if txNum, err := rawdbv3.TxNums.Min(ctx, tx, blockNumber+1); err == nil {
		meta.TxNum = txNum
	} else {
		logger.Warn("dump_state: could not resolve txNum for metadata", "err", err)
	}

	blockReader, _ := blocksIO(db, logger)
	if header, err := blockReader.HeaderByNumber(ctx, tx, blockNumber); err != nil {
		logger.Warn("dump_state: could not read header for metadata", "err", err)
	} else if header != nil {
		meta.BlockHash = header.Hash().Hex()
		meta.StateRoot = header.Root.Hex()
	}

	return meta
}

// compressionExtensionMismatch returns a warning when the output name implies a codec the
// dump is not using, which would otherwise produce a file whose extension lies about it.
// Returns an empty string when the two agree.
func compressionExtensionMismatch(path, compression string) string {
	byExt := map[string]string{".zst": "zstd", ".zstd": "zstd", ".gz": "gzip", ".gzip": "gzip"}

	implied, hasExt := byExt[strings.ToLower(filepath.Ext(path))]
	if compression == "" {
		compression = "none"
	}

	switch {
	case hasExt && implied != compression:
		return fmt.Sprintf("output name suggests %s but --compress=%s; the file will not be %s-compressed", implied, compression, implied)
	case !hasExt && compression != "none":
		return fmt.Sprintf("--compress=%s but the output name has no matching extension", compression)
	}
	return ""
}

// dumpSource names the iterator a dump came from, so a published file records whether it
// was read from latest state or reconstructed from history.
func dumpSource(atTip bool) string {
	if atTip {
		return "latest_state"
	}
	return "history_as_of"
}

// newCompressor wraps out according to kind. A nil writer means no compression. Both codecs
// use their fastest setting: at these volumes compression is the CPU bottleneck, not the scan.
func newCompressor(kind string, out io.Writer) (io.WriteCloser, error) {
	switch kind {
	case "", "none":
		return nil, nil
	case "gzip":
		return gzip.NewWriterLevel(out, gzip.BestSpeed)
	case "zstd":
		return zstd.NewWriter(out, zstd.WithEncoderLevel(zstd.SpeedFastest))
	default:
		return nil, fmt.Errorf("unknown --compress value %q, want none, gzip or zstd", kind)
	}
}

// dumpOutput writes to a temporary file and renames it over the destination only once the
// dump has completed, so an interrupted run never leaves a partial file that looks whole.
type dumpOutput struct {
	path string
	tmp  *os.File
	comp io.WriteCloser
	w    io.Writer
	done bool
}

func newDumpOutput(path, compression string) (*dumpOutput, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".partial-*")
	if err != nil {
		return nil, fmt.Errorf("creating temporary output file: %w", err)
	}

	comp, err := newCompressor(compression, tmp)
	if err != nil {
		tmp.Close()                //nolint:errcheck // already failing
		dir.RemoveFile(tmp.Name()) //nolint:errcheck // best-effort cleanup
		return nil, err
	}

	o := &dumpOutput{path: path, tmp: tmp, comp: comp, w: tmp}
	if comp != nil {
		o.w = comp
	}
	return o, nil
}

func (o *dumpOutput) Writer() io.Writer { return o.w }

// Commit finalises the compressor and file, then renames the temporary file into place.
func (o *dumpOutput) Commit() error {
	if o.done {
		return nil
	}
	o.done = true

	if o.comp != nil {
		if err := o.comp.Close(); err != nil {
			o.tmp.Close()                //nolint:errcheck // returning the more specific codec error
			dir.RemoveFile(o.tmp.Name()) //nolint:errcheck // best-effort cleanup
			return fmt.Errorf("closing compressor: %w", err)
		}
	}
	if err := o.tmp.Close(); err != nil {
		dir.RemoveFile(o.tmp.Name()) //nolint:errcheck // best-effort cleanup
		return fmt.Errorf("closing output file: %w", err)
	}
	if err := os.Rename(o.tmp.Name(), o.path); err != nil {
		return fmt.Errorf("renaming output into place: %w", err)
	}
	return nil
}

// Abort discards the temporary file. Safe to call after Commit.
func (o *dumpOutput) Abort() {
	if o.done {
		return
	}
	o.done = true
	o.tmp.Close()                //nolint:errcheck // discarding a failed dump
	dir.RemoveFile(o.tmp.Name()) //nolint:errcheck // best-effort cleanup
}

// dumpMetadata describes a dump so a published file is self-identifying: which chain and
// block it came from, and which schema produced it.
type dumpMetadata struct {
	Chain          string   `json:"chain"`
	Block          uint64   `json:"block"`
	BlockHash      string   `json:"block_hash,omitempty"`
	StateRoot      string   `json:"state_root,omitempty"`
	TxNum          uint64   `json:"tx_num"`
	AccountsInFile uint64   `json:"accounts_in_file"`
	AccountsInVal  uint64   `json:"accounts_scanned"`
	MinBalanceWei  string   `json:"min_balance_wei,omitempty"`
	Columns        []string `json:"columns"`
	Compression    string   `json:"compression"`
	Source         string   `json:"source"`
	ElapsedSeconds float64  `json:"elapsed_seconds"`
	ErigonVersion  string   `json:"erigon_version"`
	GeneratedAt    string   `json:"generated_at"`
}

// writeDumpMetadata writes the sidecar next to the dump, atomically like the dump itself.
func writeDumpMetadata(path string, meta dumpMetadata) error {
	blob, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	blob = append(blob, '\n')

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".partial-*")
	if err != nil {
		return fmt.Errorf("creating temporary metadata file: %w", err)
	}
	if _, err := tmp.Write(blob); err != nil {
		tmp.Close()                //nolint:errcheck // already failing
		dir.RemoveFile(tmp.Name()) //nolint:errcheck // best-effort cleanup
		return fmt.Errorf("writing metadata: %w", err)
	}
	if err := tmp.Close(); err != nil {
		dir.RemoveFile(tmp.Name()) //nolint:errcheck // best-effort cleanup
		return fmt.Errorf("closing metadata: %w", err)
	}
	return os.Rename(tmp.Name(), path)
}

// executionProgress reports the last executed block as seen by tx.
func executionProgress(tx kv.Tx) (uint64, error) {
	progress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return 0, fmt.Errorf("reading execution progress: %w", err)
	}
	return progress, nil
}

// resolveBlock rejects blocks past execution progress and reports whether blockNumber is
// the execution tip. rawdbv3.TxNums.Min silently returns the latest tx number for an
// unknown block, which would dump tip state while reporting the requested block.
//
// Progress is read inside the caller's read transaction, so the tip it identifies stays
// consistent with the state that transaction observes even as the node keeps executing.
func resolveBlock(tx kv.Tx, blockNumber uint64) (isTip bool, err error) {
	progress, err := executionProgress(tx)
	if err != nil {
		return false, err
	}
	if blockNumber > progress {
		return false, fmt.Errorf("block %d is beyond execution progress %d", blockNumber, progress)
	}
	return blockNumber == progress, nil
}

// openAccountsIterator streams the accounts domain as of blockNumber. At the execution tip
// the latest-state iterator is enough; older blocks additionally need the history union
// that RangeAsOf builds, which clones every record it merges.
func openAccountsIterator(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, isTip bool) (stream.KV, error) {
	if isTip {
		it, err := tx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, kv.Unlim)
		if err != nil {
			return nil, fmt.Errorf("opening latest accounts range: %w", err)
		}
		return it, nil
	}

	txNum, err := rawdbv3.TxNums.Min(ctx, tx, blockNumber+1)
	if err != nil {
		return nil, fmt.Errorf("resolving txNum for block %d: %w", blockNumber, err)
	}
	it, err := tx.RangeAsOf(kv.AccountsDomain, nil, nil, txNum, order.Asc, kv.Unlim)
	if err != nil {
		return nil, fmt.Errorf("opening accounts range as of block %d: %w", blockNumber, err)
	}
	return it, nil
}

// parseMinBalance parses --min-balance (decimal wei). An empty string means no filter.
func parseMinBalance(s string) (*uint256.Int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	v, err := uint256.FromDecimal(s)
	if err != nil {
		return nil, fmt.Errorf("parsing %q as a decimal wei amount: %w", s, err)
	}
	return v, nil
}

// meetsMinBalance reports whether balance passes the --min-balance filter. A nil min means
// no filter is configured.
func meetsMinBalance(balance, min *uint256.Int) bool {
	if min == nil {
		return true
	}
	return balance.Cmp(min) >= 0
}

// dumpResult reports what a scan covered. Scanned counts every live account read; Matched
// counts those that passed --min-balance and so reached the output.
type dumpResult struct {
	Scanned uint64
	Matched uint64
	Elapsed time.Duration
	// AtTip records whether the latest-state iterator was used instead of the historical
	// one, which is the difference between a fast scan and a slow one.
	AtTip bool
}

// dumpStateToTSV streams the AccountsDomain as of blockNumber one record at a time, so
// memory stays bounded regardless of the total account count.
func dumpStateToTSV(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, dryRun bool, expectedTotal uint64, minBalance *uint256.Int, w io.Writer, logger log.Logger) (dumpResult, error) {
	res := dumpResult{}

	isTip, err := resolveBlock(tx, blockNumber)
	if err != nil {
		return res, err
	}
	res.AtTip = isTip

	it, err := openAccountsIterator(ctx, tx, blockNumber, isTip)
	if err != nil {
		return res, err
	}
	defer it.Close()

	// A dry run writes nothing, so it does not pay for the large output buffer.
	bufSize := dumpStateWriteBufferBytes
	if dryRun {
		bufSize = 4096
	}
	bw := bufio.NewWriterSize(w, bufSize)
	start := time.Now()

	// Counting alone needs no account fields, so skip the decode entirely — it dominates
	// the scan, mostly through the global code-hash interning inside DeserialiseV3.
	countOnly := dryRun && minBalance == nil

	var acc accounts.Account
	for it.HasNext() {
		if err := ctx.Err(); err != nil {
			res.Elapsed = time.Since(start)
			return res, err
		}

		k, v, err := it.Next()
		if err != nil {
			res.Elapsed = time.Since(start)
			return res, fmt.Errorf("iterating accounts: %w", err)
		}
		if len(v) == 0 {
			continue // deleted account tombstone
		}
		res.Scanned++

		if !countOnly {
			if err := accounts.DeserialiseV3(&acc, v); err != nil {
				res.Elapsed = time.Since(start)
				return res, fmt.Errorf("decoding account %x: %w", k, err)
			}
			if !meetsMinBalance(&acc.Balance, minBalance) {
				// Progress is driven by scanned records so a restrictive filter still reports.
				if res.Scanned%dumpStateProgressEvery == 0 {
					logProgress(logger, res, time.Since(start), expectedTotal)
				}
				continue
			}
			if !dryRun {
				if err := writeAccountRow(bw, k, &acc.Balance, acc.Nonce, !acc.IsEmptyCodeHash()); err != nil {
					res.Elapsed = time.Since(start)
					return res, err
				}
			}
		}
		res.Matched++

		if res.Scanned%dumpStateProgressEvery == 0 {
			logProgress(logger, res, time.Since(start), expectedTotal)
		}
	}

	logProgress(logger, res, time.Since(start), expectedTotal)

	if err := bw.Flush(); err != nil {
		res.Elapsed = time.Since(start)
		return res, fmt.Errorf("flushing output: %w", err)
	}
	res.Elapsed = time.Since(start)
	return res, nil
}

func logProgress(logger log.Logger, res dumpResult, elapsed time.Duration, expectedTotal uint64) {
	args := []any{"scanned", res.Scanned}
	if res.Matched != res.Scanned {
		args = append(args, "matched", res.Matched)
	}

	rate, eta, haveETA := progressStats(int(res.Scanned), elapsed, expectedTotal)
	args = append(args, "accounts/s", fmt.Sprintf("%.0f", rate), "elapsed", elapsed.Round(time.Second))
	if haveETA {
		args = append(args, "eta", eta.Round(time.Second))
	}
	logger.Info("dump_state progress", args...)
}

// progressStats returns the accounts/s rate for the run so far, and — when expectedTotal
// is known (e.g. from a prior --dry-run) — an ETA for the remaining accounts.
func progressStats(total int, elapsed time.Duration, expectedTotal uint64) (rate float64, eta time.Duration, haveETA bool) {
	if elapsed <= 0 {
		return 0, 0, false
	}
	rate = float64(total) / elapsed.Seconds()
	if expectedTotal == 0 || rate == 0 || uint64(total) >= expectedTotal {
		return rate, 0, false
	}
	remaining := float64(expectedTotal) - float64(total)
	return rate, time.Duration(remaining/rate) * time.Second, true
}

// writeAccountRow writes one TSV row (address, balance, nonce, has_code) straight to bw,
// using stack buffers instead of fmt.Sprintf / addr.Hex() / balance.ToBig() to keep this
// hot, per-account path allocation-free.
func writeAccountRow(bw *bufio.Writer, addr []byte, balance *uint256.Int, nonce uint64, hasCode bool) error {
	var hexBuf [2 + 2*length.Addr]byte
	hexBuf[0], hexBuf[1] = '0', 'x'
	hex.Encode(hexBuf[2:], addr)
	if _, err := bw.Write(hexBuf[:]); err != nil {
		return err
	}

	if err := bw.WriteByte('\t'); err != nil {
		return err
	}
	if _, err := bw.WriteString(balance.Dec()); err != nil {
		return err
	}

	if err := bw.WriteByte('\t'); err != nil {
		return err
	}
	var nonceBuf [20]byte
	if _, err := bw.Write(strconv.AppendUint(nonceBuf[:0], nonce, 10)); err != nil {
		return err
	}

	if err := bw.WriteByte('\t'); err != nil {
		return err
	}
	if hasCode {
		_, err := bw.WriteString("true\n")
		return err
	}
	_, err := bw.WriteString("false\n")
	return err
}
