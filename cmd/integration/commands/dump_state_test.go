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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	chainpkg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func writeAccountRowString(t *testing.T, addr []byte, balance *uint256.Int, nonce uint64, hasCode bool) string {
	t.Helper()
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	require.NoError(t, writeAccountRow(bw, addr, balance, nonce, hasCode))
	require.NoError(t, bw.Flush())
	return buf.String()
}

func TestWriteAccountRow_NoCode(t *testing.T) {
	t.Parallel()
	addr := common.HexToAddress("0x0000000000000000000000000000000000000001")

	got := writeAccountRowString(t, addr[:], uint256.NewInt(22), 5, false)

	require.Equal(t, "0x0000000000000000000000000000000000000001\t22\t5\tfalse\n", got)
}

func TestWriteAccountRow_HasCode(t *testing.T) {
	t.Parallel()
	addr := common.HexToAddress("0x0000000000000000000000000000000000000002")

	got := writeAccountRowString(t, addr[:], uint256.NewInt(0), 1, true)

	require.Equal(t, "0x0000000000000000000000000000000000000002\t0\t1\ttrue\n", got)
}

func TestProgressStats_WithExpectedTotal(t *testing.T) {
	t.Parallel()

	rate, eta, haveETA := progressStats(500, 10*time.Second, 1000)

	require.Equal(t, 50.0, rate)
	require.True(t, haveETA)
	require.Equal(t, 10*time.Second, eta)
}

func TestProgressStats_NoExpectedTotal(t *testing.T) {
	t.Parallel()

	rate, _, haveETA := progressStats(500, 10*time.Second, 0)

	require.Equal(t, 50.0, rate)
	require.False(t, haveETA)
}

func TestProgressStats_ZeroElapsed(t *testing.T) {
	t.Parallel()

	rate, _, haveETA := progressStats(500, 0, 1000)

	require.Equal(t, 0.0, rate)
	require.False(t, haveETA)
}

func TestParseMinBalance_Empty(t *testing.T) {
	t.Parallel()

	got, err := parseMinBalance("")

	require.NoError(t, err)
	require.Nil(t, got)
}

func TestParseMinBalance_Decimal(t *testing.T) {
	t.Parallel()

	got, err := parseMinBalance("1000000000000000000")

	require.NoError(t, err)
	require.Equal(t, uint256.MustFromDecimal("1000000000000000000"), got)
}

func TestParseMinBalance_Invalid(t *testing.T) {
	t.Parallel()

	_, err := parseMinBalance("not-a-number")

	require.Error(t, err)
}

func TestMeetsMinBalance_NoFilter(t *testing.T) {
	t.Parallel()

	require.True(t, meetsMinBalance(uint256.NewInt(0), nil))
}

func TestMeetsMinBalance_BelowThreshold(t *testing.T) {
	t.Parallel()

	require.False(t, meetsMinBalance(uint256.NewInt(99), uint256.NewInt(100)))
}

func TestMeetsMinBalance_AtThreshold(t *testing.T) {
	t.Parallel()

	require.True(t, meetsMinBalance(uint256.NewInt(100), uint256.NewInt(100)))
}

// seedTestAccounts writes an EOA, a contract, and a dust (1 wei) account into a fresh
// temporal DB at block 1, and returns a tx positioned to read that state back.
func seedTestAccounts(t *testing.T) kv.TemporalTx {
	t.Helper()

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, 16)
	t.Cleanup(db.Close)
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(domains.Close)

	txNum, _, err := domains.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 1))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1))

	eoaAddr := accounts.InternAddress(common.HexToAddress("0x01"))
	contractAddr := accounts.InternAddress(common.HexToAddress("0x02"))
	dustAddr := accounts.InternAddress(common.HexToAddress("0x03"))

	st := state.New(state.NewReaderV3(domains.AsGetter(tx)))
	defer st.Release(false)

	_, err = st.GetOrNewStateObject(eoaAddr)
	require.NoError(t, err)
	require.NoError(t, st.SetNonce(eoaAddr, 5, tracing.NonceChangeUnspecified))
	require.NoError(t, st.AddBalance(eoaAddr, *uint256.NewInt(1000), tracing.BalanceChangeUnspecified))

	_, err = st.GetOrNewStateObject(contractAddr)
	require.NoError(t, err)
	require.NoError(t, st.SetNonce(contractAddr, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, st.SetCode(contractAddr, []byte{0x01, 0x02, 0x03}, tracing.CodeChangeUnspecified))

	_, err = st.GetOrNewStateObject(dustAddr)
	require.NoError(t, err)
	require.NoError(t, st.AddBalance(dustAddr, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))

	w := state.NewWriter(domains.AsPutDel(tx), nil, txNum)
	require.NoError(t, st.FinalizeTx(&chainpkg.Rules{}, w))
	blockWriter := state.NewWriter(domains.AsPutDel(tx), nil, txNum)
	require.NoError(t, st.CommitBlock(&chainpkg.Rules{}, blockWriter))
	require.NoError(t, domains.Flush(context.Background(), tx))

	return tx
}

// seedManyAccounts writes n accounts, every sixth carrying distinct code so code-hash
// interning during decode is exercised the way a real state scan would exercise it.
func seedManyAccounts(t testing.TB, n int) kv.TemporalTx {
	t.Helper()

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDBWithStepSize(t, dirs, 16)
	t.Cleanup(db.Close)
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(domains.Close)

	txNum, _, err := domains.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 1))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1))

	st := state.New(state.NewReaderV3(domains.AsGetter(tx)))
	defer st.Release(false)

	for i := range n {
		var raw common.Address
		binary.BigEndian.PutUint64(raw[12:], uint64(i+1))
		addr := accounts.InternAddress(raw)

		_, err = st.GetOrNewStateObject(addr)
		require.NoError(t, err)
		require.NoError(t, st.SetNonce(addr, uint64(i), tracing.NonceChangeUnspecified))
		require.NoError(t, st.AddBalance(addr, *uint256.NewInt(uint64(i) + 1), tracing.BalanceChangeUnspecified))
		if i%6 == 0 {
			code := binary.BigEndian.AppendUint64(nil, uint64(i))
			require.NoError(t, st.SetCode(addr, code, tracing.CodeChangeUnspecified))
		}
	}

	w := state.NewWriter(domains.AsPutDel(tx), nil, txNum)
	require.NoError(t, st.FinalizeTx(&chainpkg.Rules{}, w))
	blockWriter := state.NewWriter(domains.AsPutDel(tx), nil, txNum)
	require.NoError(t, st.CommitBlock(&chainpkg.Rules{}, blockWriter))
	require.NoError(t, domains.Flush(context.Background(), tx))

	return tx
}

func BenchmarkDumpStateToTSV(b *testing.B) {
	const accountCount = 20_000
	tx := seedManyAccounts(b, accountCount)
	logger := log.New()

	b.Run("count_only", func(b *testing.B) {
		for b.Loop() {
			res, err := dumpStateToTSV(context.Background(), tx, 1, true, 0, nil, io.Discard, logger)
			require.NoError(b, err)
			require.Equal(b, uint64(accountCount), res.Matched)
		}
	})

	b.Run("decode_no_write", func(b *testing.B) {
		for b.Loop() {
			res, err := dumpStateToTSV(context.Background(), tx, 1, true, 0, uint256.NewInt(0), io.Discard, logger)
			require.NoError(b, err)
			require.Equal(b, uint64(accountCount), res.Matched)
		}
	})

	b.Run("decode_and_write", func(b *testing.B) {
		for b.Loop() {
			res, err := dumpStateToTSV(context.Background(), tx, 1, false, 0, nil, io.Discard, logger)
			require.NoError(b, err)
			require.Equal(b, uint64(accountCount), res.Matched)
		}
	})

	// The codec is built once, as it is for a real dump: a zstd encoder allocates its
	// window up front, which would otherwise dominate at this payload size.
	for _, kind := range []string{"gzip", "zstd"} {
		b.Run("write_"+kind, func(b *testing.B) {
			comp, err := newCompressor(kind, io.Discard)
			require.NoError(b, err)
			defer comp.Close() //nolint:errcheck // benchmark teardown

			for b.Loop() {
				res, err := dumpStateToTSV(context.Background(), tx, 1, false, 0, nil, comp, logger)
				require.NoError(b, err)
				require.Equal(b, uint64(accountCount), res.Matched)
			}
		})
	}
}

func TestNewCompressor_Unknown(t *testing.T) {
	t.Parallel()

	_, err := newCompressor("bzip2", io.Discard)

	require.Error(t, err)
	require.Contains(t, err.Error(), "bzip2")
}

func TestNewCompressor_None(t *testing.T) {
	t.Parallel()

	c, err := newCompressor("none", io.Discard)

	require.NoError(t, err)
	require.Nil(t, c)
}

func TestDumpOutput_CompressionRoundTrip(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"none", "gzip", "zstd"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "state.tsv")

			out, err := newDumpOutput(path, kind)
			require.NoError(t, err)
			_, err = io.WriteString(out.Writer(), "hello\tworld\n")
			require.NoError(t, err)
			require.NoError(t, out.Commit())

			raw, err := os.ReadFile(path)
			require.NoError(t, err)

			var got []byte
			switch kind {
			case "gzip":
				r, err := gzip.NewReader(bytes.NewReader(raw))
				require.NoError(t, err)
				got, err = io.ReadAll(r)
				require.NoError(t, err)
			case "zstd":
				r, err := zstd.NewReader(bytes.NewReader(raw))
				require.NoError(t, err)
				defer r.Close()
				got, err = io.ReadAll(r)
				require.NoError(t, err)
			default:
				got = raw
			}
			require.Equal(t, "hello\tworld\n", string(got))
		})
	}
}

// An aborted dump must leave no file at the destination, so a partial dump can never be
// mistaken for a complete one.
func TestDumpOutput_AbortLeavesNoFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.tsv")

	out, err := newDumpOutput(path, "gzip")
	require.NoError(t, err)
	_, err = io.WriteString(out.Writer(), "partial")
	require.NoError(t, err)
	out.Abort()

	require.NoFileExists(t, path)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "temporary file should be removed")
}

func TestDumpOutput_AbortAfterCommitKeepsFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.tsv")

	out, err := newDumpOutput(path, "none")
	require.NoError(t, err)
	_, err = io.WriteString(out.Writer(), "kept")
	require.NoError(t, err)
	require.NoError(t, out.Commit())
	out.Abort() // deferred safety net must not delete a committed dump

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "kept", string(raw))
}

func TestCompressionExtensionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path, compression string
		wantWarning       bool
	}{
		{"/tmp/state.tsv.zstd", "none", true},
		{"/tmp/state.tsv.zst", "none", true},
		{"/tmp/state.tsv.gz", "none", true},
		{"/tmp/state.tsv.gz", "zstd", true},
		{"/tmp/state.tsv", "zstd", true},
		{"/tmp/state.tsv.zst", "zstd", false},
		{"/tmp/state.tsv.zstd", "zstd", false},
		{"/tmp/state.tsv.gz", "gzip", false},
		{"/tmp/state.tsv", "none", false},
	}

	for _, tc := range tests {
		got := compressionExtensionMismatch(tc.path, tc.compression)
		if tc.wantWarning {
			require.NotEmpty(t, got, "%s with --compress=%s should warn", tc.path, tc.compression)
		} else {
			require.Empty(t, got, "%s with --compress=%s should not warn", tc.path, tc.compression)
		}
	}
}

func TestExecutionProgress(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	progress, err := executionProgress(tx)

	require.NoError(t, err)
	require.Equal(t, uint64(1), progress)
}

// Naming execution progress as the block must select the latest-state path, which is the
// only way to reach it on a node that keeps syncing while the dump runs.
func TestDumpStateToTSV_ProgressBlockIsTip(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	progress, err := executionProgress(tx)
	require.NoError(t, err)

	res, err := dumpStateToTSV(context.Background(), tx, progress, true, 0, nil, io.Discard, log.New())

	require.NoError(t, err)
	require.True(t, res.AtTip)
	require.Equal(t, uint64(3), res.Scanned)
}

func TestDumpSource(t *testing.T) {
	t.Parallel()

	require.Equal(t, "latest_state", dumpSource(true))
	require.Equal(t, "history_as_of", dumpSource(false))
}

func TestDumpStateToTSV_ReportsTipPath(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	atTip, err := dumpStateToTSV(context.Background(), tx, 1, true, 0, nil, io.Discard, log.New())
	require.NoError(t, err)
	require.True(t, atTip.AtTip, "block 1 is execution progress, so the latest-state path applies")

	older, err := dumpStateToTSV(context.Background(), tx, 0, true, 0, nil, io.Discard, log.New())
	require.NoError(t, err)
	require.False(t, older.AtTip)
}

func TestWriteDumpMetadata(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.tsv.meta.json")

	require.NoError(t, writeDumpMetadata(path, dumpMetadata{
		Chain: "mainnet", Block: 42, AccountsInFile: 7, AccountsInVal: 9, Columns: dumpStateColumns,
	}))

	var got dumpMetadata
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &got))

	require.Equal(t, "mainnet", got.Chain)
	require.Equal(t, uint64(42), got.Block)
	require.Equal(t, uint64(7), got.AccountsInFile)
	require.Equal(t, uint64(9), got.AccountsInVal)
	require.Equal(t, []string{"address", "balance_wei", "nonce", "has_code"}, got.Columns)
}

func TestResolveBlock_AtExecutionProgress(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	isTip, err := resolveBlock(tx, 1)

	require.NoError(t, err)
	require.True(t, isTip)
}

func TestResolveBlock_BelowExecutionProgress(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	isTip, err := resolveBlock(tx, 0)

	require.NoError(t, err)
	require.False(t, isTip)
}

// A block past execution progress must be rejected rather than silently falling back to
// latest state, which is what rawdbv3.TxNums.Min does for an unknown block.
func TestResolveBlock_BeyondExecutionProgress(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	_, err := resolveBlock(tx, 2)

	require.Error(t, err)
	require.Contains(t, err.Error(), "2")
	require.Contains(t, err.Error(), "1")
}

// The tip fast path (RangeLatest) skips the history union that RangeAsOf performs, so it
// must yield byte-identical records for a block that is the execution tip.
func TestOpenAccountsIterator_TipMatchesHistorical(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	collect := func(isTip bool) []string {
		it, err := openAccountsIterator(context.Background(), tx, 1, isTip)
		require.NoError(t, err)
		defer it.Close()

		var out []string
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(t, err)
			out = append(out, fmt.Sprintf("%x=%x", k, v))
		}
		return out
	}

	require.NotEmpty(t, collect(false))
	require.Equal(t, collect(false), collect(true))
}

func TestDumpStateToTSV_RejectsBlockBeyondProgress(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	var buf bytes.Buffer
	_, err := dumpStateToTSV(context.Background(), tx, 2, false, 0, nil, &buf, log.New())

	require.Error(t, err)
	require.Empty(t, buf.String())
}

func TestDumpStateToTSV(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	var buf bytes.Buffer
	res, err := dumpStateToTSV(context.Background(), tx, 1, false, 0, nil, &buf, log.New())
	require.NoError(t, err)

	require.Equal(t, uint64(3), res.Matched)
	require.Equal(t, uint64(3), res.Scanned)
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.ElementsMatch(t, []string{
		"0x0000000000000000000000000000000000000001\t1000\t5\tfalse",
		"0x0000000000000000000000000000000000000002\t0\t1\ttrue",
		"0x0000000000000000000000000000000000000003\t1\t0\tfalse",
	}, lines)
}

// TestDumpStateToTSV_MatchesDumper pins our direct AccountsDomain scan against state.Dumper,
// the independent implementation behind debug_accountRange. Both must report the same
// accounts with the same balances and nonces.
func TestDumpStateToTSV_MatchesDumper(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	var buf bytes.Buffer
	res, err := dumpStateToTSV(context.Background(), tx, 1, false, 0, nil, &buf, log.New())
	require.NoError(t, err)

	reference := state.NewDumper(tx, rawdbv3.TxNums, 1).RawDump(true, true)
	require.Len(t, reference.Accounts, int(res.Matched))

	want := make([]string, 0, len(reference.Accounts))
	for addr, acc := range reference.Accounts {
		want = append(want, fmt.Sprintf("%s\t%s\t%d", strings.ToLower(addr.Hex()), acc.Balance, acc.Nonce))
	}

	got := make([]string, 0, res.Matched)
	for line := range strings.SplitSeq(strings.TrimRight(buf.String(), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		require.Len(t, fields, 4)
		got = append(got, strings.Join(fields[:3], "\t"))
	}

	require.ElementsMatch(t, want, got)
}

func TestDumpStateToTSV_DryRunWritesNothing(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	var buf bytes.Buffer
	res, err := dumpStateToTSV(context.Background(), tx, 1, true, 0, nil, &buf, log.New())
	require.NoError(t, err)

	require.Equal(t, uint64(3), res.Matched)
	require.Empty(t, buf.String())
}

func TestDumpStateToTSV_MinBalanceFilter(t *testing.T) {
	t.Parallel()
	tx := seedTestAccounts(t)

	var buf bytes.Buffer
	res, err := dumpStateToTSV(context.Background(), tx, 1, false, 0, uint256.NewInt(2), &buf, log.New())
	require.NoError(t, err)

	require.Equal(t, uint64(1), res.Matched)
	require.Equal(t, uint64(3), res.Scanned)
	require.Contains(t, buf.String(), "0x0000000000000000000000000000000000000001")
	require.NotContains(t, buf.String(), "0x0000000000000000000000000000000000000002")
	require.NotContains(t, buf.String(), "0x0000000000000000000000000000000000000003")
}
