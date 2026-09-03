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

package blockmetrics

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

type captureHandler struct{ msgs []string }

func (h *captureHandler) Log(r *log.Record) error {
	h.msgs = append(h.msgs, r.Msg)
	return nil
}

func (h *captureHandler) Enabled(context.Context, log.Lvl) bool { return true }

func captureLogger() (log.Logger, *captureHandler) {
	h := &captureHandler{}
	l := log.New()
	l.SetHandler(h)
	return l, h
}

func sampleRecord() *Record {
	return &Record{
		Number:    1234,
		Hash:      common.HexToHash("0xabc"),
		GasUsed:   30_000_000,
		TxCount:   7,
		Execution: 40 * time.Millisecond,
		StateHash: 50 * time.Millisecond,
		Commit:    10 * time.Millisecond,
	}
}

func TestEmitThreshold(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		threshold time.Duration
		wantEmit  bool
	}{
		{"disabled suppresses", Disabled, false},
		{"negative suppresses", -5 * time.Second, false},
		{"zero emits every block", 0, true},
		{"below total emits", 99 * time.Millisecond, true},
		{"equal to total emits", 100 * time.Millisecond, true},
		{"above total suppresses", 101 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, h := captureLogger()
			Emit(logger, tc.threshold, sampleRecord())
			if tc.wantEmit {
				require.Len(t, h.msgs, 1)
			} else {
				assert.Empty(t, h.msgs)
			}
		})
	}
}

func TestEmitNilsAreNoOps(t *testing.T) {
	t.Parallel()
	logger, h := captureLogger()
	Emit(logger, 0, nil)
	Emit(nil, 0, sampleRecord())
	assert.Empty(t, h.msgs)
}

func TestEmitJSONSchema(t *testing.T) {
	t.Parallel()
	logger, h := captureLogger()

	rec := sampleRecord()
	rec.CountersValid = true
	rec.Accounts = DomainCounts{Reads: 11, Writes: 3, CacheHits: 8, CacheMiss: 2, ReadTime: 4 * time.Millisecond}
	rec.Storage = DomainCounts{Reads: 20, Writes: 5, CacheHits: 15, CacheMiss: 5, ReadTime: 6 * time.Millisecond}
	rec.Code = DomainCounts{Reads: 2, Writes: 1, CacheHits: 1, CacheMiss: 1, ReadTime: time.Millisecond}

	Emit(logger, 0, rec)
	require.Len(t, h.msgs, 1)

	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(h.msgs[0]), &got))

	assert.Equal(t, "warn", got["level"])
	assert.Equal(t, "Slow block", got["msg"])

	block := got["block"].(map[string]any)
	assert.Equal(t, float64(1234), block["number"])
	assert.Equal(t, float64(30_000_000), block["gas_used"])
	assert.Equal(t, float64(7), block["tx_count"])
	assert.Contains(t, block, "hash")

	timing := got["timing"].(map[string]any)
	assert.Equal(t, float64(40), timing["execution_ms"])
	assert.Equal(t, float64(50), timing["state_hash_ms"])
	assert.Equal(t, float64(10), timing["commit_ms"])
	assert.Equal(t, float64(11), timing["state_read_ms"])
	assert.Equal(t, float64(100), timing["total_ms"])

	assert.InDelta(t, 30.0/0.040, got["throughput"].(map[string]any)["mgas_per_sec"], 1e-6)

	reads := got["state_reads"].(map[string]any)
	assert.Equal(t, float64(11), reads["accounts"])
	assert.Equal(t, float64(20), reads["storage_slots"])
	assert.Equal(t, float64(2), reads["code"])

	writes := got["state_writes"].(map[string]any)
	assert.Equal(t, float64(3), writes["accounts"])
	assert.Equal(t, float64(5), writes["storage_slots"])

	account := got["cache"].(map[string]any)["account"].(map[string]any)
	assert.Equal(t, float64(8), account["hits"])
	assert.Equal(t, float64(2), account["misses"])
	assert.InDelta(t, 80.0, account["hit_rate"], 1e-9)
}

func TestEmitOmitsCountersWhenNotCollected(t *testing.T) {
	t.Parallel()
	logger, h := captureLogger()

	rec := sampleRecord()
	rec.CountersValid = false
	Emit(logger, 0, rec)
	require.Len(t, h.msgs, 1)

	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(h.msgs[0]), &got))

	assert.NotContains(t, got, "state_reads")
	assert.NotContains(t, got, "state_writes")
	assert.NotContains(t, got, "cache")
	assert.Contains(t, got, "timing")
}

func TestHitRateWithoutAccesses(t *testing.T) {
	t.Parallel()
	assert.Zero(t, DomainCounts{}.entry().HitRate)
}

func TestMgasPerSecWithoutTime(t *testing.T) {
	t.Parallel()
	rec := &Record{GasUsed: 1_000_000}
	assert.Zero(t, rec.mgasPerSec())
}

func TestStateReadIsInsideExecution(t *testing.T) {
	t.Parallel()

	rec := &Record{Execution: 10 * time.Millisecond, StateHash: 5 * time.Millisecond}
	rec.Accounts = DomainCounts{ReadTime: 3 * time.Millisecond}
	rec.Storage = DomainCounts{ReadTime: 2 * time.Millisecond}

	assert.Equal(t, 5*time.Millisecond, rec.StateRead())
	assert.Equal(t, 15*time.Millisecond, rec.Total(), "reads are already inside execution; adding them double-counts")
}

func TestDiffCountsStateCacheHitsAsReads(t *testing.T) {
	t.Parallel()

	before := kvmetrics.DomainIOMetrics{}
	after := kvmetrics.DomainIOMetrics{
		CacheReadCount:      2, // sd.mem hits
		DbReadCount:         3,
		FileReadCount:       1,
		StateCacheHitCount:  5,
		StateCacheMissCount: 4, // already inside DbReadCount+FileReadCount
	}

	got := diff(before, after)
	assert.Equal(t, int64(11), got.Reads, "2 mem + 3 db + 1 file + 5 stateCache hits")
	assert.Equal(t, int64(5), got.CacheHits)
	assert.Equal(t, int64(4), got.CacheMiss)
}

func TestDiffAbsorbsCounterReset(t *testing.T) {
	t.Parallel()

	before := kvmetrics.DomainIOMetrics{CacheReadCount: 100, CachePutCount: 20, StateCacheHitCount: 7}
	got := diff(before, kvmetrics.DomainIOMetrics{})

	assert.Zero(t, got.Reads)
	assert.Zero(t, got.Writes)
	assert.Zero(t, got.CacheHits)
}

func TestSinceRequiresBothSamples(t *testing.T) {
	t.Parallel()

	_, _, _, ok := Sample{}.Since(Sample{})
	assert.False(t, ok, "an untaken sample must not report zero counts as real")

	taken := Sample{taken: true}
	_, _, _, ok = taken.Since(Sample{})
	assert.False(t, ok, "a missing baseline must not be treated as zero")

	_, _, _, ok = Sample{}.Since(taken)
	assert.False(t, ok)

	_, _, _, ok = taken.Since(taken)
	assert.True(t, ok)
}

func TestTakeIsInertWithoutReadMetrics(t *testing.T) {
	prev := dbg.KVReadLevelledMetrics
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prev })

	dbg.KVReadLevelledMetrics = false
	assert.False(t, Take(kvmetrics.NewDomainMetrics(), kvmetrics.NewDomainMetrics()).taken,
		"the read path skips the counters without the gate, so every delta would be a false zero")

	dbg.EnableKVReadLevelledMetrics()
	assert.True(t, Take(kvmetrics.NewDomainMetrics(), kvmetrics.NewDomainMetrics()).taken)

	assert.False(t, Take(nil, nil).taken)
}

func domainWithReads(domain kv.Domain, count int64, d time.Duration) *kvmetrics.DomainMetrics {
	dm := kvmetrics.NewDomainMetrics()
	dm.Domains[domain] = &kvmetrics.DomainIOMetrics{DbReadCount: count, DbReadDuration: d}
	return dm
}

func TestCommitmentReadsAreNotExecutionReads(t *testing.T) {
	prev := dbg.KVReadLevelledMetrics
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prev })
	dbg.EnableKVReadLevelledMetrics()

	total := kvmetrics.NewDomainMetrics()
	nonExec := kvmetrics.NewDomainMetrics()
	before := Take(total, nonExec)

	total.Merge(domainWithReads(kv.AccountsDomain, 10, 10*time.Millisecond))
	nonExec.Merge(domainWithReads(kv.AccountsDomain, 4, 4*time.Millisecond))

	accounts, _, _, ok := Take(total, nonExec).Since(before)
	require.True(t, ok)
	assert.Equal(t, int64(6), accounts.Reads, "commitment's reads must not be charged to execution")
	assert.Equal(t, 6*time.Millisecond, accounts.ReadTime)
}

func TestExecOnlyClampsInsteadOfGoingNegative(t *testing.T) {
	t.Parallel()

	got := execOnly(
		DomainCounts{Reads: 1, ReadTime: time.Millisecond},
		DomainCounts{Reads: 5, ReadTime: 5 * time.Millisecond},
	)
	assert.Zero(t, got.Reads)
	assert.Zero(t, got.ReadTime)
}

// The console rendering is a cross-client contract, like the field names.
var harnessPattern = regexp.MustCompile(
	`^\[(?:TRACE|DBUG|INFO|WARN|EROR|CRIT)\] \[[^\]]+\] (\{.+\})\s*$`)

func TestEmittedLineMatchesTheHarnessContract(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&buf, log.TerminalFormatNoColor()))
	Emit(logger, 0, sampleRecord())

	line := buf.String()
	require.Equal(t, 1, strings.Count(line, "\n"), "the record must occupy a single line")

	m := harnessPattern.FindStringSubmatch(strings.TrimSuffix(line, "\n"))
	require.Len(t, m, 2, "console line does not match the parser contract: %q", line)

	var probe struct {
		Msg   string `json:"msg"`
		Block struct {
			Hash string `json:"hash"`
		} `json:"block"`
	}
	require.NoError(t, json.Unmarshal([]byte(m[1]), &probe))

	assert.Equal(t, "Slow block", probe.Msg)
	assert.NotEmpty(t, probe.Block.Hash)
}
