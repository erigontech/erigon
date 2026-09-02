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

// Package blockmetrics emits per-block execution metrics as JSON. The field
// names are a cross-client contract — renaming one breaks every consumer.
// Spec: https://ethresear.ch/t/unifying-execution-layer-execution-metrics/22089
package blockmetrics

import (
	"encoding/json"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const Disabled = time.Duration(-1)

type slowBlockLog struct {
	Level      string          `json:"level"`
	Msg        string          `json:"msg"`
	Block      blockInfo       `json:"block"`
	Timing     timing          `json:"timing"`
	Throughput throughput      `json:"throughput"`
	StateReads *stateReads     `json:"state_reads,omitempty"`
	StateWrite *stateWrites    `json:"state_writes,omitempty"`
	Cache      *cacheSummaries `json:"cache,omitempty"`
}

type blockInfo struct {
	Number  uint64      `json:"number"`
	Hash    common.Hash `json:"hash"`
	GasUsed uint64      `json:"gas_used"`
	TxCount int         `json:"tx_count"`
}

type timing struct {
	ExecutionMs float64 `json:"execution_ms"`
	StateReadMs float64 `json:"state_read_ms"`
	StateHashMs float64 `json:"state_hash_ms"`
	CommitMs    float64 `json:"commit_ms"`
	TotalMs     float64 `json:"total_ms"`
}

type throughput struct {
	MgasPerSec float64 `json:"mgas_per_sec"`
}

type stateReads struct {
	Accounts     int64 `json:"accounts"`
	StorageSlots int64 `json:"storage_slots"`
	Code         int64 `json:"code"`
}

type stateWrites struct {
	Accounts     int64 `json:"accounts"`
	StorageSlots int64 `json:"storage_slots"`
	Code         int64 `json:"code"`
}

type cacheSummaries struct {
	Account cacheEntry `json:"account"`
	Storage cacheEntry `json:"storage"`
	Code    cacheEntry `json:"code"`
}

type cacheEntry struct {
	Hits    int64   `json:"hits"`
	Misses  int64   `json:"misses"`
	HitRate float64 `json:"hit_rate"`
}

type DomainCounts struct {
	Reads     int64
	Writes    int64
	CacheHits int64
	CacheMiss int64
	ReadTime  time.Duration
}

type Record struct {
	Number  uint64
	Hash    common.Hash
	GasUsed uint64
	TxCount int

	Execution time.Duration
	StateHash time.Duration
	Commit    time.Duration

	Accounts DomainCounts
	Storage  DomainCounts
	Code     DomainCounts

	// Separates "nothing was read" from "nothing was counted": an absent source
	// must not chart as zero cost.
	CountersValid bool
}

// StateRead is the part of Execution that went to domain reads. It is a
// breakdown of Execution, not a sibling of it, so Total does not add it —
// reth reports the same way (crates/engine/tree/src/tree/mod.rs, "state_read_duration
// is already included in execution_duration"). Geth instead subtracts reads
// from its execution figure, so the two clients disagree about what
// execution_ms means; this follows reth, because a subtraction that can go
// negative is a number that silently absorbs whatever else went missing.
func (r *Record) StateRead() time.Duration {
	return r.Accounts.ReadTime + r.Storage.ReadTime + r.Code.ReadTime
}

func (r *Record) Total() time.Duration {
	return r.Execution + r.StateHash + r.Commit
}

func (r *Record) mgasPerSec() float64 {
	if r.Execution <= 0 {
		return 0
	}
	return (float64(r.GasUsed) / 1e6) / r.Execution.Seconds()
}

func (d DomainCounts) entry() cacheEntry {
	e := cacheEntry{Hits: d.CacheHits, Misses: d.CacheMiss}
	if total := d.CacheHits + d.CacheMiss; total > 0 {
		e.HitRate = 100 * float64(d.CacheHits) / float64(total)
	}
	return e
}

func ms(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

func Emit(logger log.Logger, threshold time.Duration, r *Record) {
	if logger == nil || r == nil || threshold < 0 {
		return
	}
	if threshold > 0 && r.Total() < threshold {
		return
	}

	entry := slowBlockLog{
		Level: "warn",
		Msg:   "Slow block",
		Block: blockInfo{
			Number:  r.Number,
			Hash:    r.Hash,
			GasUsed: r.GasUsed,
			TxCount: r.TxCount,
		},
		Timing: timing{
			ExecutionMs: ms(r.Execution),
			StateReadMs: ms(r.StateRead()),
			StateHashMs: ms(r.StateHash),
			CommitMs:    ms(r.Commit),
			TotalMs:     ms(r.Total()),
		},
		Throughput: throughput{MgasPerSec: r.mgasPerSec()},
	}

	if r.CountersValid {
		entry.StateReads = &stateReads{
			Accounts:     r.Accounts.Reads,
			StorageSlots: r.Storage.Reads,
			Code:         r.Code.Reads,
		}
		entry.StateWrite = &stateWrites{
			Accounts:     r.Accounts.Writes,
			StorageSlots: r.Storage.Writes,
			Code:         r.Code.Writes,
		}
		entry.Cache = &cacheSummaries{
			Account: r.Accounts.entry(),
			Storage: r.Storage.entry(),
			Code:    r.Code.entry(),
		}
	}

	encoded, err := json.Marshal(entry)
	if err != nil {
		logger.Error("marshal slow block metrics", "err", err, "block", r.Number)
		return
	}
	logger.Warn(string(encoded))
}
