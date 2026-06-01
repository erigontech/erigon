// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package kv

// inMemoryTables is the set of ChainDB tables routed to the in-memory
// (memstoredb) half of the hybrid backend when dbg.UseInMemoryKV is on. They
// are the temporal/domain delta on top of the on-disk .kv snapshots, which
// the aggregator re-derives on startup from .kv files + the authoritative
// MDBX state.
//
// Aggressive partition: Domain *Vals tables (current state of accounts,
// storage, code, commitment, receipts) are also in memory. On a tip-running
// node the .kv snapshots already hold the bulk of state so cold reads
// fall through to .kv via the aggregator. On a fresh datadir without .kv
// snapshots a restart loses *Vals — known limitation,
// TestFcuReturnsReorgTooDeepCode38006 (predeploy contract code missing
// after close-reopen on fresh genesis).
var inMemoryTables = map[string]struct{}{
	// Account domain — values + history + index.
	TblAccountVals:        {},
	TblAccountHistoryKeys: {},
	TblAccountHistoryVals: {},
	TblAccountIdx:         {},

	// Storage domain — values + history + index.
	TblStorageVals:        {},
	TblStorageHistoryKeys: {},
	TblStorageHistoryVals: {},
	TblStorageIdx:         {},

	// Code domain — values + history + index.
	TblCodeVals:        {},
	TblCodeHistoryKeys: {},
	TblCodeHistoryVals: {},
	TblCodeIdx:         {},

	// Commitment domain — values + history + index.
	TblCommitmentVals:        {},
	TblCommitmentHistoryKeys: {},
	TblCommitmentHistoryVals: {},
	TblCommitmentIdx:         {},

	// Receipt domain — values + history + index.
	TblReceiptVals:        {},
	TblReceiptHistoryKeys: {},
	TblReceiptHistoryVals: {},
	TblReceiptIdx:         {},

	// Receipt cache — values + history + index.
	TblRCacheVals:        {},
	TblRCacheHistoryKeys: {},
	TblRCacheHistoryVals: {},
	TblRCacheIdx:         {},

	// Inverted indices over log addresses / topics / traces.
	TblLogAddressKeys: {},
	TblLogAddressIdx:  {},
	TblLogTopicsKeys:  {},
	TblLogTopicsIdx:   {},
	TblTracesFromKeys: {},
	TblTracesFromIdx:  {},
	TblTracesToKeys:   {},
	TblTracesToIdx:    {},

	// Aggregator pruning progress markers — recomputable.
	TblPruningProgress: {},
	TblPruningValsProg: {},

	// Transient "this header hash is bad" markers.
	BadHeaderNumber: {},

	// ChangeSets3 — the per-step changeset feed; aggregator-managed delta.
	ChangeSets3: {},
}

// IsInMemoryTable reports whether the named ChainDB table is part of the
// in-memory half of the hybrid backend. Used by db/kv/hybridkv to dispatch
// per-table operations. Stable identity-by-name; safe to call before the
// hybrid backend is constructed.
func IsInMemoryTable(name string) bool {
	_, ok := inMemoryTables[name]
	return ok
}

// InMemoryTables returns a fresh slice of in-memory ChainDB table names.
// Order is unspecified.
func InMemoryTables() []string {
	out := make([]string, 0, len(inMemoryTables))
	for n := range inMemoryTables {
		out = append(out, n)
	}
	return out
}
