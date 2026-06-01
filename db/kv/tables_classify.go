// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package kv

// inMemoryTables is the set of ChainDB tables routed to the in-memory
// (memstoredb) half of the hybrid backend when dbg.UseInMemoryKV is on. They
// hold the per-step history/index delta on top of the on-disk .kv snapshots,
// which the aggregator re-derives on startup from .kv files + the
// authoritative MDBX state. Domain *Vals tables (the current state of
// accounts, storage, code, commitment, receipts) stay in MDBX so a process
// restart on a fresh datadir — i.e. before the aggregator has had a chance
// to flush its first .kv step — still finds the predeploy contract code,
// genesis allocations, and so on.
//
// Tables NOT in this set stay in MDBX (block bodies, headers, receipts,
// senders, txlookup, stage progress, all *Vals tables, beacon, bor, config,
// …). They hold authoritative state that's expensive (or impossible on a
// fresh node) to re-derive.
var inMemoryTables = map[string]struct{}{
	// Account domain — history + index only; values stay in MDBX.
	TblAccountHistoryKeys: {},
	TblAccountHistoryVals: {},
	TblAccountIdx:         {},

	// Storage domain — history + index only.
	TblStorageHistoryKeys: {},
	TblStorageHistoryVals: {},
	TblStorageIdx:         {},

	// Code domain — history + index only.
	TblCodeHistoryKeys: {},
	TblCodeHistoryVals: {},
	TblCodeIdx:         {},

	// Commitment domain — history + index only.
	TblCommitmentHistoryKeys: {},
	TblCommitmentHistoryVals: {},
	TblCommitmentIdx:         {},

	// Receipt domain — history + index only.
	TblReceiptHistoryKeys: {},
	TblReceiptHistoryVals: {},
	TblReceiptIdx:         {},

	// Receipt cache — history + index only.
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
