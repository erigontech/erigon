package stagedsync

import "github.com/erigontech/erigon/execution/state"

// filterWritesByVersionMap keeps only the entries from collectorWrites
// whose (Address, Path, Key) also appear in vmWrites. CollectorWrites
// from the LightCollector emits ALL loaded accounts with ALL fields,
// but fields the TX didn't modify may carry stale values from
// speculative execution. The versionMap WriteSet contains only the
// (Address, Path, Key) tuples the TX's IBS actually wrote.
//
// applyVersionedWrites reads a base account from the cache/domain for
// any missing fields, so partial field sets are safe.
func filterWritesByVersionMap(collectorWrites, vmWrites *state.WriteSet) *state.WriteSet {
	if vmWrites.IsEmpty() {
		return collectorWrites
	}
	return collectorWrites.Filter(vmWrites.Has)
}
