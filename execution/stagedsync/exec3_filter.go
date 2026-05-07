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
func filterWritesByVersionMap(collectorWrites, vmWrites state.VersionedWrites) state.VersionedWrites {
	if len(vmWrites) == 0 {
		return collectorWrites
	}

	type pathKey struct {
		path state.AccountPath
		key  [32]byte
	}
	vmSet := make(map[[20]byte]map[pathKey]struct{}, len(vmWrites))
	for _, w := range vmWrites {
		addrVal := w.Address.Value()
		pk := pathKey{path: w.Path, key: w.Key.Value()}
		if m, ok := vmSet[addrVal]; ok {
			m[pk] = struct{}{}
		} else {
			vmSet[addrVal] = map[pathKey]struct{}{pk: {}}
		}
	}

	filtered := make(state.VersionedWrites, 0, len(collectorWrites))
	for _, w := range collectorWrites {
		addrVal := w.Address.Value()
		pk := pathKey{path: w.Path, key: w.Key.Value()}
		if m, ok := vmSet[addrVal]; ok {
			if _, ok := m[pk]; ok {
				filtered = append(filtered, w)
			}
		}
	}
	return filtered
}
