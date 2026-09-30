package state

import "math"

// finalTxIdx is above any real tx index, so a prev-block layer reads a finalized
// block's map at its highest (final) write for each path — the committed state
// that block leaves behind.
const finalTxIdx = math.MaxInt

// layerVersionMaps layers read-only finalized versionMaps over base so a read
// resolves through each block's finalized state before falling through to base
// (the raw sd read). prevBlocks is ordered oldest→newest; the newest wraps
// outermost, so a key written by several blocks resolves to the most recent. This
// gives block N+1 a base reflecting earlier blocks not yet committed to the shared
// domain; the maps are read-only, so no coordination is needed.
func layerVersionMaps(base StateReader, prevBlocks []*VersionMap) StateReader {
	r := base
	for _, vm := range prevBlocks {
		if vm == nil {
			continue
		}
		// eip8246=false: this base-read layer sits beneath the fork-aware IBS field
		// path, which reconstructs the EIP-8246 balance-preserve itself. Only a
		// direct whole-account reader with no IBS above it needs that reconstruction.
		r = NewVersionedStateReader(finalTxIdx, ReadSet{}, vm, r, false)
	}
	return r
}
