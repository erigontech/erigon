package state

import "math"

// finalTxIdx is above any real tx index, so a prev-block layer reads a finalized
// block's map at its highest (final) write for each path — the committed state
// that block leaves behind.
const finalTxIdx = math.MaxInt

// layerVersionMaps layers a list of read-only finalized versionMaps over base so
// a committed read resolves through each block's finalized state before falling
// through to base (the raw sd read). prevBlocks is ordered oldest→newest; the
// newest wraps outermost, so a key written by several blocks resolves to the most
// recent. Each layer reads its map at finalTxIdx (the block's sealed final state)
// and is a plain versionedStateReader read-through — the same machinery intra-
// block reads use — so no new composition logic is introduced.
//
// This gives block N+1 a base that reflects earlier blocks whose writes have not
// yet committed to the shared domain. The maps are read-only (finished) here, so
// the layering needs no coordination; a map whose block has since committed is
// only ever redundant with the base, never wrong.
func layerVersionMaps(base StateReader, prevBlocks []*VersionMap) StateReader {
	r := base
	for _, vm := range prevBlocks {
		if vm == nil {
			continue
		}
		r = NewVersionedStateReader(finalTxIdx, ReadSet{}, vm, r)
	}
	return r
}
