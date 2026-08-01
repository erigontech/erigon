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

package commitment

import "github.com/erigontech/erigon/common"

// Witness capture for the binary trie. The tap sits in pbinHasher, not in the
// fold: sibling cells (hashRowCell) and the root cell (RootHash) are hashed
// outside foldBranch, and a fold-level tap would miss them.

// emitNode hands a node's consensus preimage and hash to the tracer. The
// preimage is pbinHasher's scratch buffer, overwritten by the next hash, so a
// tracer that keeps it must copy.
func (h *pbinHasher) emitNode(preimage []byte, hash *common.Hash) {
	if h.tracer == nil {
		return
	}
	h.tracer.onNode(preimage, hash[:])
}

// setWitnessTracer taps every node this engine hashes. Reset detaches, so it
// must be called after any reset and never survives into a pooled reuse.
func (pph *PBinPatriciaHashed) setWitnessTracer(tracer witnessTracer) {
	pph.hasher.tracer = tracer
}
