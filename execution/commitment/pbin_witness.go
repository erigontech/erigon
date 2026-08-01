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

import (
	"bytes"
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
)

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

// pbinWitnessReadOnly drops the branch writes a fold makes on its way up. The
// witness pass folds rows it never modified, so writing them back would rewrite
// stored records under this pass's empty touch map.
type pbinWitnessReadOnly struct{ PatriciaContext }

func (pbinWitnessReadOnly) PutBranch(prefix, data, prevData []byte) error { return nil }

// Code forwards the code seam the update stream reaches for by type assertion;
// without it the wrapper would hide the wrapped context's own Code.
func (c pbinWitnessReadOnly) Code(plainKey []byte) ([]byte, error) {
	inner, ok := c.PatriciaContext.(pbinCodeContext)
	if !ok {
		return nil, fmt.Errorf("%w: %T serves no code", ErrPBinUnsupported, c.PatriciaContext)
	}
	return inner.Code(plainKey)
}

// Witnesses walks the tree along every key the update stream expands to, taps
// each node as it is hashed, and returns the captured superset (root first), the
// keys walked, and the root hash. Callers prune to the lean set.
//
// No update is applied: the caller checks the returned root against the parent
// block's, so it must be the pre-state one.
//
// produceExclusionProofs is accepted and ignored. It materializes the branch an
// extension node hides, and EIP-8297 has no extension node.
func (pph *PBinPatriciaHashed) Witnesses(ctx context.Context, updates *Updates, produceExclusionProofs bool, logPrefix string) (nodes [][]byte, provedKeys [][]byte, rootHash []byte, err error) {
	set := newWitnessNodeSet()
	pph.setWitnessTracer(set)
	defer pph.setWitnessTracer(nil)

	stateCtx := pph.ctx
	pph.ctx = pbinWitnessReadOnly{PatriciaContext: stateCtx}
	defer func() { pph.ctx = stateCtx }()

	pph.lastKeyLen = 0
	provedKeys = make([][]byte, 0, updates.Size())
	// The proved keys are the stream's, not HashSort's: one account touch expands
	// into a BASIC_DATA leaf, a CODE_HASH leaf and one leaf per code chunk, and
	// only the sink sees all of them.
	_, err = pph.updateStream.process(ctx, updates, pph.ctx, func(treeKey, _ []byte, _ *Update) error {
		provedKeys = append(provedKeys, bytes.Clone(treeKey))
		_, err := pph.seek(treeKey)
		return err
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("pbin: witness %s: %w", logPrefix, err)
	}
	for pph.grid.activeRows > 0 {
		if err = pph.fold(); err != nil {
			return nil, nil, nil, fmt.Errorf("pbin: witness final fold: %w", err)
		}
	}
	if rootHash, err = pph.RootHash(); err != nil {
		return nil, nil, nil, err
	}
	if nodes, err = set.nodes(rootHash); err != nil {
		return nil, nil, nil, err
	}
	return nodes, provedKeys, rootHash, nil
}
