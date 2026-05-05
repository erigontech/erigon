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

package snapshot

// StepBlockBoundary records the canonical (step, block) pair derived
// from a commitment-domain file's recorded state. The binding is
// produced by Stage 2 batch validation (see
// docs/plans/20260504-step-and-minimum-unified.md): when a commitment
// step batch validates, the validator opens commitment.kv, decodes
// KeyCommitmentState, and registers `(toStep, blockNum)` here.
//
// Consumers query the binding to map block ranges (used by block
// snapshot file names) into step units, giving uniform step semantics
// across block + state files.
type StepBlockBoundary struct {
	// ToStep is the exclusive upper bound of the commitment step that
	// recorded this state — i.e. the file's ToStep field. The state
	// recorded at this boundary is the state AFTER all blocks in
	// step range [FromStep, ToStep).
	ToStep uint64

	// Block is the canonical block number whose end-state is recorded
	// at this step boundary. Read from the commitment file's
	// KeyCommitmentState value (HexTrieExtractStateRoot).
	Block uint64
}

// RegisterStepBlockBoundary records a (step, block) binding produced
// by a commitment-domain batch validation. Idempotent: re-registering
// the same step with the same block is a no-op; re-registering with a
// different block overwrites (the latest validation wins, since
// retire-merge cycles can replace a step's commitment file).
func (inv *Inventory) RegisterStepBlockBoundary(toStep, block uint64) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.stepBlockBoundaries == nil {
		inv.stepBlockBoundaries = make(map[uint64]uint64)
	}
	inv.stepBlockBoundaries[toStep] = block
}

// BlockToStep returns the smallest registered ToStep whose block
// boundary is >= the given block number. In other words: the step
// whose end-state covers (or immediately follows) block. Returns
// (0, false) if no boundary covers block — typically because the
// relevant commitment file hasn't been validated yet.
//
// Used by callers that have a block number (e.g. parsed from a block
// snapshot file's name) and want to map it into step units. The
// mapping is monotonic in block: a later block maps to a later (or
// equal) step.
func (inv *Inventory) BlockToStep(block uint64) (uint64, bool) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if len(inv.stepBlockBoundaries) == 0 {
		return 0, false
	}
	var bestStep uint64
	var bestBlock uint64
	found := false
	for step, b := range inv.stepBlockBoundaries {
		if b < block {
			continue
		}
		if !found || step < bestStep || (step == bestStep && b < bestBlock) {
			bestStep = step
			bestBlock = b
			found = true
		}
	}
	return bestStep, found
}

// StepBlockBoundaries returns a snapshot of all registered bindings
// in ascending step order. Useful for diagnostics / metrics.
func (inv *Inventory) StepBlockBoundaries() []StepBlockBoundary {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	if len(inv.stepBlockBoundaries) == 0 {
		return nil
	}
	out := make([]StepBlockBoundary, 0, len(inv.stepBlockBoundaries))
	for step, block := range inv.stepBlockBoundaries {
		out = append(out, StepBlockBoundary{ToStep: step, Block: block})
	}
	// Sort ascending by ToStep — helper rather than callers
	// re-sorting each query.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].ToStep > out[j].ToStep; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
