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

package validation

import (
	"context"
	"fmt"
)

// BatchValidator runs across a whole set of files (a generation, a
// retire-cycle output, the entire inventory) rather than per-file.
// The integration point is a "post-batch hook" — after a producer
// step that mints N candidate files, the BatchChain runs ONCE across
// the batch before the producer-side gate flips Advertisable on any
// of them.
//
// The existing db/integrity checks (CheckBlocks, CheckCommitmentKvi,
// CheckInvertedIndex, CheckPublishable, etc.) match this shape
// naturally — they walk a domain or sample from it and return a
// verdict. The bridge that adapts those checks lives in
// node/components/storage (which already imports db/integrity); the
// validation framework stays format-agnostic and never imports
// db/integrity itself, preserving the layering.
//
// Per-file Validator (the existing Validator interface) and
// BatchValidator are complementary, not alternatives. Per-file
// validators run at MarkAdvertisable on the candidate file's
// metadata + content; batch validators run when a coherent set of
// files needs to be verified together (cross-file consistency that
// only makes sense across the whole batch — commitment chaining,
// publishability of the whole snapshot, etc.).
type BatchValidator interface {
	// Name is a stable identifier (used in error wrapping, log
	// messages, and future PeerPenalized event payloads).
	Name() string
	// ValidateBatch runs the check. The validator pulls whatever
	// store-side context it needs from its own constructor — the
	// interface stays parameter-free so the validation package
	// doesn't need to model storage internals.
	ValidateBatch(ctx context.Context) error
}

// BatchChain runs a sequence of BatchValidators in order, returning
// the first failure with the validator's name wrapped around the
// underlying error. An empty chain accepts everything.
//
// Same fail-fast semantics as Chain (the per-file equivalent).
type BatchChain []BatchValidator

// Validate runs every validator in chain order. Stops on first
// failure. The returned error is wrapped with the failing
// validator's Name so callers can attribute the rejection without
// unwrapping.
func (c BatchChain) Validate(ctx context.Context) error {
	for _, v := range c {
		if err := v.ValidateBatch(ctx); err != nil {
			return fmt.Errorf("%s: %w", v.Name(), err)
		}
	}
	return nil
}
