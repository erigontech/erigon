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
	"fmt"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Producer runs the full producer-side validation pipeline before a
// locally-built file becomes advertisable in the published V2 manifest.
//
// Two chains run in series:
//
//  1. The built-in chain — network-wide invariants every honest
//     producer must hold (NameNotEmpty, RangeOrdering, the eventual
//     hash-vs-torrent and content-shape checks). Failure here means
//     the file is malformed for the network and must NOT be
//     advertised.
//
//  2. The plugin chain — operator-defined gates layered on top.
//     Examples: deployment-private data-quality checks, workflow
//     signoffs ("retirement output must pass review process X"),
//     embargo rules. The plugin chain is a producer-only concern;
//     consumers ignore the plugin list entirely (per
//     feature-pluggable-validation-phase). Plugins fail-fast same as
//     built-ins.
//
// Producer is a verdict — it does NOT mutate the inventory. The
// caller flips Inventory.MarkAdvertisable on success. Decoupling
// keeps the verdict reusable (a CI tool can run Producer.Validate to
// pre-flight a file without an inventory in scope).
//
// Both fields are optional: a zero-valued Producer accepts
// everything. The default-on-bootstrap producer is
// validation.NewDefaultProducer() — built-in chain only, no
// plugins.
type Producer struct {
	// Chain is the built-in / network-invariant validators. Operators
	// who want to extend with deployment-specific checks add to
	// Plugins, not Chain.
	Chain Chain
	// Plugins is the operator-defined extension chain. Run after
	// Chain; same fail-fast semantics. Empty = no extra checks.
	Plugins Chain
}

// NewDefaultProducer returns a Producer wrapping DefaultStage1Chain
// with no plugins. Suitable for tests, harness, and the initial
// production hookup.
func NewDefaultProducer() *Producer {
	return &Producer{Chain: DefaultStage1Chain()}
}

// Validate runs the built-in chain then the plugin chain. Returns nil
// on accept; the first failure's wrapped error otherwise. The
// returned error names the chain (built-in vs plugin) so failures
// can be triaged without unwrapping.
func (p *Producer) Validate(file *snapshot.FileEntry, content ContentSource) error {
	if p == nil {
		return nil
	}
	if err := p.Chain.Validate(file, content); err != nil {
		return fmt.Errorf("producer built-in: %w", err)
	}
	if err := p.Plugins.Validate(file, content); err != nil {
		return fmt.Errorf("producer plugin: %w", err)
	}
	return nil
}

// MarkAdvertisable runs Validate and, on success, flips the
// inventory's Advertisable flag for file.Name. Returns the validation
// error on failure (inventory untouched). Returns the result of
// Inventory.MarkAdvertisable on success (true if the flag was newly
// set; false if it was already true OR the file is not in the
// inventory — caller decides which is an error).
//
// Convenience method for the common "validate + mark" sequence.
// Tests that want finer control call Validate + Inventory.MarkAdvertisable
// separately.
func (p *Producer) MarkAdvertisable(inv *snapshot.Inventory, file *snapshot.FileEntry, content ContentSource) (bool, error) {
	if inv == nil {
		return false, fmt.Errorf("nil inventory")
	}
	if file == nil {
		return false, fmt.Errorf("nil file entry")
	}
	if err := p.Validate(file, content); err != nil {
		return false, err
	}
	return inv.MarkAdvertisable(file.Name), nil
}
