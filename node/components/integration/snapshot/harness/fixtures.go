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

package harness

import (
	"fmt"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Fixture describes an inventory state used to drive a test scenario.
type Fixture struct {
	Name    string
	Domains map[snapshot.Domain][]*snapshot.FileEntry
	Blocks  []*snapshot.FileEntry
}

// Apply populates inv with the fixture's file entries.
func (f *Fixture) Apply(inv *snapshot.Inventory) {
	for _, entries := range f.Domains {
		for _, e := range entries {
			inv.AddFile(e)
		}
	}
	for _, e := range f.Blocks {
		inv.AddFile(e)
	}
}

// HoodiBaseline returns a procedural fixture approximating a fully-merged
// Hoodi archive snapshot tree. Each domain has a single merged file covering
// steps [0, 256), plus a handful of block snapshot files.
func HoodiBaseline() *Fixture {
	mkDomain := func(d snapshot.Domain, ext string, from, to uint64, trust snapshot.TrustLevel) *snapshot.FileEntry {
		return &snapshot.FileEntry{
			Domain:   d,
			FromStep: from,
			ToStep:   to,
			Name:     fmt.Sprintf("v1.0-%s.%d-%d.%s", d, from, to, ext),
			Size:     1024,
			Local:    true,
			Trust:    trust,
		}
	}
	mkBlock := func(name string, from, to uint64) *snapshot.FileEntry {
		return &snapshot.FileEntry{
			FromStep: from,
			ToStep:   to,
			Name:     name,
			Size:     2048,
			Local:    true,
			Trust:    snapshot.TrustVerified,
		}
	}
	return &Fixture{
		Name: "hoodi_baseline",
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainAccounts: {
				mkDomain(snapshot.DomainAccounts, "kv", 0, 256, snapshot.TrustVerified),
				mkDomain(snapshot.DomainAccounts, "kvi", 0, 256, snapshot.TrustVerified),
			},
			snapshot.DomainStorage: {
				mkDomain(snapshot.DomainStorage, "kv", 0, 256, snapshot.TrustVerified),
				mkDomain(snapshot.DomainStorage, "kvi", 0, 256, snapshot.TrustVerified),
			},
			snapshot.DomainCode: {
				mkDomain(snapshot.DomainCode, "kv", 0, 256, snapshot.TrustVerified),
			},
			snapshot.DomainCommitment: {
				mkDomain(snapshot.DomainCommitment, "kv", 0, 256, snapshot.TrustVerified),
			},
		},
		Blocks: []*snapshot.FileEntry{
			mkBlock("v1.0-000000-000500-headers.seg", 0, 500),
			mkBlock("v1.0-000000-000500-bodies.seg", 0, 500),
			mkBlock("v1.0-000500-001000-headers.seg", 500, 1000),
			mkBlock("v1.0-000500-001000-bodies.seg", 500, 1000),
		},
	}
}

// HoodiBaselineMixedTrust is HoodiBaseline with accounts domain at TrustNone
// (as if downloaded from an unverified peer). Used to exercise
// CoverageAtTrust filtering.
func HoodiBaselineMixedTrust() *Fixture {
	f := HoodiBaseline()
	f.Name = "hoodi_baseline_mixed_trust"
	for _, e := range f.Domains[snapshot.DomainAccounts] {
		e.Trust = snapshot.TrustNone
	}
	return f
}
