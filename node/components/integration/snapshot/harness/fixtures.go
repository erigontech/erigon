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

// FileCount returns the total number of file entries across all domains and
// block snapshots.
func (f *Fixture) FileCount() int {
	total := len(f.Blocks)
	for _, entries := range f.Domains {
		total += len(entries)
	}
	return total
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

// HoodiMerged500 represents a fully-merged archive covering steps [0, 500):
// every domain has a single merged file per role. Used as "peer with the
// merge completed" in merge-divergence scenarios.
func HoodiMerged500() *Fixture {
	return mergedFixture("hoodi_merged_500", 0, 500)
}

// HoodiUnmerged500 covers the same range [0, 500) as HoodiMerged500 but as
// five unmerged 100-step files per domain role (.kv and .kvi). Other domains
// stay merged to keep the fixture focused — only the accounts domain is
// unmerged, since that is what the three-node divergence test exercises.
func HoodiUnmerged500() *Fixture {
	f := mergedFixture("hoodi_unmerged_500", 0, 500)
	f.Domains[snapshot.DomainAccounts] = unmergedSlices(snapshot.DomainAccounts, 0, 500, 100)
	return f
}

// mergedFixture builds a fixture where every domain is a single merged file
// per role over [from, to).
func mergedFixture(name string, from, to uint64) *Fixture {
	mkDomain := func(d snapshot.Domain, ext string) *snapshot.FileEntry {
		return &snapshot.FileEntry{
			Domain:   d,
			FromStep: from,
			ToStep:   to,
			Name:     fmt.Sprintf("v1.0-%s.%d-%d.%s", d, from, to, ext),
			Size:     1024,
			Local:    true,
			Trust:    snapshot.TrustVerified,
		}
	}
	mkBlock := func(kind string, blkFrom, blkTo uint64) *snapshot.FileEntry {
		return &snapshot.FileEntry{
			FromStep: blkFrom,
			ToStep:   blkTo,
			Name:     fmt.Sprintf("v1.0-%06d-%06d-%s.seg", blkFrom, blkTo, kind),
			Size:     2048,
			Local:    true,
			Trust:    snapshot.TrustVerified,
		}
	}
	return &Fixture{
		Name: name,
		Domains: map[snapshot.Domain][]*snapshot.FileEntry{
			snapshot.DomainAccounts: {
				mkDomain(snapshot.DomainAccounts, "kv"),
				mkDomain(snapshot.DomainAccounts, "kvi"),
			},
			snapshot.DomainStorage: {
				mkDomain(snapshot.DomainStorage, "kv"),
				mkDomain(snapshot.DomainStorage, "kvi"),
			},
			snapshot.DomainCode:       {mkDomain(snapshot.DomainCode, "kv")},
			snapshot.DomainCommitment: {mkDomain(snapshot.DomainCommitment, "kv")},
		},
		Blocks: []*snapshot.FileEntry{
			mkBlock("headers", 0, 500),
			mkBlock("bodies", 0, 500),
		},
	}
}

// unmergedSlices builds a series of small .kv / .kvi entries covering
// [from, to) in `step`-sized slices, approximating a domain whose merge
// cycle has not yet collapsed them.
func unmergedSlices(d snapshot.Domain, from, to, step uint64) []*snapshot.FileEntry {
	entries := make([]*snapshot.FileEntry, 0, 2*((to-from)/step))
	for lo := from; lo < to; lo += step {
		hi := lo + step
		if hi > to {
			hi = to
		}
		for _, ext := range []string{"kv", "kvi"} {
			entries = append(entries, &snapshot.FileEntry{
				Domain:   d,
				FromStep: lo,
				ToStep:   hi,
				Name:     fmt.Sprintf("v1.0-%s.%d-%d.%s", d, lo, hi, ext),
				Size:     256,
				Local:    true,
				Trust:    snapshot.TrustVerified,
			})
		}
	}
	return entries
}
