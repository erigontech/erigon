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

package flow

import "testing"

func TestFileRole(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// State files: the role names the index, not just the extension.
		// Files the inventory cannot attribute to a domain all share the
		// blocks bucket, so a role of "ef" alone makes every standalone
		// index look like coverage of every other at the same step range,
		// and only the first-sorted one is ever requested.
		{"kv domain file", "v1.0-accounts.0-256.kv", "accounts.kv"},
		{"kvi domain file", "v1.0-accounts.0-256.kvi", "accounts.kvi"},
		{"storage kv", "v1.0-storage.0-512.kv", "storage.kv"},
		{"logaddrs ef", "v3.0-logaddrs.330-331.ef", "logaddrs.ef"},
		{"logtopics ef", "v3.0-logtopics.330-331.ef", "logtopics.ef"},
		{"logaddrs efi accessor", "accessor/v2.1-logaddrs.330-331.efi", "logaddrs.efi"},
		{"rcache efi accessor", "accessor/v2.0-rcache.330-331.efi", "rcache.efi"},
		{"rcache history", "history/v3.1-rcache.330-331.v", "rcache.v"},
		{"v4 txnum-named kv", "v4.0-code.126593796-126953125.kv", "code.kv"},

		// Block files: alphabetic segment before extension is part of role.
		{"headers seg", "v1.0-000000-000500-headers.seg", "headers.seg"},
		{"bodies seg", "v1.0-000000-000500-bodies.seg", "bodies.seg"},
		{"headers idx", "v1.0-000000-000500-headers.idx", "headers.idx"},

		// Edge cases.
		{"no extension", "salt-blocks", "salt-blocks"},
		{"only extension", "thing.kv", "kv"},
		{"dot but no dash", "salt.txt", "txt"},
		{"empty string", "", ""},
		{"extension is empty after dot", "weird.", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := fileRole(tc.in)
			if got != tc.want {
				t.Errorf("fileRole(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsAlphabetic(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"headers", true},
		{"Headers", true},
		{"MixedCase", true},
		{"has-dash", false},
		{"has digit 0", false},
		{"256", false},
		{"accounts1", false},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := isAlphabetic(tc.in)
			if got != tc.want {
				t.Errorf("isAlphabetic(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
