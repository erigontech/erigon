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

package example_extension

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestFileNameSuffixWhitelist_AcceptsAllowedSuffix(t *testing.T) {
	v := FileNameSuffixWhitelist{AllowedSuffixes: []string{".seg", ".kv"}}
	err := v.Validate(&snapshot.FileEntry{Name: "v1.0-headers.0-500.seg"}, nil)
	require.NoError(t, err)
}

func TestFileNameSuffixWhitelist_RejectsUnknownSuffix(t *testing.T) {
	v := FileNameSuffixWhitelist{AllowedSuffixes: []string{".seg", ".kv"}}
	err := v.Validate(&snapshot.FileEntry{Name: "mystery.dat"}, nil)
	require.Error(t, err)
	// Operator-readable: includes the rejected name AND the allowed set.
	require.Contains(t, err.Error(), "mystery.dat")
	require.Contains(t, err.Error(), ".seg")
	require.Contains(t, err.Error(), ".kv")
}

func TestExampleChain_BuiltinsRunAlongsideExtension(t *testing.T) {
	chain := ExampleChain()

	// A well-formed file passes every validator in the chain.
	good := &snapshot.FileEntry{
		Name:     "v1.0-accounts.0-256.kv",
		Domain:   snapshot.DomainAccounts,
		FromStep: 0,
		ToStep:   256,
	}
	require.NoError(t, chain.Validate(good, nil))

	// A file with an unknown suffix gets rejected by the extension —
	// the chain wraps the failing validator's Name in the error.
	bad := &snapshot.FileEntry{
		Name:     "v1.0-accounts.0-256.weird",
		Domain:   snapshot.DomainAccounts,
		FromStep: 0,
		ToStep:   256,
	}
	err := chain.Validate(bad, nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "filename_suffix_whitelist"),
		"chain must wrap the failing validator's Name; got %v", err)
}
