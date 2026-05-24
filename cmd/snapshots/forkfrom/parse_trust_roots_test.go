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

package forkfrom

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validPubkeyHex(b byte) string {
	pk := make([]byte, 33)
	for i := range pk {
		pk[i] = b
	}
	return hex.EncodeToString(pk)
}

func TestParseValidParentTrustRoots_SingleEntry(t *testing.T) {
	pkHex := validPubkeyHex(0xab)
	got, err := parseValidParentTrustRoots("enr:" + pkHex)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "enr", got[0].Kind)
	decoded, _ := hex.DecodeString(pkHex)
	require.Equal(t, decoded, got[0].Pubkey)
	require.Empty(t, got[0].DID,
		"CLI parser doesn't populate DID — that's a JSON-config thing")
}

func TestParseValidParentTrustRoots_MultipleEntries(t *testing.T) {
	pk1 := validPubkeyHex(0x11)
	pk2 := validPubkeyHex(0x22)
	pk3 := validPubkeyHex(0x33)
	got, err := parseValidParentTrustRoots(
		"did:" + pk1 + ",enr:" + pk2 + ",bootnode:" + pk3,
	)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, "did", got[0].Kind)
	require.Equal(t, "enr", got[1].Kind)
	require.Equal(t, "bootnode", got[2].Kind)
}

func TestParseValidParentTrustRoots_LowercasesKind(t *testing.T) {
	pkHex := validPubkeyHex(0xcd)
	got, err := parseValidParentTrustRoots("ENR:" + strings.ToUpper(pkHex))
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "enr", got[0].Kind,
		"kind is canonicalised to lowercase")
}

func TestParseValidParentTrustRoots_TolersExtraWhitespace(t *testing.T) {
	pkHex := validPubkeyHex(0xef)
	got, err := parseValidParentTrustRoots("  enr:" + pkHex + "  ,  did:" + validPubkeyHex(0x12) + "  ")
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestParseValidParentTrustRoots_RejectsEmptyInput(t *testing.T) {
	_, err := parseValidParentTrustRoots("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty input")
}

func TestParseValidParentTrustRoots_RejectsMissingColon(t *testing.T) {
	// No "<kind>:" separator — invalid.
	_, err := parseValidParentTrustRoots(validPubkeyHex(0x11))
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected <kind>:<pubkey-hex>")
}

func TestParseValidParentTrustRoots_RejectsUnknownKind(t *testing.T) {
	_, err := parseValidParentTrustRoots("oops:" + validPubkeyHex(0x11))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown kind")
}

func TestParseValidParentTrustRoots_RejectsMalformedHex(t *testing.T) {
	_, err := parseValidParentTrustRoots("enr:not-hex-at-all")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pubkey hex decode")
}

func TestParseValidParentTrustRoots_RejectsWrongLengthPubkey(t *testing.T) {
	// 32 bytes (64 hex chars) instead of 33.
	short := hex.EncodeToString(make([]byte, 32))
	_, err := parseValidParentTrustRoots("enr:" + short)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pubkey length 32")
}

func TestParseValidParentTrustRoots_IgnoresEmptyComponents(t *testing.T) {
	// Trailing comma / double commas — accepted as empty components,
	// resulting list excludes them.
	pkHex := validPubkeyHex(0x77)
	got, err := parseValidParentTrustRoots("enr:" + pkHex + ",,")
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestParseValidParentTrustRoots_RejectsAllEmptyEntries(t *testing.T) {
	// Input of just commas / whitespace produces no entries; that's
	// an error so the operator notices they typed something bogus.
	_, err := parseValidParentTrustRoots(",,, ,")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no entries parsed")
}
