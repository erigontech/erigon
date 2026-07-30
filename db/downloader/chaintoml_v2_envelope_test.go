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

package downloader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParentEnvelope_NewComputesDigest pins the emitter-side digest
// computation. Emitters use New*Section; parser-side verifier must
// accept whatever New* produced.
func TestParentEnvelope_NewComputesDigest(t *testing.T) {
	yamlBody := "CONFIG_NAME: hoodi-fork-42\nPRESET_BASE: mainnet\nSECONDS_PER_SLOT: 12\n"
	cl := NewParentCLConfig([]byte(yamlBody))
	require.Equal(t, yamlBody, cl.YAML)
	require.Equal(t, envelopeDigest(yamlBody), cl.SHA256)

	jsonBody := `{"cut_block":3164608,"cut_block_hash":"0x1234"}`
	pc := NewParentCutSection([]byte(jsonBody))
	require.Equal(t, jsonBody, pc.JSON)
	require.Equal(t, envelopeDigest(jsonBody), pc.SHA256)
}

// TestParentEnvelope_ParseRoundTrip pins the emit → parse round-trip:
// a manifest with envelopes emitted via MarshalV2 must reparse via
// ParseV2 with the digests intact and no verification error.
func TestParentEnvelope_ParseRoundTrip(t *testing.T) {
	yamlBody := "CONFIG_NAME: hoodi-fork-42\nSECONDS_PER_SLOT: 12\n"
	jsonBody := `{"cut_block":3164608}`
	orig := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Parent: &ParentSection{
			Chain:                   "hoodi",
			ManifestHash:            "20004fef6f6b652bde5f7c20e67e33cbc3e059d3",
			CutBlock:                3164608,
			CutTxNum:                117629246,
			CutBlockHash:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			CLGenesisValidatorsRoot: "0000000000000000000000000000000000000000000000000000000000000000",
			CLForkVersion:           "10000910",
			CLConfig:                NewParentCLConfig([]byte(yamlBody)),
			ParentCut:               NewParentCutSection([]byte(jsonBody)),
		},
	}
	bytes, err := MarshalV2(orig)
	require.NoError(t, err)

	parsed, err := ParseV2(bytes)
	require.NoError(t, err)
	require.NotNil(t, parsed.Parent)
	require.NotNil(t, parsed.Parent.CLConfig)
	require.Equal(t, yamlBody, parsed.Parent.CLConfig.YAML)
	require.Equal(t, envelopeDigest(yamlBody), parsed.Parent.CLConfig.SHA256)
	require.NotNil(t, parsed.Parent.ParentCut)
	require.Equal(t, jsonBody, parsed.Parent.ParentCut.JSON)
	require.Equal(t, envelopeDigest(jsonBody), parsed.Parent.ParentCut.SHA256)
}

// TestParentEnvelope_ParseRejectsTamperedYAML pins the sha256 gate:
// swap the YAML body without updating the digest, expect ParseV2 to
// reject at the verifyParentEnvelopeDigests pass. Without this,
// a peer could ship a chain.toml whose UCAN-attested digest matches
// bytes different from what a fork-follower would actually run.
func TestParentEnvelope_ParseRejectsTamperedYAML(t *testing.T) {
	orig := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Parent: &ParentSection{
			Chain:                   "hoodi",
			ManifestHash:            "20004fef6f6b652bde5f7c20e67e33cbc3e059d3",
			CutBlock:                1,
			CutTxNum:                1,
			CutBlockHash:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			CLGenesisValidatorsRoot: "0000000000000000000000000000000000000000000000000000000000000000",
			CLForkVersion:           "10000910",
			CLConfig:                NewParentCLConfig([]byte("CONFIG_NAME: original\n")),
		},
	}
	// Tamper: overwrite YAML without recomputing SHA256.
	orig.Parent.CLConfig.YAML = "CONFIG_NAME: tampered\n"
	bytes, err := MarshalV2(orig)
	require.NoError(t, err)

	_, err = ParseV2(bytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sha256 mismatch")
	require.Contains(t, err.Error(), "[parent.cl_config]")
}

// TestParentEnvelope_ParseRejectsTamperedJSON — same as YAML but on
// the parent-cut envelope.
func TestParentEnvelope_ParseRejectsTamperedJSON(t *testing.T) {
	orig := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Parent: &ParentSection{
			Chain:                   "hoodi",
			ManifestHash:            "20004fef6f6b652bde5f7c20e67e33cbc3e059d3",
			CutBlock:                1,
			CutTxNum:                1,
			CutBlockHash:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			CLGenesisValidatorsRoot: "0000000000000000000000000000000000000000000000000000000000000000",
			CLForkVersion:           "10000910",
			ParentCut:               NewParentCutSection([]byte(`{"cut_block":1}`)),
		},
	}
	orig.Parent.ParentCut.JSON = `{"cut_block":999}`
	bytes, err := MarshalV2(orig)
	require.NoError(t, err)

	_, err = ParseV2(bytes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sha256 mismatch")
	require.Contains(t, err.Error(), "[parent.parent_cut]")
}

// TestParentEnvelope_ParseRejectsUnpaired covers the two "half-filled
// envelope" shapes: sha256 without body, and body without sha256.
// Both are shapes a mis-written emitter could produce; parser must
// reject rather than silently accept the well-formed half.
func TestParentEnvelope_ParseRejectsUnpaired(t *testing.T) {
	base := func() *ChainTomlV2 {
		return &ChainTomlV2{
			Version: ChainTomlV2Version,
			Parent: &ParentSection{
				Chain:                   "hoodi",
				ManifestHash:            "20004fef6f6b652bde5f7c20e67e33cbc3e059d3",
				CutBlock:                1,
				CutTxNum:                1,
				CutBlockHash:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				CLGenesisValidatorsRoot: "0000000000000000000000000000000000000000000000000000000000000000",
				CLForkVersion:           "10000910",
			},
		}
	}

	// sha256 declared, body empty.
	orig := base()
	orig.Parent.CLConfig = &ParentCLConfig{YAML: "", SHA256: envelopeDigest("something")}
	bytes, err := MarshalV2(orig)
	require.NoError(t, err)
	_, err = ParseV2(bytes)
	require.ErrorContains(t, err, "sha256 declared but body empty")

	// Body present, sha256 empty.
	orig = base()
	orig.Parent.CLConfig = &ParentCLConfig{YAML: "CONFIG_NAME: x\n", SHA256: ""}
	bytes, err = MarshalV2(orig)
	require.NoError(t, err)
	_, err = ParseV2(bytes)
	require.ErrorContains(t, err, "body present but sha256 empty")
}

// TestParentEnvelope_BackCompat_V2WithoutEnvelopes pins the v2 back-
// compat contract: a manifest without either envelope section parses
// clean and downstream consumers see CLConfig/ParentCut as nil (the
// signal to fall back to on-disk cl-config.<fork>.yaml + parent-cut
// .<fork>.json). Guards against a future refactor that would silently
// populate empty envelopes and break the fallback.
func TestParentEnvelope_BackCompat_V2WithoutEnvelopes(t *testing.T) {
	orig := &ChainTomlV2{
		Version: ChainTomlV2Version,
		Parent: &ParentSection{
			Chain:                   "hoodi",
			ManifestHash:            "20004fef6f6b652bde5f7c20e67e33cbc3e059d3",
			CutBlock:                1,
			CutTxNum:                1,
			CutBlockHash:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			CLGenesisValidatorsRoot: "0000000000000000000000000000000000000000000000000000000000000000",
			CLForkVersion:           "10000910",
		},
	}
	bytes, err := MarshalV2(orig)
	require.NoError(t, err)

	parsed, err := ParseV2(bytes)
	require.NoError(t, err)
	require.NotNil(t, parsed.Parent)
	require.Nil(t, parsed.Parent.CLConfig, "no cl_config → nil, signals fall-back to on-disk yaml")
	require.Nil(t, parsed.Parent.ParentCut, "no parent_cut → nil, signals fall-back to on-disk json")
}
