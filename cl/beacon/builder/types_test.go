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

package builder

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// Bids from the ethereum/builder-specs examples. Their signatures are placeholders.
var (
	//go:embed test_data/signed_builder_bid_bellatrix.json
	bellatrixBidJSON []byte
	//go:embed test_data/signed_builder_bid_capella.json
	capellaBidJSON []byte
	//go:embed test_data/signed_builder_bid_deneb.json
	denebBidJSON []byte
	//go:embed test_data/signed_builder_bid_electra.json
	electraBidJSON []byte
)

// The expected roots are computed with github.com/attestantio/go-builder-client BuilderBid.HashTreeRoot and the
// mainnet builder domain 0x00000001f5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a9.
var builderBidTestCases = []struct {
	version        clparams.StateVersion
	bid            []byte
	messageRoot    common.Hash
	mainnetSigning common.Hash
}{
	{clparams.BellatrixVersion, bellatrixBidJSON, common.HexToHash("ab8bd7908353ca1353b96dafd78b99cd079f921d4dbff93998628dd2c19ce01d"), common.HexToHash("5b7edbaf65ee4da3bf84d7be4c34bd699805e18179520c4c4bf24fe3db95019f")},
	{clparams.CapellaVersion, capellaBidJSON, common.HexToHash("2e7b3f03e05aae1e336654e584ad94b13941f3381532e4ddfa1c730240fdd578"), common.HexToHash("809f0d22c705ef33dc61c2f55598bf308c6f4787043766dcb92cb0d0095bb149")},
	{clparams.DenebVersion, denebBidJSON, common.HexToHash("b8badff2ce16663a6a3100943d008c6a64d996ca6f7b4310a4c690015b3fb587"), common.HexToHash("d53726153d7c685cb08a22bf87f3617ce7b4806b3c54486829ef07a9f507536e")},
	{clparams.ElectraVersion, electraBidJSON, common.HexToHash("85d585608310deb5a25300a11ce447fb023dff00b3589623731d0995ecc3da25"), common.HexToHash("ece57448ac121a7365a104f000d8a26c99d4dea80316d13e1b0db8b6dfd9e4a0")},
	{clparams.FuluVersion, electraBidJSON, common.HexToHash("85d585608310deb5a25300a11ce447fb023dff00b3589623731d0995ecc3da25"), common.HexToHash("ece57448ac121a7365a104f000d8a26c99d4dea80316d13e1b0db8b6dfd9e4a0")},
}

func decodeBuilderBidForTest(t *testing.T, cfg *clparams.BeaconChainConfig, version clparams.StateVersion, raw []byte) *ExecutionHeader {
	t.Helper()
	header := &ExecutionHeader{Data: ExecutionHeaderData{Message: ExecutionHeaderMessage{
		Header:             cltypes.NewEth1Header(version),
		ExecutionRequests:  cltypes.NewExecutionRequestsWithVersion(cfg, version),
		BlobKzgCommitments: solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}}}
	require.NoError(t, json.Unmarshal(raw, header))
	return header
}

func TestBuilderBidSigningRootMatchesSpec(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for _, tc := range builderBidTestCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			header := decodeBuilderBidForTest(t, cfg, tc.version, tc.bid)

			messageRoot, err := builderBid{message: &header.Data.Message, value: header.BlockValue(), version: tc.version}.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, tc.messageRoot, common.Hash(messageRoot))

			signingRoot, err := header.SigningRoot(cfg, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.mainnetSigning, common.Hash(signingRoot))
		})
	}
}

func TestBuilderBidVerifySignature(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	// A non-zero genesis fork version tells it apart from the zero genesis validators root.
	cfg.GenesisForkVersion = 0x10000910
	key, err := bls.NewPrivateKeyFromIKM(bytes.Repeat([]byte{0x42}, 32))
	require.NoError(t, err)
	otherKey, err := bls.NewPrivateKeyFromIKM(bytes.Repeat([]byte{0x43}, 32))
	require.NoError(t, err)

	sign := func(t *testing.T, header *ExecutionHeader, version clparams.StateVersion, signer *bls.PrivateKey, domainType [4]byte, forkVersion uint32, genesisValidatorsRoot common.Hash) {
		t.Helper()
		copy(header.Data.Message.PubKey[:], bls.CompressPublicKey(key.PublicKey()))
		domain, err := fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(forkVersion), genesisValidatorsRoot)
		require.NoError(t, err)
		root, err := fork.ComputeSigningRoot(builderBid{message: &header.Data.Message, value: header.BlockValue(), version: version}, domain)
		require.NoError(t, err)
		copy(header.Data.Signature[:], signer.Sign(root[:]).Bytes())
	}
	genesisForkVersion := uint32(cfg.GenesisForkVersion)

	for _, tc := range builderBidTestCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			for _, sc := range []struct {
				name  string
				setup func(t *testing.T, header *ExecutionHeader)
				valid bool
			}{
				{
					name: "valid",
					setup: func(t *testing.T, header *ExecutionHeader) {
						sign(t, header, tc.version, key, cfg.DomainApplicationBuilder, genesisForkVersion, common.Hash{})
					},
					valid: true,
				},
				{
					name:  "placeholder signature",
					setup: func(*testing.T, *ExecutionHeader) {},
				},
				{
					name: "signed by another key",
					setup: func(t *testing.T, header *ExecutionHeader) {
						sign(t, header, tc.version, otherKey, cfg.DomainApplicationBuilder, genesisForkVersion, common.Hash{})
					},
				},
				{
					name: "value changed after signing",
					setup: func(t *testing.T, header *ExecutionHeader) {
						sign(t, header, tc.version, key, cfg.DomainApplicationBuilder, genesisForkVersion, common.Hash{})
						header.Data.Message.Value = "2"
					},
				},
				{
					name: "header changed after signing",
					setup: func(t *testing.T, header *ExecutionHeader) {
						sign(t, header, tc.version, key, cfg.DomainApplicationBuilder, genesisForkVersion, common.Hash{})
						header.Data.Message.Header.BlockHash[0] ^= 0xff
					},
				},
				{
					name: "beacon proposer domain",
					setup: func(t *testing.T, header *ExecutionHeader) {
						sign(t, header, tc.version, key, cfg.DomainBeaconProposer, genesisForkVersion, common.Hash{})
					},
				},
				{
					name: "current fork version",
					setup: func(t *testing.T, header *ExecutionHeader) {
						sign(t, header, tc.version, key, cfg.DomainApplicationBuilder, cfg.GetForkVersionByVersion(tc.version), common.Hash{})
					},
				},
				{
					name: "non-zero genesis validators root",
					setup: func(t *testing.T, header *ExecutionHeader) {
						sign(t, header, tc.version, key, cfg.DomainApplicationBuilder, genesisForkVersion, common.Hash{0x01})
					},
				},
			} {
				t.Run(sc.name, func(t *testing.T) {
					header := decodeBuilderBidForTest(t, &cfg, tc.version, tc.bid)
					sc.setup(t, header)

					err := header.VerifySignature(&cfg, tc.version)

					if sc.valid {
						require.NoError(t, err)
						return
					}
					require.ErrorIs(t, err, ErrInvalidBidSignature)
				})
			}
		})
	}
}

func TestBuilderBidSigningRootRejectsMalformedBid(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for _, tc := range []struct {
		name    string
		version clparams.StateVersion
		mutate  func(*ExecutionHeader)
		wantErr string
	}{
		{"invalid value", clparams.ElectraVersion, func(h *ExecutionHeader) { h.Data.Message.Value = "-1" }, "invalid builder block value"},
		{"missing header", clparams.ElectraVersion, func(h *ExecutionHeader) { h.Data.Message.Header = nil }, "missing execution payload header"},
		{"missing commitments", clparams.DenebVersion, func(h *ExecutionHeader) { h.Data.Message.BlobKzgCommitments = nil }, "missing blob KZG commitments"},
		{"missing execution requests", clparams.ElectraVersion, func(h *ExecutionHeader) { h.Data.Message.ExecutionRequests = nil }, "missing execution requests"},
		{"gloas", clparams.GloasVersion, func(*ExecutionHeader) {}, "no builder API bid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := decodeBuilderBidForTest(t, cfg, clparams.ElectraVersion, electraBidJSON)
			tc.mutate(header)

			_, err := header.SigningRoot(cfg, tc.version)

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestBlockValueRejectsOversizedInputBeforeParsing(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Root().GetHandler()
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previous) })

	value := strings.Repeat("9", 2<<20)
	header := ExecutionHeader{Data: ExecutionHeaderData{Message: ExecutionHeaderMessage{Value: value}}}
	start := time.Now()

	require.Nil(t, header.BlockValue())
	require.Less(t, time.Since(start), time.Second)
	require.Less(t, logs.Len(), 1024)
}
