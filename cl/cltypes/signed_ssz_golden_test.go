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

package cltypes_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

// Golden tests pinning the exact SSZ encoding and hash tree root of every
// signed container type ({Message, Signature} pair). These types cross the
// network, so any refactor of their SSZ methods must stay byte-identical.

type signedContainerSSZ interface {
	EncodeSSZ([]byte) ([]byte, error)
	DecodeSSZ([]byte, int) error
	EncodingSizeSSZ() int
	HashSSZ() ([32]byte, error)
}

func requireGoldenSSZ(t *testing.T, obj signedContainerSSZ, wantEncHex, wantRootHex string, wantSize int) []byte {
	t.Helper()
	enc, err := obj.EncodeSSZ(nil)
	require.NoError(t, err)
	root, err := obj.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantEncHex, hex.EncodeToString(enc))
	require.Equal(t, wantRootHex, hex.EncodeToString(root[:]))
	require.Equal(t, wantSize, obj.EncodingSizeSSZ())
	return enc
}

func requireSSZRoundTrip(t *testing.T, dec signedContainerSSZ, enc []byte, version clparams.StateVersion, wantRootHex string) {
	t.Helper()
	require.NoError(t, dec.DecodeSSZ(enc, int(version)))
	reenc, err := dec.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, enc, reenc)
	root, err := dec.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantRootHex, hex.EncodeToString(root[:]))
}

func patBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

func patHash(seed byte) (h common.Hash)       { copy(h[:], patBytes(32, seed)); return }
func patAddress(seed byte) (a common.Address) { copy(a[:], patBytes(20, seed)); return }
func patBytes48(seed byte) (b common.Bytes48) { copy(b[:], patBytes(48, seed)); return }
func patBytes96(seed byte) (b common.Bytes96) { copy(b[:], patBytes(96, seed)); return }

func TestSignedVoluntaryExitSSZGolden(t *testing.T) {
	const (
		wantEnc  = "181716151413121128272625242322213132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f90"
		wantRoot = "f7c4afc8ffc3f698628a2d0dc77a12e6f2d3ba63bc90ac3b39053c455a5d386b"
		wantSize = 112
	)
	obj := &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{
			Epoch:          0x1112131415161718,
			ValidatorIndex: 0x2122232425262728,
		},
		Signature: patBytes96(0x31),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)
	requireSSZRoundTrip(t, &cltypes.SignedVoluntaryExit{}, enc, clparams.Phase0Version, wantRoot)
}

func TestSignedBeaconBlockHeaderSSZGolden(t *testing.T) {
	const (
		wantEnc  = "080706050403020118171615141312112122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f4022232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f4041232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f4041424142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0"
		wantRoot = "fc32cf8253ad44adc094412642b7f924aea4b67f988b82b1b3824f93dbee26b3"
		wantSize = 208
	)
	obj := &cltypes.SignedBeaconBlockHeader{
		Header: &cltypes.BeaconBlockHeader{
			Slot:          0x0102030405060708,
			ProposerIndex: 0x1112131415161718,
			ParentRoot:    patHash(0x21),
			Root:          patHash(0x22),
			BodyRoot:      patHash(0x23),
		},
		Signature: patBytes96(0x41),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)
	requireSSZRoundTrip(t, &cltypes.SignedBeaconBlockHeader{}, enc, clparams.Phase0Version, wantRoot)
}

func TestSignedBLSToExecutionChangeSSZGolden(t *testing.T) {
	const (
		wantEnc  = "28272625242322215152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f806162636465666768696a6b6c6d6e6f70717273747172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0"
		wantRoot = "3ef1e68971de9ac8b053d6e545cab6db895d8c5d0f79751a3789ee93b03bca87"
		wantSize = 172
	)
	obj := &cltypes.SignedBLSToExecutionChange{
		Message: &cltypes.BLSToExecutionChange{
			ValidatorIndex: 0x2122232425262728,
			From:           patBytes48(0x51),
			To:             patAddress(0x61),
		},
		Signature: patBytes96(0x71),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)
	requireSSZRoundTrip(t, &cltypes.SignedBLSToExecutionChange{}, enc, clparams.CapellaVersion, wantRoot)
}

func TestSignedContributionAndProofSSZGolden(t *testing.T) {
	const (
		wantEnc  = "383736353433323148474645444342418182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa00a000000000000009192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff00b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f10c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
		wantRoot = "9baea36083648238f3584861724f5dd876209361376ac98974c259be06823f94"
		wantSize = 360
	)
	obj := &cltypes.SignedContributionAndProof{
		Message: &cltypes.ContributionAndProof{
			AggregatorIndex: 0x3132333435363738,
			Contribution: &cltypes.Contribution{
				Slot:              0x4142434445464748,
				BeaconBlockRoot:   patHash(0x81),
				SubcommitteeIndex: 0x0a,
				AggregationBits:   patBytes(cltypes.DefaultSyncCommitteeAggregationBitsSize, 0x91),
				Signature:         patBytes96(0xa1),
			},
			SelectionProof: patBytes96(0xb1),
		},
		Signature: patBytes96(0xc1),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)
	requireSSZRoundTrip(t, &cltypes.SignedContributionAndProof{}, enc, clparams.AltairVersion, wantRoot)
}

func TestSignedAggregateAndProofSSZGolden(t *testing.T) {
	const (
		wantEnc  = "640000003132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f9058575655545352516c0000002122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f80e400000068676665646362610b00000000000000d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff00c00000000000000e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000d00000000000000f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70aabbcc2d"
		wantRoot = "6d149aca2f316705051778de27c1ae274b318bf9d2980430d5a3c56b35b0463e"
		wantSize = 440
	)
	obj := &cltypes.SignedAggregateAndProof{
		Message: &cltypes.AggregateAndProof{
			AggregatorIndex: 0x5152535455565758,
			Aggregate: &solid.Attestation{
				AggregationBits: solid.BitlistFromBytes([]byte{0xaa, 0xbb, 0xcc, 0x2d}, 2048),
				Data: &solid.AttestationData{
					Slot:            0x6162636465666768,
					CommitteeIndex:  0x0b,
					BeaconBlockRoot: patHash(0xd1),
					Source:          solid.Checkpoint{Epoch: 0x0c, Root: patHash(0xe1)},
					Target:          solid.Checkpoint{Epoch: 0x0d, Root: patHash(0xf1)},
				},
				Signature: patBytes96(0x11),
			},
			SelectionProof: patBytes96(0x21),
		},
		Signature: patBytes96(0x31),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)
	requireSSZRoundTrip(t, &cltypes.SignedAggregateAndProof{}, enc, clparams.DenebVersion, wantRoot)
}

func TestSignedProposerPreferencesSSZGolden(t *testing.T) {
	const (
		wantEnc  = "15161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333478777675747372710e0000000000000025262728292a2b2c2d2e2f3031323334353637380f0000000000000035363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f9091929394"
		wantRoot = "593a67daddfee5a7ef1ff405c073f32ff95a6f6397e3f35239100dd0ca9ad6c7"
		wantSize = 172
	)
	obj := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			DependentRoot:  patHash(0x15),
			ProposalSlot:   0x7172737475767778,
			ValidatorIndex: 0x0e,
			FeeRecipient:   patAddress(0x25),
			TargetGasLimit: 0x0f,
		},
		Signature: patBytes96(0x35),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)
	requireSSZRoundTrip(t, &cltypes.SignedProposerPreferences{}, enc, clparams.GloasVersion, wantRoot)

	require.Equal(t, 96, (&cltypes.SignedProposerPreferences{}).EncodingSizeSSZ())
}

func TestSignedExecutionPayloadBidSSZGolden(t *testing.T) {
	const (
		wantEnc  = "64000000c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b888878685848382811000000000000000110000000000000012000000000000001300000000000000e0000000b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f8081828384"
		wantRoot = "83f2e9dcb83b1c43e13aa7cd46c2251938cd9b2cec71e243ed97d384ef85f3d2"
		wantSize = 420
	)
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
	var c1, c2 cltypes.KZGCommitment
	copy(c1[:], patBytes(48, 0x45))
	copy(c2[:], patBytes(48, 0x55))
	commitments.Append(&c1)
	commitments.Append(&c2)
	obj := &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			ParentBlockHash:       patHash(0x65),
			ParentBlockRoot:       patHash(0x75),
			BlockHash:             patHash(0x85),
			PrevRandao:            patHash(0x95),
			FeeRecipient:          patAddress(0xa5),
			GasLimit:              0x8182838485868788,
			BuilderIndex:          0x10,
			Slot:                  0x11,
			Value:                 0x12,
			ExecutionPayment:      0x13,
			BlobKzgCommitments:    *commitments,
			ExecutionRequestsRoot: patHash(0xb5),
		},
		Signature: patBytes96(0xc5),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)
	requireSSZRoundTrip(t, &cltypes.SignedExecutionPayloadBid{}, enc, clparams.GloasVersion, wantRoot)
}

func TestSignedExecutionPayloadEnvelopeSSZGolden(t *testing.T) {
	const (
		wantEnc  = "64000000f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f50515253545550000000b10200002000000000000000d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f1011121314161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f50515253545514000000000000001500000000000000160000000000000017000000000000001c020000565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485210200002c0200001b000000000000001c00000000000000580200001d00000000000000464748494a04000000767778797a7b7c18000000000000001900000000000000868788898a8b8c8d8e8f909192939495969798991a00000000000000969798999a9b9c9d9e0c000000cc000000cc000000a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d51e00000000000000c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021222324251f00000000000000"
		wantRoot = "b600054b8896b2498137587aeac1f0698f5a71d3214e85bedf19ed605a4f89e4"
		wantSize = 981
	)
	cfg := &clparams.MainnetBeaconConfig

	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.ParentHash = patHash(0xd5)
	payload.FeeRecipient = patAddress(0xe5)
	payload.StateRoot = patHash(0xf5)
	payload.ReceiptsRoot = patHash(0x16)
	copy(payload.LogsBloom[:], patBytes(256, 0x26))
	payload.PrevRandao = patHash(0x36)
	payload.BlockNumber = 0x14
	payload.GasLimit = 0x15
	payload.GasUsed = 0x16
	payload.Time = 0x17
	payload.Extra = solid.NewExtraData()
	payload.Extra.SetBytes(patBytes(5, 0x46))
	payload.BaseFeePerGas = patHash(0x56)
	payload.BlockHash = patHash(0x66)
	payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{patBytes(7, 0x76)})
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.Withdrawals.Append(&cltypes.Withdrawal{
		Index:     0x18,
		Validator: 0x19,
		Address:   patAddress(0x86),
		Amount:    0x1a,
	})
	payload.BlobGasUsed = 0x1b
	payload.ExcessBlobGas = 0x1c
	require.NoError(t, payload.BlockAccessList.SetBytes(patBytes(9, 0x96)))
	payload.SlotNumber = 0x1d

	requests := cltypes.NewExecutionRequests(cfg)
	requests.Deposits.Append(&solid.DepositRequest{
		PubKey:                patBytes48(0xa6),
		WithdrawalCredentials: patHash(0xb6),
		Amount:                0x1e,
		Signature:             patBytes96(0xc6),
		Index:                 0x1f,
	})

	obj := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload:               payload,
			ExecutionRequests:     requests,
			BuilderIndex:          0x20,
			BeaconBlockRoot:       patHash(0xd6),
			ParentBeaconBlockRoot: patHash(0xe6),
		},
		Signature: patBytes96(0xf6),
	}
	enc := requireGoldenSSZ(t, obj, wantEnc, wantRoot, wantSize)

	dec := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	requireSSZRoundTrip(t, dec, enc, clparams.GloasVersion, wantRoot)
}
