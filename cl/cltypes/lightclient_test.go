package cltypes_test

import (
	"testing"

	_ "embed"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

//go:embed tests/light_client_bootstrap.ssz_snappy
var bootstrapTest []byte

//go:embed tests/light_client_optimistic.ssz_snappy
var optimisticTest []byte

//go:embed tests/light_client_finality.ssz_snappy
var finalityTest []byte

//go:embed tests/light_client_update.ssz_snappy
var updateTest []byte

func TestLightclientBootstrapEncodingDecoding(t *testing.T) {
	lc := cltypes.LightClientBootstrap{}
	decodedSSZ, err := utils.DecompressSnappy(bootstrapTest)
	require.NoError(t, err)
	require.NoError(t, lc.DecodeSSZWithVersion(decodedSSZ, int(clparams.CapellaVersion)))
	// checking fields
	require.Equal(t, lc.Header.HeaderEth2.BodyRoot, libcommon.HexToHash("0x9eb9f39c2e88739dc4274483d1e9072c3685864d5bcbf2f3c072b97f54536a4e"))
	require.Equal(t, lc.Header.HeaderEth1.WithdrawalsRoot, libcommon.HexToHash("0xd327e2d43fedf327de8f01ab8bbf4fbdbfececc890c5059ad53cfb37264cc03c"))
	require.Equal(t, lc.Header.ExecutionBranch[0], libcommon.HexToHash("0x4138a3e27e9aea5c1c9eb311a7889bfddeb3d2ad3987943d68ee7cc659b6d947"))
	require.Equal(t, lc.CurrentSyncCommittee.PubKeys[0][:], common.Hex2Bytes("009c6eacc6e3f29b10763a446fc6a6dfff95b408fb44c92b453e1349572d2b45b77305668349d39360e775752a4faf9a"))
	require.Equal(t, lc.CurrentSyncCommittee.PubKeys[len(lc.CurrentSyncCommittee.PubKeys)-1][:], common.Hex2Bytes("dce9afb56fe779662a206c7d1503efacaf9d62fe81fa79d1fbfb9fd8ff03f7a11f263be6e4df633094cdfebcbd50112c"))
	require.Equal(t, lc.CurrentSyncCommittee.AggregatePublicKey[:], common.Hex2Bytes("7455aa9ac6113a04b6976796d95dc2e4d7435ca37f47b9463b10fbaa32759db32ff9f7c4920c67d461ac1e4ebca56bb4"))
	require.Equal(t, lc.CurrentSyncCommitteeBranch[0], libcommon.HexToHash("0x93a080a9f03a19868f6293f18236b8203f7ade39b78043184ada44b6b363ecfb"))
	// encoding it back
	dec, err := lc.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, dec, decodedSSZ)
}

func TestLightclientOptimisticEncodingDecoding(t *testing.T) {
	lc := cltypes.LightClientOptimisticUpdate{}
	decodedSSZ, err := utils.DecompressSnappy(optimisticTest)
	require.NoError(t, err)
	require.NoError(t, lc.DecodeSSZWithVersion(decodedSSZ, int(clparams.CapellaVersion)))
	// checking fields
	require.Equal(t, lc.AttestedHeader.HeaderEth2.BodyRoot, libcommon.HexToHash("0x95e145e5890ccf3bf6c1c3708a51d42d7c602312d0ac2b225a1d54e30649e2f6"))
	require.Equal(t, lc.AttestedHeader.HeaderEth1.WithdrawalsRoot, libcommon.HexToHash("0xdc96edf3b75ae19c608c0d5f821c3e71a577f310bf5da42bf51293394205b282"))
	require.Equal(t, lc.AttestedHeader.ExecutionBranch[0], libcommon.HexToHash("0x9e29304aab0d41643d772d076861e0c951c5212f92e2c8b7788ee79c7a03c045"))
	require.Equal(t, lc.SignatureSlot, uint64(6292665015452153680))
	require.Equal(t, lc.SyncAggregate.SyncCommiteeSignature[:], common.Hex2Bytes("60ac5c67c7a2f460b665a3a5f618d3b8e2356a2338279e2d24134795398e286ac0299cd6367986266f073c3a6b400f7d62a586e9687e7aace3dede4172677c6af2515338ceb3591a86bdae014c3cc057ffbcc1afe4127e8b55ad508ebcead13a"))
	// encoding it back
	dec, err := lc.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, dec, decodedSSZ)
}

func TestLightclientFinalityEncodingDecoding(t *testing.T) {
	lc := cltypes.LightClientFinalityUpdate{}
	decodedSSZ, err := utils.DecompressSnappy(finalityTest)
	require.NoError(t, err)
	require.NoError(t, lc.DecodeSSZWithVersion(decodedSSZ, int(clparams.CapellaVersion)))
	// checking fields
	require.Equal(t, lc.AttestedHeader.HeaderEth2.BodyRoot, libcommon.HexToHash("0xcc67d01f9d529e16333170f366defcc0bb8fbd0a9b84bd13d787bbcf86fe3cc5"))
	require.Equal(t, lc.AttestedHeader.HeaderEth1.WithdrawalsRoot, libcommon.HexToHash("0x28ce6c427881a19a7aaf0d2f8255e30376ee19193cd0278244141e1a66f3c45b"))
	require.Equal(t, lc.AttestedHeader.ExecutionBranch[0], libcommon.HexToHash("0x3e85d631970da85df7a6dab6c62c198d0a291e891d5a897bafc87bcfd2312cf6"))
	// Finalized header
	require.Equal(t, lc.FinalizedHeader.HeaderEth2.BodyRoot, libcommon.HexToHash("0xbaa18d70c7a36d7e3af79bf0874b902a173d83daad5c56f83ede72788abda4e8"))
	require.Equal(t, lc.FinalizedHeader.HeaderEth1.WithdrawalsRoot, libcommon.HexToHash("0x778c759c4dc95d80121cc56338c0e8fa657d3a39b6cfa67eec5ec471de48fa7e"))
	require.Equal(t, lc.FinalizedHeader.ExecutionBranch[0], libcommon.HexToHash("0x103bab66f63d57f6158eef6f18c9ffcc6e9b2e2593731a58001fc991c39ac618"))
	// Rest
	require.Equal(t, lc.FinalityBranch[0], libcommon.HexToHash("838ab6e8d817f3d454a69e79a435cca780df0e5b1e62971df7fec176e57f7f37"))
	require.Equal(t, lc.SignatureSlot, uint64(9588297899091073326))
	require.Equal(t, lc.SyncAggregate.SyncCommiteeSignature[:], common.Hex2Bytes("8eb0eb4c085e840235f15086287f62815607f2aac486f07b78a8b41ec6d0baee8b34f664b8efdee877080b44e3104792fe90dce4a211e9e58abeeefcb67b4918c61e01950b927ad25a988ebf52761c6c1d26eae4f501238359aac17361d6c3ae"))
	// encoding it back
	dec, err := lc.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, dec, decodedSSZ)
}

func TestLightclientUpdateEncodingDecoding(t *testing.T) {
	lc := cltypes.LightClientUpdate{}
	decodedSSZ, err := utils.DecompressSnappy(updateTest)
	require.NoError(t, err)
	require.NoError(t, lc.DecodeSSZWithVersion(decodedSSZ, int(clparams.CapellaVersion)))
	// checking fields
	require.Equal(t, lc.AttestedHeader.HeaderEth2.BodyRoot, libcommon.HexToHash("0x49f3c74b8874e1595d74d970886245aff4748a11d499a8e4a9459e76e2c8c42f"))
	require.Equal(t, lc.AttestedHeader.HeaderEth1.WithdrawalsRoot, libcommon.HexToHash("0xab14e4976e1af2983f637d57360f03fafb3d0586be3efe254ba32d612ebf21ea"))
	require.Equal(t, lc.AttestedHeader.ExecutionBranch[0], libcommon.HexToHash("0x1b42aa11e8fb2cfe30f4ce521021426eb22824b1f2934e85c134c8d71795faf7"))
	// Finalized header
	require.Equal(t, lc.FinalizedHeader.HeaderEth2.BodyRoot, libcommon.HexToHash("0xcb968a1a97415c40e9fffcb2afc34cf97a41f37dabe0a2135f54441652cc9f9d"))
	require.Equal(t, lc.FinalizedHeader.HeaderEth1.WithdrawalsRoot, libcommon.HexToHash("0xf9c686992daa952349ec70a44bf14238135b25c788f1286d8c5cb7aa8d43b94b"))
	require.Equal(t, lc.FinalizedHeader.ExecutionBranch[0], libcommon.HexToHash("0x0c921fe9c8f7818d508ff6fb77c65ec143f1822ae520a63c4ebf8a80f11d5456"))
	// Next sync committee
	require.Equal(t, lc.NextSyncCommitee.PubKeys[0][:], common.Hex2Bytes("29a24e45031507af4f61d534c734042b67ce0e36a549f93e8b72644a4e3874e142a438d4e30eaa53d235125dd1cd7853"))
	require.Equal(t, lc.NextSyncCommitee.PubKeys[len(lc.NextSyncCommitee.PubKeys)-1][:], common.Hex2Bytes("9dd39a3f44bdcf1b8df92202049b3f9114d59964d75d838bdd0011e161d65c1b320b22df82e78cc1ded097738e44f943"))
	require.Equal(t, lc.NextSyncCommitee.AggregatePublicKey[:], common.Hex2Bytes("fa65b9c18cdc09b0378365eef95bc7607f15c27ac655b0d36ab415eec9b86b611f9e4a91fde40ee41f9756e445b7bf60"))
	require.Equal(t, lc.NextSyncCommitteeBranch[0], libcommon.HexToHash("dbfa8fa48e3d6ddc4f03f7eaf7a8c972d93874be3dc099f04321d9f0330b4a7a"))

	// Rest
	require.Equal(t, lc.FinalityBranch[0], libcommon.HexToHash("4fba794addb5d9a72464494236a7d571d4291bce2e03d64bec942f1fc72ab0df"))
	require.Equal(t, lc.SignatureSlot, uint64(10239421204327532221))
	require.Equal(t, lc.SyncAggregate.SyncCommiteeSignature[:], common.Hex2Bytes("7d280e88d4358bcd312e9ccd894fef1dfeaf46e4a081ea07971779238a3dd8ad2ba339954e464df0a4cbbbbbd22258bdf543ad6f51232ec48e2d151527909c084189480dd6e350d5e022621aceeb478ff215b9d0e8fcd4fc7dca8c963f70533a"))
	// encoding it back
	dec, err := lc.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, dec, decodedSSZ)
}
