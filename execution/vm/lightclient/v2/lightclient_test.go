// Package v2 is used for tendermint v0.34.22 and its compatible version.
package v2

import (
	"bytes"
	"encoding/hex"
	tmproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/types"
)

type validatorInfo struct {
	pubKey         string
	votingPower    int64
	relayerAddress string
	relayerBlsKey  string
}

var testcases = []struct {
	chainID              string
	height               uint64
	nextValidatorSetHash string
	vals                 []validatorInfo
	consensusStateBytes  string
}{
	{
		chainID:              "chain_9000-121",
		height:               1,
		nextValidatorSetHash: "0CE856B1DC9CDCF3BF2478291CF02C62AEEB3679889E9866931BF1FB05A10EDA",
		vals: []validatorInfo{
			{
				pubKey:         "c3d9a1082f42ca161402f8668f8e39ec9e30092affd8d3262267ac7e248a959e",
				votingPower:    int64(10000),
				relayerAddress: "B32d0723583040F3A16D1380D1e6AA874cD1bdF7",
				relayerBlsKey:  "a60afe627fd78b19e07e07e19d446009dd53a18c6c8744176a5d851a762bbb51198e7e006f2a6ea7225661a61ecd832d",
			},
		},
		consensusStateBytes: "636861696e5f393030302d31323100000000000000000000000000000000000000000000000000010ce856b1dc9cdcf3bf2478291cf02c62aeeb3679889e9866931bf1fb05a10edac3d9a1082f42ca161402f8668f8e39ec9e30092affd8d3262267ac7e248a959e0000000000002710b32d0723583040f3a16d1380d1e6aa874cd1bdf7a60afe627fd78b19e07e07e19d446009dd53a18c6c8744176a5d851a762bbb51198e7e006f2a6ea7225661a61ecd832d",
	},
	{
		chainID:              "chain_9000-121",
		height:               1,
		nextValidatorSetHash: "A5F1AF4874227F1CDBE5240259A365AD86484A4255BFD65E2A0222D733FCDBC3",
		vals: []validatorInfo{
			{
				pubKey:         "20cc466ee9412ddd49e0fff04cdb41bade2b7622f08b6bdacac94d4de03bdb97",
				votingPower:    int64(10000),
				relayerAddress: "d5e63aeee6e6fa122a6a23a6e0fca87701ba1541",
				relayerBlsKey:  "aa2d28cbcd1ea3a63479f6fb260a3d755853e6a78cfa6252584fee97b2ec84a9d572ee4a5d3bc1558bb98a4b370fb861",
			},
			{
				pubKey:         "6b0b523ee91ad18a63d63f21e0c40a83ef15963f4260574ca5159fd90a1c5270",
				votingPower:    int64(10000),
				relayerAddress: "6fd1ceb5a48579f322605220d4325bd9ff90d5fa",
				relayerBlsKey:  "b31e74a881fc78681e3dfa440978d2b8be0708a1cbbca2c660866216975fdaf0e9038d9b7ccbf9731f43956dba7f2451",
			},
			{
				pubKey:         "919606ae20bf5d248ee353821754bcdb456fd3950618fda3e32d3d0fb990eeda",
				votingPower:    int64(10000),
				relayerAddress: "97376a436bbf54e0f6949b57aa821a90a749920a",
				relayerBlsKey:  "b32979580ea04984a2be033599c20c7a0c9a8d121b57f94ee05f5eda5b36c38f6e354c89328b92cdd1de33b64d3a0867",
			},
		},
		consensusStateBytes: "636861696e5f393030302d3132310000000000000000000000000000000000000000000000000001a5f1af4874227f1cdbe5240259a365ad86484a4255bfd65e2a0222d733fcdbc320cc466ee9412ddd49e0fff04cdb41bade2b7622f08b6bdacac94d4de03bdb970000000000002710d5e63aeee6e6fa122a6a23a6e0fca87701ba1541aa2d28cbcd1ea3a63479f6fb260a3d755853e6a78cfa6252584fee97b2ec84a9d572ee4a5d3bc1558bb98a4b370fb8616b0b523ee91ad18a63d63f21e0c40a83ef15963f4260574ca5159fd90a1c527000000000000027106fd1ceb5a48579f322605220d4325bd9ff90d5fab31e74a881fc78681e3dfa440978d2b8be0708a1cbbca2c660866216975fdaf0e9038d9b7ccbf9731f43956dba7f2451919606ae20bf5d248ee353821754bcdb456fd3950618fda3e32d3d0fb990eeda000000000000271097376a436bbf54e0f6949b57aa821a90a749920ab32979580ea04984a2be033599c20c7a0c9a8d121b57f94ee05f5eda5b36c38f6e354c89328b92cdd1de33b64d3a0867",
	},
}

var applyBlocksTestcases = []struct {
	consensusStateBytes string
	lightBlockBytes     string
	expectHeight        uint64
	expectValSetChanged bool
}{
	{
		consensusStateBytes: "677265656e6669656c645f393030302d3132310000000000000000000000000000000000000000013c350cd55b99dc6c2b7da9bef5410fbfb869fede858e7b95bf7ca294e228bb40e33f6e876d63791ebd05ff617a1b4f4ad1aa2ce65e3c3a9cdfb33e0ffa7e8423000000000098968015154514f68ce65a0d9eecc578c0ab12da0a2a28a0805521b5b7ae56eb3fb24555efbfe59e1622bfe9f7be8c9022e9b3f2442739c1ce870b9adee169afe60f674edd7c86451c5363d89052fde8351895eeea166ce5373c36e31b518ed191d0c599aa0f5b0000000000989680432f6c4908a9aa5f3444421f466b11645235c99b831b2a2de9e504d7ea299e52a202ce529808618eb3bfc0addf13d8c5f2df821d81e18f9bc61583510b322d067d46323b0a572635c06a049c0a2a929e3c8184a50cf6a8b95708c25834ade456f399015a0000000000989680864cb9828254d712f8e59b164fc6a9402dc4e6c59065e38cff24f5323c8c5da888a0f97e5ee4ba1e11b0674b0a0d06204c1dfa247c370cd4be3e799fc4f6f48d977ac7ca",
		lightBlockBytes:     "0aeb060adb030a02080b1213677265656e6669656c645f393030302d3132311802220c08b2d7f3a10610e8d2adb3032a480a20ec6ecb5db4ffb17fabe40c60ca7b8441e9c5d77585d0831186f3c37aa16e9c15122408011220a2ab9e1eb9ea52812f413526e424b326aff2f258a56e00d690db9f805b60fe7e32200f40aeff672e8309b7b0aefbb9a1ae3d4299b5c445b7d54e8ff398488467f0053a20e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85542203c350cd55b99dc6c2b7da9bef5410fbfb869fede858e7b95bf7ca294e228bb404a203c350cd55b99dc6c2b7da9bef5410fbfb869fede858e7b95bf7ca294e228bb405220294d8fbd0b94b767a7eba9840f299a3586da7fe6b5dead3b7eecba193c400f935a20bc50557c12d7392b0d07d75df0b61232d48f86a74fdea6d1485d9be6317d268c6220e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8556a20e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85572146699336aa109d1beab3946198c8e59f3b2cbd92f7a4065e3cd89e315ca39d87dee92835b98f8b8ec0861d6d9bb2c60156df5d375b3ceb1fbe71af6a244907d62548a694165caa660fec7a9b4e7b9198191361c71be0b128a0308021a480a20726abd0fdbfb6f779b0483e6e4b4b6f12241f6ea2bf374233ab1a316692b6415122408011220159f10ff15a8b58fc67a92ffd7f33c8cd407d4ce81b04ca79177dfd00ca19a67226808021214050cff76cc632760ba9db796c046004c900967361a0c08b3d7f3a10610808cadba03224080713027ffb776a702d78fd0406205c629ba473e1f8d6af646190f6eb9262cd67d69be90d10e597b91e06d7298eb6fa4b8f1eb7752ebf352a1f51560294548042268080212146699336aa109d1beab3946198c8e59f3b2cbd92f1a0c08b3d7f3a10610b087c1c00322405e2ddb70acfe4904438be3d9f4206c0ace905ac4fc306a42cfc9e86268950a0fbfd6ec5f526d3e41a3ef52bf9f9f358e3cb4c3feac76c762fa3651c1244fe004226808021214c55765fd2d0570e869f6ac22e7f2916a35ea300d1a0c08b3d7f3a10610f0b3d492032240ca17898bd22232fc9374e1188636ee321a396444a5b1a79f7628e4a11f265734b2ab50caf21e8092c55d701248e82b2f011426cb35ba22043b497a6b4661930612a0050aa8010a14050cff76cc632760ba9db796c046004c9009673612220a20e33f6e876d63791ebd05ff617a1b4f4ad1aa2ce65e3c3a9cdfb33e0ffa7e84231880ade2042080a6bbf6ffffffffff012a30a0805521b5b7ae56eb3fb24555efbfe59e1622bfe9f7be8c9022e9b3f2442739c1ce870b9adee169afe60f674edd7c86321415154514f68ce65a0d9eecc578c0ab12da0a2a283a14ee7a2a6a44d427f6949eeb8f12ea9fbb2501da880aa2010a146699336aa109d1beab3946198c8e59f3b2cbd92f12220a20451c5363d89052fde8351895eeea166ce5373c36e31b518ed191d0c599aa0f5b1880ade2042080ade2042a30831b2a2de9e504d7ea299e52a202ce529808618eb3bfc0addf13d8c5f2df821d81e18f9bc61583510b322d067d46323b3214432f6c4908a9aa5f3444421f466b11645235c99b3a14a0a7769429468054e19059af4867da0a495567e50aa2010a14c55765fd2d0570e869f6ac22e7f2916a35ea300d12220a200a572635c06a049c0a2a929e3c8184a50cf6a8b95708c25834ade456f399015a1880ade2042080ade2042a309065e38cff24f5323c8c5da888a0f97e5ee4ba1e11b0674b0a0d06204c1dfa247c370cd4be3e799fc4f6f48d977ac7ca3214864cb9828254d712f8e59b164fc6a9402dc4e6c53a143139916d97df0c589312b89950b6ab9795f34d1a12a8010a14050cff76cc632760ba9db796c046004c9009673612220a20e33f6e876d63791ebd05ff617a1b4f4ad1aa2ce65e3c3a9cdfb33e0ffa7e84231880ade2042080a6bbf6ffffffffff012a30a0805521b5b7ae56eb3fb24555efbfe59e1622bfe9f7be8c9022e9b3f2442739c1ce870b9adee169afe60f674edd7c86321415154514f68ce65a0d9eecc578c0ab12da0a2a283a14ee7a2a6a44d427f6949eeb8f12ea9fbb2501da88",
		expectHeight:        2,
		expectValSetChanged: false,
	},
	{
		consensusStateBytes: "677265656e6669656c645f393030302d313734310000000000000000000000000000000000000001af6b801dda578dddfa4da1d5d67fd1b32510db24ec271346fc573e9242b01c9a112b51dda2d336246bdc0cc51407ba0cb0e5087be0db5f1cdc3285bbaa8e647500000000000003e84202722cf6a34d727be762b46825b0d26b6263a0a9355ebf3c24bedac5a357a56feeb2cd8b6fed9f14cca15c3091f523b9fb21183b4bb31eb482a0321885e3f57072156448e2b2f7d9a3e7b668757d9cc0bbd28cd674c34ed1c2ed75c5de3b6a8f8cad4600000000000003e8668a0acd8f6db5cae959a0e02132f4d6a672c4d7a4726b542012cc8023ee07b29ab3971cc999d8751bbd16f23413968afcdb070ed66ab47e6e1842bf875bef21dfc5b8af6813bfd82860d361e339bd1ae2f801b6d6ee46b8497a3d51c80b50b6160ea1cc00000000000003e80dfa99423d3084c596c5e3bd6bcb4f654516517b8d4786703c56b300b70f085c0d0482e5d6a3c7208883f0ec8abd2de893f71d18e8f919e7ab198499201d87f92c57ebce83ed2b763bb872e9bc148fb216fd5c93b18819670d9a946ae4b3075672d726b800000000000003e824aab6f85470ff73e3048c64083a09e980d4cb7f8146d231a7b2051c5f7a9c07ab6e6bfe277bd5f4a94f901fe6ee7a6b6bd8479e9e5e448de4b1b33d5ddd74194c86b3852cc140a3f08a9c4149efd45643202f8bef2ad7eecf53e58951c6df6fd932004b00000000000003e84998f6ef8d999a0f36a851bfa29dbcf0364dd65695c286deb3f1657664859d59876bf1ec5a288f6e66e18b37b8a2a1e6ee4a3ef8fa50784d8b758d0c3e70a7cdfe65ab5d",
		lightBlockBytes:     "0aeb070ade030a02080b1214677265656e6669656c645f393030302d3137343118e9d810220c08f2f2b6a30610af9fcc8e022a480a20315130cf3a10f78c5f7633e3941f605151a6901910713c84da0d7929898e9b9e122408011220f09b2290e56b59a7286c2144a811c780f0fd5f631614a9f7ec2dec43f14ac5d63220d15354fdbcc6c7d3e8c5ede34f4f71e896599ba67773605eb6579e10e09254773a20e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8554220311b22582926e7833b72904605441ed602896e8aeb093bca5f2e8170cea5ed6a4a20311b22582926e7833b72904605441ed602896e8aeb093bca5f2e8170cea5ed6a5220048091bc7ddc283f77bfbf91d73c44da58c3df8a9cbc867405d8b7f3daada22f5a20ee2da802b95c55e551291d96fe6ee4fe8074ddfa2df110042d6809acb665628a6220e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8556a20e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8557214793cee4b478e537592c40ecfb2148ebe32b8f6057a4034248b04af30e0d302cf8cedff585d5e1c6ff8db526bcf298d665cf301ca938a874c76ba9a1fd9fae302b2ec49a335930cf0242762c92843d7f9f7963d60580a12870408e9d8101a480a20452e1984f64c79550ac23db0c408b3eb021675d678ad94f9206ad7a2dec83a181224080112205224c29260b6c220685b29f593bac728e522e3e3675ec7edd92c12251acfe4b4226808021214d742fa5318dc3986e075e2b050529a22c6fa3b8b1a0c08f4f2b6a306109898f6a70322409762b7abd4dd63bb8858673dffd5795b1a87532d3719458d12fbbd1fd2443ca76bd36c4c09fa8952a440de4904f1b6b9270037a147431892c8ace96ad43bf90b2268080212145fa8b3f3fcd4a3ea2495e11dd5dbd399b3d8d4f81a0c08f4f2b6a30610f8f2fd9e03224093f2fc21a41492a34ed3b31ff2eba571ca752ae989f2e47728740bb1eec0f20eb59f59d390ce3d67734ab49a72bc2e97e185d21a4b00f3288ea50b0f1383220a226808021214793cee4b478e537592c40ecfb2148ebe32b8f6051a0c08f4f2b6a306108e8ed7a7032240a4a3c047ca75aeb6e9a21fbc3742f4339c64ead15d117675a2757f7db965aae3e6901f81a3707a67d91c61d6c842b95009e132e7fab187965dc04861d7faa902226808021214f0f07dc2f5e159a35b9662553c6b4e51868502f71a0c08f4f2b6a30610bfed829f032240e23ddc98b0bf7cc6cd494fd8ec96d440d29193910a6eca3dc7e41cdb14efa32471feb1ea2d613bb5acdd8623e8372ed3a36e1838bc75646bdfe9d2ef96647400220f08011a0b088092b8c398feffffff0112d0060a90010a14d742fa5318dc3986e075e2b050529a22c6fa3b8b12220a2083ed2b763bb872e9bc148fb216fd5c93b18819670d9a946ae4b3075672d726b818880820abe8ffffffffffffff012a308146d231a7b2051c5f7a9c07ab6e6bfe277bd5f4a94f901fe6ee7a6b6bd8479e9e5e448de4b1b33d5ddd74194c86b385321424aab6f85470ff73e3048c64083a09e980d4cb7f0a88010a145fa8b3f3fcd4a3ea2495e11dd5dbd399b3d8d4f812220a2048e2b2f7d9a3e7b668757d9cc0bbd28cd674c34ed1c2ed75c5de3b6a8f8cad4618fc0720fc072a30a4726b542012cc8023ee07b29ab3971cc999d8751bbd16f23413968afcdb070ed66ab47e6e1842bf875bef21dfc5b8af3214668a0acd8f6db5cae959a0e02132f4d6a672c4d70a88010a14793cee4b478e537592c40ecfb2148ebe32b8f60512220a206813bfd82860d361e339bd1ae2f801b6d6ee46b8497a3d51c80b50b6160ea1cc18ec0720ec072a308d4786703c56b300b70f085c0d0482e5d6a3c7208883f0ec8abd2de893f71d18e8f919e7ab198499201d87f92c57ebce32140dfa99423d3084c596c5e3bd6bcb4f654516517b0a88010a14f0f07dc2f5e159a35b9662553c6b4e51868502f712220a202cc140a3f08a9c4149efd45643202f8bef2ad7eecf53e58951c6df6fd932004b18ec0720ec072a3095c286deb3f1657664859d59876bf1ec5a288f6e66e18b37b8a2a1e6ee4a3ef8fa50784d8b758d0c3e70a7cdfe65ab5d32144998f6ef8d999a0f36a851bfa29dbcf0364dd6560a86010a1468478c1a37bc01c3acb7470cc6a78f1009a14f7012220a20de83e10566b038855254800b5b0ebf7c21aede9883c11e5cf289979e233b3efe180120012a3089063607696a9e6dbddbe6c23b4634a7c02b80212afc7ec65fb0d379d55d2d0cb25df19c0252356ffa2e2252eedd8f57321400000000000000000000000000000000000000001290010a14d742fa5318dc3986e075e2b050529a22c6fa3b8b12220a2083ed2b763bb872e9bc148fb216fd5c93b18819670d9a946ae4b3075672d726b818880820abe8ffffffffffffff012a308146d231a7b2051c5f7a9c07ab6e6bfe277bd5f4a94f901fe6ee7a6b6bd8479e9e5e448de4b1b33d5ddd74194c86b385321424aab6f85470ff73e3048c64083a09e980d4cb7f",
		expectHeight:        273513,
		expectValSetChanged: true,
	},
}

func TestEncodeConsensusState(t *testing.T) {
	for i := 0; i < len(testcases); i++ {
		testcase := testcases[i]

		var validatorSet []*types.Validator

		for j := 0; j < len(testcase.vals); j++ {
			valInfo := testcase.vals[j]

			pubKeyBytes, err := hex.DecodeString(valInfo.pubKey)
			require.NoError(t, err)
			relayerAddress, err := hex.DecodeString(valInfo.relayerAddress)
			require.NoError(t, err)
			relayerBlsKey, err := hex.DecodeString(valInfo.relayerBlsKey)
			require.NoError(t, err)

			pubkey := ed25519.PubKey(make([]byte, ed25519.PubKeySize))
			copy(pubkey[:], pubKeyBytes)
			validator := types.NewValidator(pubkey, valInfo.votingPower)
			validator.SetRelayerAddress(relayerAddress)
			validator.SetBlsKey(relayerBlsKey)
			validatorSet = append(validatorSet, validator)
		}

		nextValidatorHash, err := hex.DecodeString(testcase.nextValidatorSetHash)
		require.NoError(t, err)

		consensusState := ConsensusState{
			ChainID:              testcase.chainID,
			Height:               testcase.height,
			NextValidatorSetHash: nextValidatorHash,
			ValidatorSet: &types.ValidatorSet{
				Validators: validatorSet,
			},
		}

		csBytes, err := consensusState.EncodeConsensusState()
		require.NoError(t, err)

		expectCsBytes, err := hex.DecodeString(testcase.consensusStateBytes)
		require.NoError(t, err)

		if !bytes.Equal(csBytes, expectCsBytes) {
			t.Fatalf("Consensus state mimatch, expect: %s, real:%s\n", testcase.consensusStateBytes, hex.EncodeToString(csBytes))
		}
	}
}

func TestDecodeConsensusState(t *testing.T) {
	for i := 0; i < len(testcases); i++ {
		testcase := testcases[i]

		csBytes, err := hex.DecodeString(testcase.consensusStateBytes)
		require.NoError(t, err)

		cs, err := DecodeConsensusState(csBytes)
		require.NoError(t, err)

		if cs.ChainID != testcase.chainID {
			t.Fatalf("Chain ID mimatch, expect: %s, real:%s\n", testcase.chainID, cs.ChainID)
		}

		if cs.Height != testcase.height {
			t.Fatalf("Height mimatch, expect: %d, real:%d\n", testcase.height, cs.Height)
		}

		nextValidatorSetHashBytes, err := hex.DecodeString(testcase.nextValidatorSetHash)
		if err != nil {
			t.Fatalf("Decode next validator set hash failed: %v\n", err)
		}

		if !bytes.Equal(cs.NextValidatorSetHash, nextValidatorSetHashBytes) {
			t.Fatalf("Next validator set hash mimatch, expect: %s, real:%s\n", testcase.nextValidatorSetHash, hex.EncodeToString(cs.NextValidatorSetHash))
		}
	}
}

func TestConsensusStateApplyLightBlock(t *testing.T) {
	for i := 0; i < len(applyBlocksTestcases); i++ {
		testcase := applyBlocksTestcases[i]

		csBytes, err := hex.DecodeString(testcase.consensusStateBytes)
		require.NoError(t, err)

		blockBytes, err := hex.DecodeString(testcase.lightBlockBytes)
		require.NoError(t, err)

		var lbpb tmproto.LightBlock
		err = lbpb.Unmarshal(blockBytes)
		require.NoError(t, err)
		block, err := types.LightBlockFromProto(&lbpb)
		require.NoError(t, err)

		cs, err := DecodeConsensusState(csBytes)
		require.NoError(t, err)
		validatorSetChanged, err := cs.ApplyLightBlock(block, true)
		require.NoError(t, err)

		if cs.Height != testcase.expectHeight {
			t.Fatalf("Height is unexpected, expected: %d, actual: %d\n", testcase.expectHeight, cs.Height)
		}

		if validatorSetChanged != testcase.expectValSetChanged {
			t.Fatalf("Validator set changed is unexpected, expected: %v, actual: %v\n", testcase.expectValSetChanged, validatorSetChanged)
		}
	}
}
