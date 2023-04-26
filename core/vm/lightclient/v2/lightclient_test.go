// Package v2 is used for tendermint v0.34.22 and its compatible version.
package v2

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/crypto/ed25519"
	tmproto "github.com/cometbft/cometbft/proto/tendermint/types"
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
	csBytes, err := hex.DecodeString("677265656e6669656c645f393030302d3132310000000000000000000000000000000000000000013c350cd55b99dc6c2b7da9bef5410fbfb869fede858e7b95bf7ca294e228bb40e33f6e876d63791ebd05ff617a1b4f4ad1aa2ce65e3c3a9cdfb33e0ffa7e8423000000000098968015154514f68ce65a0d9eecc578c0ab12da0a2a28a0805521b5b7ae56eb3fb24555efbfe59e1622bfe9f7be8c9022e9b3f2442739c1ce870b9adee169afe60f674edd7c86451c5363d89052fde8351895eeea166ce5373c36e31b518ed191d0c599aa0f5b0000000000989680432f6c4908a9aa5f3444421f466b11645235c99b831b2a2de9e504d7ea299e52a202ce529808618eb3bfc0addf13d8c5f2df821d81e18f9bc61583510b322d067d46323b0a572635c06a049c0a2a929e3c8184a50cf6a8b95708c25834ade456f399015a0000000000989680864cb9828254d712f8e59b164fc6a9402dc4e6c59065e38cff24f5323c8c5da888a0f97e5ee4ba1e11b0674b0a0d06204c1dfa247c370cd4be3e799fc4f6f48d977ac7ca")
	require.NoError(t, err)
	t.Logf("cs length: %d\n", len(csBytes))
	blockBytes, err := hex.DecodeString("0aeb060adb030a02080b1213677265656e6669656c645f393030302d3132311802220c08b2d7f3a10610e8d2adb3032a480a20ec6ecb5db4ffb17fabe40c60ca7b8441e9c5d77585d0831186f3c37aa16e9c15122408011220a2ab9e1eb9ea52812f413526e424b326aff2f258a56e00d690db9f805b60fe7e32200f40aeff672e8309b7b0aefbb9a1ae3d4299b5c445b7d54e8ff398488467f0053a20e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85542203c350cd55b99dc6c2b7da9bef5410fbfb869fede858e7b95bf7ca294e228bb404a203c350cd55b99dc6c2b7da9bef5410fbfb869fede858e7b95bf7ca294e228bb405220294d8fbd0b94b767a7eba9840f299a3586da7fe6b5dead3b7eecba193c400f935a20bc50557c12d7392b0d07d75df0b61232d48f86a74fdea6d1485d9be6317d268c6220e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b8556a20e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85572146699336aa109d1beab3946198c8e59f3b2cbd92f7a4065e3cd89e315ca39d87dee92835b98f8b8ec0861d6d9bb2c60156df5d375b3ceb1fbe71af6a244907d62548a694165caa660fec7a9b4e7b9198191361c71be0b128a0308021a480a20726abd0fdbfb6f779b0483e6e4b4b6f12241f6ea2bf374233ab1a316692b6415122408011220159f10ff15a8b58fc67a92ffd7f33c8cd407d4ce81b04ca79177dfd00ca19a67226808021214050cff76cc632760ba9db796c046004c900967361a0c08b3d7f3a10610808cadba03224080713027ffb776a702d78fd0406205c629ba473e1f8d6af646190f6eb9262cd67d69be90d10e597b91e06d7298eb6fa4b8f1eb7752ebf352a1f51560294548042268080212146699336aa109d1beab3946198c8e59f3b2cbd92f1a0c08b3d7f3a10610b087c1c00322405e2ddb70acfe4904438be3d9f4206c0ace905ac4fc306a42cfc9e86268950a0fbfd6ec5f526d3e41a3ef52bf9f9f358e3cb4c3feac76c762fa3651c1244fe004226808021214c55765fd2d0570e869f6ac22e7f2916a35ea300d1a0c08b3d7f3a10610f0b3d492032240ca17898bd22232fc9374e1188636ee321a396444a5b1a79f7628e4a11f265734b2ab50caf21e8092c55d701248e82b2f011426cb35ba22043b497a6b4661930612a0050aa8010a14050cff76cc632760ba9db796c046004c9009673612220a20e33f6e876d63791ebd05ff617a1b4f4ad1aa2ce65e3c3a9cdfb33e0ffa7e84231880ade2042080a6bbf6ffffffffff012a30a0805521b5b7ae56eb3fb24555efbfe59e1622bfe9f7be8c9022e9b3f2442739c1ce870b9adee169afe60f674edd7c86321415154514f68ce65a0d9eecc578c0ab12da0a2a283a14ee7a2a6a44d427f6949eeb8f12ea9fbb2501da880aa2010a146699336aa109d1beab3946198c8e59f3b2cbd92f12220a20451c5363d89052fde8351895eeea166ce5373c36e31b518ed191d0c599aa0f5b1880ade2042080ade2042a30831b2a2de9e504d7ea299e52a202ce529808618eb3bfc0addf13d8c5f2df821d81e18f9bc61583510b322d067d46323b3214432f6c4908a9aa5f3444421f466b11645235c99b3a14a0a7769429468054e19059af4867da0a495567e50aa2010a14c55765fd2d0570e869f6ac22e7f2916a35ea300d12220a200a572635c06a049c0a2a929e3c8184a50cf6a8b95708c25834ade456f399015a1880ade2042080ade2042a309065e38cff24f5323c8c5da888a0f97e5ee4ba1e11b0674b0a0d06204c1dfa247c370cd4be3e799fc4f6f48d977ac7ca3214864cb9828254d712f8e59b164fc6a9402dc4e6c53a143139916d97df0c589312b89950b6ab9795f34d1a12a8010a14050cff76cc632760ba9db796c046004c9009673612220a20e33f6e876d63791ebd05ff617a1b4f4ad1aa2ce65e3c3a9cdfb33e0ffa7e84231880ade2042080a6bbf6ffffffffff012a30a0805521b5b7ae56eb3fb24555efbfe59e1622bfe9f7be8c9022e9b3f2442739c1ce870b9adee169afe60f674edd7c86321415154514f68ce65a0d9eecc578c0ab12da0a2a283a14ee7a2a6a44d427f6949eeb8f12ea9fbb2501da88")
	require.NoError(t, err)

	var lbpb tmproto.LightBlock
	err = lbpb.Unmarshal(blockBytes)
	require.NoError(t, err)
	block, err := types.LightBlockFromProto(&lbpb)
	require.NoError(t, err)

	cs, err := DecodeConsensusState(csBytes)
	require.NoError(t, err)
	validatorSetChanged, err := cs.ApplyLightBlock(block)
	require.NoError(t, err)

	if cs.Height != 2 {
		t.Fatalf("Height is unexpected, expected: 2, actual: %d\n", cs.Height)
	}

	if validatorSetChanged {
		t.Fatalf("Validator set has exchanaged which is not expected.\n")
	}
}
