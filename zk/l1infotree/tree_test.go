package l1infotree_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
	l1infotree2 "github.com/ledgerwatch/erigon/zk/tests/vectors/l1infotree"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/stretchr/testify/require"
)

func TestComputeTreeRoot(t *testing.T) {
	data, err := os.ReadFile("../tests/vectors/l1infotree/root-vectors.json")
	require.NoError(t, err)
	var mtTestVectors []l1infotree2.L1InfoTree
	err = json.Unmarshal(data, &mtTestVectors)
	require.NoError(t, err)
	for _, testVector := range mtTestVectors {
		input := testVector.PreviousLeafValues
		mt, err := l1infotree.NewL1InfoTree(uint8(32), [][32]byte{})
		require.NoError(t, err)

		var leaves [][32]byte
		for _, v := range input {
			leaves = append(leaves, v)
		}

		if len(leaves) != 0 {
			root, err := mt.BuildL1InfoRoot(leaves)
			require.NoError(t, err)
			require.Equal(t, testVector.CurrentRoot, root)
		}

		leaves = append(leaves, testVector.NewLeafValue)
		newRoot, err := mt.BuildL1InfoRoot(leaves)
		require.NoError(t, err)
		require.Equal(t, testVector.NewRoot, newRoot)
	}
}

func TestComputeMerkleProof(t *testing.T) {
	mt, err := l1infotree.NewL1InfoTree(uint8(32), [][32]byte{})
	require.NoError(t, err)
	leaves := [][32]byte{
		common.HexToHash("0x83fc198de31e1b2b1a8212d2430fbb7766c13d9ad305637dea3759065606475d"),
		common.HexToHash("0x83fc198de31e1b2b1a8212d2430fbb7766c13d9ad305637dea3759065606475d"),
		common.HexToHash("0x0349657c7850dc9b2b73010501b01cd6a38911b6a2ad2167c164c5b2a5b344de"),
		common.HexToHash("0xb32f96fad8af99f3b3cb90dfbb4849f73435dbee1877e4ac2c213127379549ce"),
		common.HexToHash("0x79ffa1294bf48e0dd41afcb23b2929921e4e17f2f81b7163c23078375b06ba4f"),
		common.HexToHash("0x0004063b5c83f56a17f580db0908339c01206cdf8b59beb13ce6f146bb025fe2"),
		common.HexToHash("0x68e4f2c517c7f60c3664ac6bbe78f904eacdbe84790aa0d15d79ddd6216c556e"),
		common.HexToHash("0xf7245f4d84367a189b90873e4563a000702dbfe974b872fdb13323a828c8fb71"),
		common.HexToHash("0x0e43332c71c6e2f4a48326258ea17b75d77d3063a4127047dd32a4cb089e62a4"),
		common.HexToHash("0xd35a1dc90098c0869a69891094c119eb281cee1a7829d210df1bf8afbea08adc"),
		common.HexToHash("0x13bffd0da370d1e80a470821f1bee9607f116881feb708f1ec255da1689164b3"),
		common.HexToHash("0x5fa79a24c9bc73cd507b02e5917cef9782529080aa75eacb2bf4e1d45fda7f1d"),
		common.HexToHash("0x975b5bbc67345adc6ee6d1d67d1d5cd2a430c231d93e5a8b5a6f00b0c0862215"),
		common.HexToHash("0x0d0fa887c045a53ec6212dee58964d0ae89595b7d11745a05c397240a4dceb20"),
		common.HexToHash("0xa6ae5bc494a2ee0e5173d0e0b546533973104e0031c69d0cd65cdc7bb4d64670"),
		common.HexToHash("0x21ccc18196a8fd74e720c6c129977d80bb804d3331673d6411871df14f7e7ae4"),
		common.HexToHash("0xf8b1b98ac75bea8dbed034d0b3cd08b4c9275644c2242781a827e53deb2386c3"),
		common.HexToHash("0x26401c418ef8bc5a80380f25f16dfc78b7053a26c0ca425fda294b1678b779fc"),
		common.HexToHash("0xc53fd99005361738fc811ce87d194deed34a7f06ebd5371b19a008e8d1e8799f"),
		common.HexToHash("0x570bd643e35fbcda95393994812d9212335e6bd4504b3b1dc8f3c6f1eeb247b2"),
		common.HexToHash("0xb21ac971d007810540583bd3c0d4f35e0c2f4b62753e51c104a5753c6372caf8"),
		common.HexToHash("0xb8dae305b34c749cbbd98993bfd71ec2323e8364861f25b4c5e0ac3c9587e16d"),
		common.HexToHash("0x57c7fabd0f70e0059e871953fcb3dd43c6b8a5f348dbe771190cc8b0320336a5"),
		common.HexToHash("0x95b0d23c347e2a88fc8e2ab900b09212a1295ab8f169075aa27e8719557d9b06"),
		common.HexToHash("0x95b0d23c347e2a88fc8e2ab900b09212a1295ab8f169075aa27e8719557d9b06"),
		common.HexToHash("0x95b0d23c347e2a88fc8e2ab900b09212a1295ab8f169075aa27e8719557d9b06"),
	}
	require.Equal(t, 26, len(leaves))
	siblings, root, err := mt.ComputeMerkleProof(1, leaves)
	require.NoError(t, err)
	require.Equal(t, "0x4ed479841384358f765966486782abb598ece1d4f834a22474050d66a18ad296", root.String())
	expectedProof := []string{"0x83fc198de31e1b2b1a8212d2430fbb7766c13d9ad305637dea3759065606475d", "0x2815e0bbb1ec18b8b1bc64454a86d072e12ee5d43bb559b44059e01edff0af7a", "0x7fb6cc0f2120368a845cf435da7102ff6e369280f787bc51b8a989fc178f7252", "0x407db5edcdc0ddd4f7327f208f46db40c4c4dbcc46c94a757e1d1654acbd8b72", "0xce2cdd1ef2e87e82264532285998ff37024404ab3a2b77b50eb1ad856ae83e14", "0x0eb01ebfc9ed27500cd4dfc979272d1f0913cc9f66540d7e8005811109e1cf2d", "0x887c22bd8750d34016ac3c66b5ff102dacdd73f6b014e710b51e8022af9a1968", "0xffd70157e48063fc33c97a050f7f640233bf646cc98d9524c6b92bcf3ab56f83", "0x9867cc5f7f196b93bae1e27e6320742445d290f2263827498b54fec539f756af", "0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e0", "0xf9dc3e7fe016e050eff260334f18a5d4fe391d82092319f5964f2e2eb7c1c3a5", "0xf8b13a49e282f609c317a833fb8d976d11517c571d1221a265d25af778ecf892", "0x3490c6ceeb450aecdc82e28293031d10c7d73bf85e57bf041a97360aa2c5d99c", "0xc1df82d9c4b87413eae2ef048f94b4d3554cea73d92b0f7af96e0271c691e2bb", "0x5c67add7c6caf302256adedf7ab114da0acfe870d449a3a489f781d659e8becc", "0xda7bce9f4e8618b6bd2f4132ce798cdc7a60e7e1460a7299e3c6342a579626d2", "0x2733e50f526ec2fa19a22b31e8ed50f23cd1fdf94c9154ed3a7609a2f1ff981f", "0xe1d3b5c807b281e4683cc6d6315cf95b9ade8641defcb32372f1c126e398ef7a", "0x5a2dce0a8a7f68bb74560f8f71837c2c2ebbcbf7fffb42ae1896f13f7c7479a0", "0xb46a28b6f55540f89444f63de0378e3d121be09e06cc9ded1c20e65876d36aa0", "0xc65e9645644786b620e2dd2ad648ddfcbf4a7e5b1a3a4ecfe7f64667a3f0b7e2", "0xf4418588ed35a2458cffeb39b93d26f18d2ab13bdce6aee58e7b99359ec2dfd9", "0x5a9c16dc00d6ef18b7933a6f8dc65ccb55667138776f7dea101070dc8796e377", "0x4df84f40ae0c8229d0d6069e5c8f39a7c299677a09d367fc7b05e3bc380ee652", "0xcdc72595f74c7b1043d0e1ffbab734648c838dfb0527d971b602bc216c9619ef", "0x0abf5ac974a1ed57f4050aa510dd9c74f508277b39d7973bb2dfccc5eeb0618d", "0xb8cd74046ff337f0a7bf2c8e03e10f642c1886798d71806ab1e888d9e5ee87d0", "0x838c5655cb21c6cb83313b5a631175dff4963772cce9108188b34ac87c81c41e", "0x662ee4dd2dd7b2bc707961b1e646c4047669dcb6584f0d8d770daf5d7e7deb2e", "0x388ab20e2573d171a88108e79d820e98f26c0b84aa8b2f4aa4968dbb818ea322", "0x93237c50ba75ee485f4c22adf2f741400bdf8d6a9cc7df7ecae576221665d735", "0x8448818bb4ae4562849e949e17ac16e0be16688e156b5cf15e098c627c0056a9"}
	for i := 0; i < len(siblings); i++ {
		require.Equal(t, expectedProof[i], "0x"+hex.EncodeToString(siblings[i][:]))
	}
}

func TestAddLeaf(t *testing.T) {
	data, err := os.ReadFile("../tests/vectors/l1infotree/proof-vectors.json")
	require.NoError(t, err)
	var mtTestVectors []l1infotree2.L1InfoTreeProof
	err = json.Unmarshal(data, &mtTestVectors)
	require.NoError(t, err)
	testVector := mtTestVectors[3]
	var leaves [][32]byte
	mt, err := l1infotree.NewL1InfoTree(uint8(32), leaves)
	require.NoError(t, err)
	for _, leaf := range testVector.Leaves {
		_, count, _ := mt.GetCurrentRootCountAndSiblings()
		_, err := mt.AddLeaf(count, leaf)
		require.NoError(t, err)
	}
	log.Debugf("%d leaves added successfully", len(testVector.Leaves))
	root, _, _ := mt.GetCurrentRootCountAndSiblings()
	require.Equal(t, testVector.Root, root)
	log.Debug("Final root: ", root)
}

func TestAddLeaf2(t *testing.T) {
	data, err := os.ReadFile("../tests/vectors/l1infotree/root-vectors.json")
	require.NoError(t, err)
	var mtTestVectors []l1infotree2.L1InfoTree
	err = json.Unmarshal(data, &mtTestVectors)
	require.NoError(t, err)
	for _, testVector := range mtTestVectors {
		input := testVector.PreviousLeafValues

		var leaves [][32]byte
		for _, v := range input {
			leaves = append(leaves, v)
		}
		mt, err := l1infotree.NewL1InfoTree(uint8(32), leaves)
		require.NoError(t, err)

		initialRoot, count, _ := mt.GetCurrentRootCountAndSiblings()
		require.Equal(t, testVector.CurrentRoot, initialRoot)

		newRoot, err := mt.AddLeaf(count, testVector.NewLeafValue)
		require.NoError(t, err)
		require.Equal(t, testVector.NewRoot, newRoot)
	}
}

func TestLeafExists(t *testing.T) {
	data, err := os.ReadFile("../tests/vectors/l1infotree/proof-vectors.json")
	require.NoError(t, err)
	var mtTestVectors []l1infotree2.L1InfoTreeProof
	err = json.Unmarshal(data, &mtTestVectors)
	require.NoError(t, err)
	testVector := mtTestVectors[3]
	var leaves [][32]byte
	mt, err := l1infotree.NewL1InfoTree(uint8(32), leaves)
	require.NoError(t, err)
	for _, leaf := range testVector.Leaves {
		_, count, _ := mt.GetCurrentRootCountAndSiblings()
		_, err := mt.AddLeaf(count, leaf)
		require.NoError(t, err)
	}
	log.Debugf("%d leaves added successfully", len(testVector.Leaves))
	root, _, _ := mt.GetCurrentRootCountAndSiblings()
	require.Equal(t, testVector.Root, root)
	log.Debug("Final root: ", root)

	for _, leaf := range testVector.Leaves {
		exists := mt.LeafExists(leaf)
		require.True(t, exists)
	}

	nonExistentLeaf := common.HexToHash("0x1234")
	exists := mt.LeafExists(nonExistentLeaf)
	require.False(t, exists)
}
