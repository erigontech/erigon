package vtree

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/gballet/go-verkle"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerkleUnordered(t *testing.T) {
	listKeys := [][]byte{common.Hex2Bytes("00000005f3b0bda97267951a939c1e2541e28551991fab1aa1c4a92d8fd80b00"),
		common.Hex2Bytes("00000005f3b0bda97267951a939c1e2541e28551991fab1aa1c4a92d8fd80b01"), common.Hex2Bytes("00000005f3b0bda97267951a939c1e2541e28551991fab1aa1c4a92d8fd80b02"),
		common.Hex2Bytes("00000005f3b0bda97267951a939c1e2541e28551991fab1aa1c4a92d8fd80b03"), common.Hex2Bytes("00000005f3b0bda97267951a939c1e2541e28551991fab1aa1c4a92d8fd80b04"),
		common.Hex2Bytes("00000008ea8ceb604a0ff8867a8445f3323a926cbe7ddd83d754820c7658ef00"), common.Hex2Bytes("00000008ea8ceb604a0ff8867a8445f3323a926cbe7ddd83d754820c7658ef01"),
		common.Hex2Bytes("00000008ea8ceb604a0ff8867a8445f3323a926cbe7ddd83d754820c7658ef02"), common.Hex2Bytes("00000008ea8ceb604a0ff8867a8445f3323a926cbe7ddd83d754820c7658ef03"),
		common.Hex2Bytes("00000008ea8ceb604a0ff8867a8445f3323a926cbe7ddd83d754820c7658ef04"), common.Hex2Bytes("0000000e675d53bc1055291f3c49c582b4d67e70f195033a0573b05fe1e7e4f2"),
		common.Hex2Bytes("0000000e675d53bc1055291f3c49c582b4d67e70f195033a0573b05fe1e7e4f3"), common.Hex2Bytes("0000000e675d53bc1055291f3c49c582b4d67e70f195033a0573b05fe1e7e4f4"),
		common.Hex2Bytes("0000000e675d53bc1055291f3c49c582b4d67e70f195033a0573b05fe1e7e4f5"), common.Hex2Bytes("00000012c7774be0e265e08c24ed47ba01b0cb429421c86dd724ac97cdff24f9"),
		common.Hex2Bytes("000000139337aba7797d43e47a8fb1801ffb53f59817119161b41238dd9d5c4e"), common.Hex2Bytes("0000001909c18d004f6f1be18c40cf5ff68baff7ecb616db615c8752e265d785"), common.Hex2Bytes("000000253ddcddb876cab6a759dd8e5cda429ccc831bd947bf6bb22b7ce5ec00"),
	}
	root := verkle.New()
	for _, l := range listKeys {
		fmt.Println(common.Bytes2Hex(l))
		require.NoError(t, root.InsertOrdered(l, []byte{0}, nil))
	}
	panic("a")
}

func TestVerkleAccount(t *testing.T) {
	acc := VerkleAccount{
		Balance:  big.NewInt(2935584985),
		Nonce:    100000,
		CodeHash: common.HexToHash("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
	}
	encodingSize := acc.GetVerkleAccountSizeForStorage()
	enc := make([]byte, encodingSize)
	acc.EncodeVerkleAccountForStorage(enc)
	fmt.Println(common.Bytes2Hex(enc))
	newAcc := DecodeVerkleAccountForStorage(enc)
	assert.Equal(t, acc.Nonce, newAcc.Nonce)
	assert.Equal(t, acc.CodeHash, newAcc.CodeHash)
	assert.Equal(t, acc.Balance.String(), newAcc.Balance.String())
}

func TestVerkleAccountEmptyCodeHash(t *testing.T) {
	acc := VerkleAccount{
		Balance:  big.NewInt(2935584985),
		Nonce:    100000,
		CodeHash: emptyCodeHash,
	}
	encodingSize := acc.GetVerkleAccountSizeForStorage()
	assert.Equal(t, encodingSize, 10)
	enc := make([]byte, encodingSize)
	acc.EncodeVerkleAccountForStorage(enc)
	newAcc := DecodeVerkleAccountForStorage(enc)
	assert.Equal(t, acc.Nonce, newAcc.Nonce)
	assert.Equal(t, acc.CodeHash, emptyCodeHash)
	assert.Equal(t, acc.Balance.String(), newAcc.Balance.String())
}
