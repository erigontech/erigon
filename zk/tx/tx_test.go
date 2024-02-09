package tx

import (
	"encoding/hex"
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecodeRandomBatchL2Data(t *testing.T) {
	randomData := []byte("Random data")
	txs, _, _, err := DecodeTxs(randomData, forkID5)
	require.Error(t, err)
	assert.Equal(t, []types.Transaction{}, txs)
	t.Log("Txs decoded 1: ", txs)

	randomData = []byte("Esto es autentica basura")
	txs, _, _, err = DecodeTxs(randomData, forkID5)
	require.Error(t, err)
	assert.Equal(t, []types.Transaction{}, txs)
	t.Log("Txs decoded 2: ", txs)

	randomData = []byte("beef")
	txs, _, _, err = DecodeTxs(randomData, forkID5)
	require.Error(t, err)
	assert.Equal(t, []types.Transaction{}, txs)
	t.Log("Txs decoded 3: ", txs)
}

func TestDecodePre155BatchL2DataPreForkID5(t *testing.T) {
	pre155, err := hex.DecodeString("e480843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77cb7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed1b")
	require.NoError(t, err)
	txs, _, _, err := DecodeTxs(pre155, forkID4)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs, len(txs))
	assert.Equal(t, 1, len(txs))
	v, r, s := txs[0].RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), txs[0].GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())

	pre155, err = hex.DecodeString("e580843b9aca00830186a0941275fbb540c8efc58b812ba83b0d0b8b9917ae988084159278193d7bcd98c00060650f12c381cc2d4f4cc8abf54059aecd2c7aabcfcdd191ba6827b1e72f0eb0b8d5daae64962f4aafde7853e1c102de053edbedf066e6e3c2dc1b")
	require.NoError(t, err)
	txs, _, _, err = DecodeTxs(pre155, forkID4)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(0), txs[0].GetValue())
	assert.Equal(t, "15927819", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(100000), txs[0].GetGas())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())
}

func TestDecodePre155Tx(t *testing.T) {
	pre155 := "0xf86780843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77c1ba0b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feba0246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed"
	pre155Bytes, err := hex.DecodeString(pre155[2:])
	require.NoError(t, err)
	tx, _, err := DecodeTx(pre155Bytes, 0, forkID4)
	require.NoError(t, err)
	v, r, s := tx.RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", tx.GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), tx.GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(tx.GetData()))
	assert.Equal(t, uint64(0), tx.GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), tx.GetPrice())
}

func TestDecodePost155Tx(t *testing.T) {
	post155 := "0xf86780843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77c1ba0b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feba0246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed"
	post155Bytes, err := hex.DecodeString(post155[2:])
	require.NoError(t, err)
	//post155Bytes = append(post155Bytes, 75)
	tx, pct, err := DecodeTx(post155Bytes, 75, forkID5)
	require.NoError(t, err)
	v, r, s := tx.RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", tx.GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), tx.GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(tx.GetData()))
	assert.Equal(t, uint64(0), tx.GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), tx.GetPrice())
	assert.Equal(t, pct, uint8(0x4b))
}

func TestDecodePre155BatchL2DataForkID5(t *testing.T) {
	pre155, err := hex.DecodeString("e480843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77cb7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed1bff")
	require.NoError(t, err)
	txs, _, _, err := DecodeTxs(pre155, forkID5)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs, len(txs))
	assert.Equal(t, 1, len(txs))
	v, r, s := txs[0].RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), txs[0].GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())

	pre155, err = hex.DecodeString("e580843b9aca00830186a0941275fbb540c8efc58b812ba83b0d0b8b9917ae988084159278193d7bcd98c00060650f12c381cc2d4f4cc8abf54059aecd2c7aabcfcdd191ba6827b1e72f0eb0b8d5daae64962f4aafde7853e1c102de053edbedf066e6e3c2dc1b")
	require.NoError(t, err)
	txs, _, _, err = DecodeTxs(pre155, forkID4)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(0), txs[0].GetValue())
	assert.Equal(t, "15927819", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(100000), txs[0].GetGas())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())
}
