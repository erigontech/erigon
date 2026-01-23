package txn

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func Test_LegacyTx_Timeboosted(t *testing.T) {
	timeboostedVals := []bool{true, false}
	for i := 0; i < 2; i++ {
		two := uint256.NewInt(2)
		ltx := types.NewTransaction(4, common.HexToAddress("0x2"), two, 21000, two, []byte("data"))
		ltx.Timeboosted = timeboostedVals[i]

		buf := bytes.NewBuffer(nil)
		err := ltx.EncodeRLP(buf)
		require.NoError(t, err)

		var ltx2 types.LegacyTx
		stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), uint64(buf.Len()))
		err = ltx2.DecodeRLP(stream)
		require.NoError(t, err)

		require.EqualValues(t, ltx.Timeboosted, ltx2.Timeboosted)
		require.EqualValues(t, ltx.GasLimit, ltx2.GasLimit)
		require.EqualValues(t, ltx.GasPrice.Bytes(), ltx2.GasPrice.Bytes())
		require.EqualValues(t, ltx.Value.Bytes(), ltx2.Value.Bytes())
		require.EqualValues(t, ltx.Data, ltx2.Data)
		require.EqualValues(t, ltx.To, ltx2.To)

		require.EqualValues(t, timeboostedVals[i], ltx2.IsTimeBoosted())
	}
}

func Test_DynamicFeeTx_Timeboosted(t *testing.T) {
	timeboostedVals := []bool{true, false}
	for i := 0; i < 2; i++ {
		two := uint256.NewInt(2)
		three := uint256.NewInt(3)
		chainID := uint256.NewInt(1)
		accessList := types.AccessList{
			{Address: common.HexToAddress("0x1"), StorageKeys: []common.Hash{common.HexToHash("0x01")}},
		}

		tx := &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    4,
				To:       &common.Address{0x2},
				Value:    two,
				GasLimit: 21000,
				Data:     []byte("data"),
			},
			ChainID:     chainID,
			TipCap:      two,
			FeeCap:      three,
			AccessList:  accessList,
			Timeboosted: timeboostedVals[i],
		}

		buf := bytes.NewBuffer(nil)
		err := tx.EncodeRLP(buf)
		require.NoError(t, err)

		// Decode using DecodeRLPTransaction pattern
		stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		decoded, err := types.DecodeRLPTransaction(stream, false)
		require.NoError(t, err)

		tx2, ok := decoded.(*types.DynamicFeeTransaction)
		require.True(t, ok)

		require.EqualValues(t, tx.Timeboosted, tx2.Timeboosted)
		require.EqualValues(t, tx.GasLimit, tx2.GasLimit)
		require.EqualValues(t, tx.TipCap.Bytes(), tx2.TipCap.Bytes())
		require.EqualValues(t, tx.FeeCap.Bytes(), tx2.FeeCap.Bytes())
		require.EqualValues(t, tx.Value.Bytes(), tx2.Value.Bytes())
		require.EqualValues(t, tx.Data, tx2.Data)
		require.EqualValues(t, tx.To, tx2.To)
		require.EqualValues(t, tx.ChainID.Bytes(), tx2.ChainID.Bytes())
		require.EqualValues(t, len(tx.AccessList), len(tx2.AccessList))
		require.EqualValues(t, timeboostedVals[i], tx.IsTimeBoosted())
	}
}

func Test_AccessListTx_Timeboosted(t *testing.T) {
	timeboostedVals := []bool{true, false}
	for i := 0; i < 2; i++ {
		two := uint256.NewInt(2)
		chainID := uint256.NewInt(1)
		accessList := types.AccessList{
			{Address: common.HexToAddress("0x1"), StorageKeys: []common.Hash{common.HexToHash("0x01")}},
		}

		tx := &types.AccessListTx{
			LegacyTx: types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    4,
					To:       &common.Address{0x2},
					Value:    two,
					GasLimit: 21000,
					Data:     []byte("data"),
				},
				GasPrice: two,
			},
			ChainID:     chainID,
			AccessList:  accessList,
			Timeboosted: timeboostedVals[i],
		}

		buf := bytes.NewBuffer(nil)
		err := tx.EncodeRLP(buf)
		require.NoError(t, err)

		// Decode using DecodeRLPTransaction pattern
		stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		decoded, err := types.DecodeRLPTransaction(stream, false)
		require.NoError(t, err)

		tx2, ok := decoded.(*types.AccessListTx)
		require.True(t, ok)

		require.EqualValues(t, tx.Timeboosted, tx2.Timeboosted)
		require.EqualValues(t, tx.GasLimit, tx2.GasLimit)
		require.EqualValues(t, tx.GasPrice.Bytes(), tx2.GasPrice.Bytes())
		require.EqualValues(t, tx.Value.Bytes(), tx2.Value.Bytes())
		require.EqualValues(t, tx.Data, tx2.Data)
		require.EqualValues(t, tx.To, tx2.To)
		require.EqualValues(t, tx.ChainID.Bytes(), tx2.ChainID.Bytes())
		require.EqualValues(t, len(tx.AccessList), len(tx2.AccessList))
		require.EqualValues(t, timeboostedVals[i], tx.IsTimeBoosted())
	}
}

func Test_BlobTx_Timeboosted(t *testing.T) {
	timeboostedVals := []bool{true, false}
	for i := 0; i < 2; i++ {
		two := uint256.NewInt(2)
		three := uint256.NewInt(3)
		chainID := uint256.NewInt(1)
		maxFeePerBlobGas := uint256.NewInt(5)
		accessList := types.AccessList{
			{Address: common.HexToAddress("0x1"), StorageKeys: []common.Hash{common.HexToHash("0x01")}},
		}
		blobHashes := []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")}

		tx := &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    4,
					To:       &common.Address{0x2},
					Value:    two,
					GasLimit: 21000,
					Data:     []byte("data"),
				},
				ChainID:     chainID,
				TipCap:      two,
				FeeCap:      three,
				AccessList:  accessList,
				Timeboosted: timeboostedVals[i],
			},
			MaxFeePerBlobGas:    maxFeePerBlobGas,
			BlobVersionedHashes: blobHashes,
		}

		buf := bytes.NewBuffer(nil)
		err := tx.EncodeRLP(buf)
		require.NoError(t, err)

		// Decode using DecodeRLPTransaction pattern
		stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		decoded, err := types.DecodeRLPTransaction(stream, false)
		require.NoError(t, err)

		tx2, ok := decoded.(*types.BlobTx)
		require.True(t, ok)

		require.EqualValues(t, tx.Timeboosted, tx2.Timeboosted)
		require.EqualValues(t, tx.GasLimit, tx2.GasLimit)
		require.EqualValues(t, tx.TipCap.Bytes(), tx2.TipCap.Bytes())
		require.EqualValues(t, tx.FeeCap.Bytes(), tx2.FeeCap.Bytes())
		require.EqualValues(t, tx.Value.Bytes(), tx2.Value.Bytes())
		require.EqualValues(t, tx.Data, tx2.Data)
		require.EqualValues(t, tx.To, tx2.To)
		require.EqualValues(t, tx.ChainID.Bytes(), tx2.ChainID.Bytes())
		require.EqualValues(t, tx.MaxFeePerBlobGas.Bytes(), tx2.MaxFeePerBlobGas.Bytes())
		require.EqualValues(t, len(tx.AccessList), len(tx2.AccessList))
		require.EqualValues(t, len(tx.BlobVersionedHashes), len(tx2.BlobVersionedHashes))
		require.EqualValues(t, timeboostedVals[i], tx.IsTimeBoosted())
	}
}

func Test_SetCodeTx_Timeboosted(t *testing.T) {
	timeboostedVals := []bool{true, false}
	for i := 0; i < 2; i++ {
		two := uint256.NewInt(2)
		three := uint256.NewInt(3)
		chainID := uint256.NewInt(1)
		accessList := types.AccessList{
			{Address: common.HexToAddress("0x1"), StorageKeys: []common.Hash{common.HexToHash("0x01")}},
		}

		auth := types.Authorization{
			ChainID: *chainID,
			Address: common.HexToAddress("0x3"),
			Nonce:   1,
		}

		tx := &types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    4,
					To:       &common.Address{0x2},
					Value:    two,
					GasLimit: 21000,
					Data:     []byte("data"),
				},
				ChainID:     chainID,
				TipCap:      two,
				FeeCap:      three,
				AccessList:  accessList,
				Timeboosted: timeboostedVals[i],
			},
			Authorizations: []types.Authorization{auth},
		}

		buf := bytes.NewBuffer(nil)
		err := tx.EncodeRLP(buf)
		require.NoError(t, err)

		// Decode using DecodeRLPTransaction pattern
		stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		decoded, err := types.DecodeRLPTransaction(stream, false)
		require.NoError(t, err)

		tx2, ok := decoded.(*types.SetCodeTransaction)
		require.True(t, ok)

		require.EqualValues(t, tx.Timeboosted, tx2.Timeboosted)
		require.EqualValues(t, tx.GasLimit, tx2.GasLimit)
		require.EqualValues(t, tx.TipCap.Bytes(), tx2.TipCap.Bytes())
		require.EqualValues(t, tx.FeeCap.Bytes(), tx2.FeeCap.Bytes())
		require.EqualValues(t, tx.Value.Bytes(), tx2.Value.Bytes())
		require.EqualValues(t, tx.Data, tx2.Data)
		require.EqualValues(t, tx.To, tx2.To)
		require.EqualValues(t, tx.ChainID.Bytes(), tx2.ChainID.Bytes())
		require.EqualValues(t, len(tx.AccessList), len(tx2.AccessList))
		require.EqualValues(t, len(tx.Authorizations), len(tx2.Authorizations))
		require.EqualValues(t, timeboostedVals[i], tx.IsTimeBoosted())
	}
}

func Test_ArbRetryTx_Timeboosted(t *testing.T) {
	timeboostedVals := []bool{true, false}
	for i := 0; i < 2; i++ {
		two := big.NewInt(2)
		chainID := big.NewInt(1)
		ticketId := common.HexToHash("0x123")
		toAddr := common.HexToAddress("0x2")

		tx := &ArbitrumRetryTx{
			ChainId:             chainID,
			Nonce:               4,
			From:                common.HexToAddress("0x1"),
			GasFeeCap:           two,
			Gas:                 21000,
			To:                  &toAddr,
			Value:               two,
			Data:                []byte("data"),
			TicketId:            ticketId,
			RefundTo:            common.HexToAddress("0x3"),
			MaxRefund:           two,
			SubmissionFeeRefund: two,
			Timeboosted:         timeboostedVals[i],
		}

		buf := bytes.NewBuffer(nil)
		err := tx.EncodeRLP(buf)
		require.NoError(t, err)

		// Decode using DecodeRLPTransaction pattern
		stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), 0)
		decoded, err := types.DecodeRLPTransaction(stream, false)
		require.NoError(t, err)

		tx2, ok := decoded.(*ArbitrumRetryTx)
		require.True(t, ok)

		require.EqualValues(t, tx.Timeboosted, tx2.Timeboosted)
		require.EqualValues(t, tx.Gas, tx2.Gas)
		require.EqualValues(t, tx.GasFeeCap, tx2.GasFeeCap)
		require.EqualValues(t, tx.Value, tx2.Value)
		require.EqualValues(t, tx.Data, tx2.Data)
		require.EqualValues(t, tx.To, tx2.To)
		require.EqualValues(t, tx.From, tx2.From)
		require.EqualValues(t, tx.Nonce, tx2.Nonce)
		require.EqualValues(t, tx.ChainId, tx2.ChainId)
		require.EqualValues(t, tx.TicketId, tx2.TicketId)
		require.EqualValues(t, tx.RefundTo, tx2.RefundTo)
		require.EqualValues(t, tx.MaxRefund, tx2.MaxRefund)
		require.EqualValues(t, tx.SubmissionFeeRefund, tx2.SubmissionFeeRefund)
		require.EqualValues(t, timeboostedVals[i], tx.IsTimeBoosted())

	}
}
