package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	txPoolProto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

var (
	ErrOnlyStarknetTx     = errors.New("only support starknet transactions")
	ErrOnlyContractDeploy = errors.New("only support contract creation")
)

// SendRawTransaction deploy new cairo contract
func (api *StarknetImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	txn, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(encodedTx), uint64(len(encodedTx))))

	if err != nil {
		return common.Hash{}, err
	}

	if !txn.IsStarkNet() {
		return common.Hash{}, ErrOnlyStarknetTx
	}

	if !txn.IsContractDeploy() {
		return common.Hash{}, ErrOnlyContractDeploy
	}

	hash := txn.Hash()
	res, err := api.txPool.Add(ctx, &txPoolProto.AddRequest{RlpTxs: [][]byte{encodedTx}})
	if err != nil {
		return common.Hash{}, err
	}

	if res.Imported[0] != txPoolProto.ImportResult_SUCCESS {
		return hash, fmt.Errorf("%s: %s", txPoolProto.ImportResult_name[int32(res.Imported[0])], res.Errors[0])
	}

	log.Info("Submitted contract creation", "hash", txn.Hash().Hex(), "nonce", txn.GetNonce(), "value", txn.GetValue())

	return txn.Hash(), nil
}
