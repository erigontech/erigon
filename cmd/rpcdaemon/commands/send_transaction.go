package commands

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// SendRawTransaction implements eth_sendRawTransaction. Creates new message call transaction or a contract creation for previously-signed transactions.
func (api *APIImpl) SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error) {
	tx, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(encodedTx), uint64(len(encodedTx))))
	if err != nil {
		return common.Hash{}, err
	}
	hash := tx.Hash()
	res, err := api.txPool.Add(ctx, &txpool.AddRequest{RlpTxs: [][]byte{encodedTx}, IsLocal: true})
	if err != nil {
		return common.Hash{}, err
	}
	switch res.Imported[0] {
	case txpool.ImportResult_SUCCESS:
		return hash, nil
	case txpool.ImportResult_ALREADY_EXISTS:
		return hash, nil
	case txpool.ImportResult_FEE_TOO_LOW:
		return hash, fmt.Errorf("fee too low")
	case txpool.ImportResult_STALE:
		return hash, fmt.Errorf("tx is stale")
	case txpool.ImportResult_INVALID:
		return hash, fmt.Errorf("tx is invalid")
	case txpool.ImportResult_INTERNAL_ERROR:
		return hash, fmt.Errorf("internal error happened, see TxPool service logs")
	}
	return hash, err
}

// SendTransaction implements eth_sendTransaction. Creates new message call transaction or a contract creation if the data field contains code.
func (api *APIImpl) SendTransaction(_ context.Context, txObject interface{}) (common.Hash, error) {
	return common.Hash{0}, fmt.Errorf(NotImplemented, "eth_sendTransaction")
}
