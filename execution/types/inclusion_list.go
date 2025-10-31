package types

import (
	"encoding/json"

	"github.com/erigontech/erigon-lib/common/hexutil"
)

type InclusionList [][]byte

func (inclusionList InclusionList) MarshalJSON() ([]byte, error) {
	inclusionListHex := make([]hexutil.Bytes, len(inclusionList))

	for i, tx := range inclusionList {
		inclusionListHex[i] = tx
	}
	return json.Marshal(inclusionListHex)
}

func (inclusionList *InclusionList) UnmarshalJSON(input []byte) error {
	var inclusionListHex []hexutil.Bytes
	if err := json.Unmarshal(input, &inclusionListHex); err != nil {
		return err
	}

	*inclusionList = make([][]byte, len(inclusionListHex))
	for i, tx := range inclusionListHex {
		(*inclusionList)[i] = tx
	}
	return nil
}

func ConvertInclusionListToTransactions(inclusionList InclusionList) (Transactions, error) {
	txs, err := DecodeTransactions(inclusionList)
	if err != nil {
		return nil, err
	}

	return txs, nil
}

func ConvertTransactionstoInclusionList(txs Transactions) (InclusionList, error) {
	return MarshalTransactionsBinary(txs)
}
