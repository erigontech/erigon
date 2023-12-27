package snapshot_format

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

type MockBlockReader struct {
	Block *cltypes.Eth1Block
}

func (t *MockBlockReader) Withdrawals(number uint64, hash libcommon.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
	return t.Block.Withdrawals, nil
}

func (t *MockBlockReader) Transactions(number uint64, hash libcommon.Hash) (*solid.TransactionsSSZ, error) {
	return t.Block.Transactions, nil
}
