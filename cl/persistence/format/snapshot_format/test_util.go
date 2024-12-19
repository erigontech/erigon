package snapshot_format

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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
