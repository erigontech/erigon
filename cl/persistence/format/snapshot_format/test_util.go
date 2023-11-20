package snapshot_format

import (
	"io"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	libcommon "github.com/ledgerwatch/erigon/erigon-lib/common"
)

type MockBlockReader struct {
	Block *cltypes.Eth1Block
}

func (t *MockBlockReader) WithdrawalsSZZ(out io.Writer, number uint64, hash libcommon.Hash) error {
	l, err := t.Block.Withdrawals.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	_, err = out.Write(l)
	return err
}

func (t *MockBlockReader) TransactionsSSZ(out io.Writer, number uint64, hash libcommon.Hash) error {
	l, err := t.Block.Transactions.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	_, err = out.Write(l)
	return err
}
