package getters

import (
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type ExecutionBlockReaderByNumber interface {
	TransactionsSSZ(w io.Writer, number uint64, hash libcommon.Hash) error
	WithdrawalsSZZ(w io.Writer, number uint64, hash libcommon.Hash) error
}
