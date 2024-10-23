package witness

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types/accounts"
)

type AccountWithAddress struct {
	Address libcommon.Address
	Account *accounts.Account
}

type CodeWithHash struct {
	Code     []byte
	CodeHash libcommon.Hash
}
