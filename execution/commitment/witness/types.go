package witness

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type AccountWithAddress struct {
	Address common.Address
	Account *accounts.Account
}

type CodeWithHash struct {
	Code     []byte
	CodeHash common.Hash
}
