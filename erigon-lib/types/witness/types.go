package witness

import (
	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/types/accounts"
)

type AccountWithAddress struct {
	Address common.Address
	Account *accounts.Account
}

type CodeWithHash struct {
	Code     []byte
	CodeHash common.Hash
}
