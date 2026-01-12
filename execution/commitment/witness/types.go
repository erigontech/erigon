package witness

import (
<<<<<<<< HEAD:execution/types/witness/types.go
	"github.com/erigontech/erigon-lib/common"
========
	"github.com/erigontech/erigon/common"
>>>>>>>> main:execution/commitment/witness/types.go
	"github.com/erigontech/erigon/execution/types/accounts"
)

type AccountWithAddress struct {
	Address common.Address
	Account *accounts.Account
}

type CodeWithHash struct {
	Code     []byte
	CodeHash accounts.CodeHash
}
