package witness

import (
<<<<<<<< HEAD:execution/commitment/witness/types.go
	"github.com/erigontech/erigon/common"
========
	"github.com/erigontech/erigon-lib/common"
>>>>>>>> arbitrum:execution/types/witness/types.go
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
