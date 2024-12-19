package generics

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
)

func Empty[T any]() (t T) {
	return
}

type Response struct {
	Headers []*types.Header
	Hashes  []libcommon.Hash
}
