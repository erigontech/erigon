package snapshot_format

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

type MockBlockReader struct {
	Block *cltypes.Eth1Block
}

func (t *MockBlockReader) BlockByNumber(number uint64, hash libcommon.Hash) (*cltypes.Eth1Block, error) {
	return t.Block, nil
}
