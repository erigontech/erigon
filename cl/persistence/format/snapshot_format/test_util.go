package snapshot_format

import "github.com/ledgerwatch/erigon/cl/cltypes"

type MockBlockReader struct {
	Block *cltypes.Eth1Block
}

func (t *MockBlockReader) BlockByNumber(number uint64) (*cltypes.Eth1Block, error) {
	return t.Block, nil
}
