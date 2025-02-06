package shutter

import (
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
)

var ErrInvalidKeyperIndex = fmt.Errorf("invalid keyper index")

type EonIndex uint64

type Eon struct {
	Index           EonIndex
	ActivationBlock uint64
	Key             []byte
	Threshold       uint64
	Members         []libcommon.Address
}

func (e Eon) KeyperAt(index uint64) (libcommon.Address, error) {
	if index >= uint64(len(e.Members)) {
		return libcommon.Address{}, fmt.Errorf("%w: %d >= %d", ErrInvalidKeyperIndex, index, len(e.Members))
	}

	return e.Members[index], nil
}
