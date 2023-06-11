package eth2

import "github.com/ledgerwatch/erigon/cl/transition/machine"

type Impl = impl

var _ machine.Interface = (*impl)(nil)

type impl struct {
	FullValidation bool
}
