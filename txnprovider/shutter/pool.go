package shutter

import (
	"github.com/erigontech/erigon/txnprovider"
)

type Pool struct {
}

func (p Pool) Yield() (txnprovider.YieldResult, error) {
	//
	// TODO
	//
	return txnprovider.YieldResult{}, nil
}
