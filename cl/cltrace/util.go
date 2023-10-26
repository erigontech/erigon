package cltrace

import "github.com/ledgerwatch/erigon/cl/abstract"

type InvocationHandler interface {
	Invoke(method string, args []any) (retvals []any, intercept bool)
}

var _ abstract.BeaconState = (*BeaconStateProxy)(nil)

func (b *BeaconStateProxy) Copy() (abstract.BeaconState, error) {
	c, err := b.Underlying.Copy()
	if err != nil {
		return nil, err
	}
	return &BeaconStateProxy{
		Handler:    b.Handler,
		Underlying: c,
	}, nil
}
