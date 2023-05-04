package state

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/clonable"
)

func (b *BeaconState) EncodeSSZ(buf []byte) ([]byte, error) {
	return b.BeaconState.EncodeSSZ(buf)
}

func (b *BeaconState) DecodeSSZWithVersion(buf []byte, version int) error {
	if err := b.BeaconState.DecodeSSZWithVersion(buf, version); err != nil {
		return err
	}
	return b.initBeaconState()
}

// SSZ size of the Beacon State
func (b *BeaconState) EncodingSizeSSZ() (size int) {
	return b.BeaconState.EncodingSizeSSZ()
}

func (b *BeaconState) Clone() clonable.Clonable {
	return New(b.BeaconConfig())
}
