package state

import (
	"github.com/ledgerwatch/erigon/metrics/methelp"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
)

func (b *BeaconState) EncodeSSZ(buf []byte) ([]byte, error) {
	h := methelp.NewHistTimer("encode_ssz_beacon_state_dur")
	bts, err := b.BeaconState.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	h.PutSince()
	sz := methelp.NewHistTimer("encode_ssz_beacon_state_size")
	sz.Update(float64(len(bts)))
	return bts, err
}

func (b *BeaconState) DecodeSSZ(buf []byte, version int) error {
	h := methelp.NewHistTimer("decode_ssz_beacon_state_dur")
	if err := b.BeaconState.DecodeSSZ(buf, version); err != nil {
		return err
	}
	sz := methelp.NewHistTimer("decode_ssz_beacon_state_size")
	sz.Update(float64(len(buf)))
	h.PutSince()
	return b.initBeaconState()
}

// SSZ size of the Beacon State
func (b *BeaconState) EncodingSizeSSZ() (size int) {
	sz := b.BeaconState.EncodingSizeSSZ()
	return sz
}

func (b *BeaconState) Clone() clonable.Clonable {
	return New(b.BeaconConfig())
}
