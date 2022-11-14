package cldb

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func EncodeSlot(n uint64) []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, uint32(n))
	return ret
}

// WriteBeaconState writes beacon state for specific block to database.
func WriteBeaconState(tx kv.Putter, state *cltypes.BeaconState) error {
	data, err := utils.EncodeSSZSnappy(state)
	if err != nil {
		return err
	}

	return tx.Put(kv.BeaconState, EncodeSlot(state.Slot), data)
}

// ReadBeaconState reads beacon state for specific block from database.
func ReadBeaconState(tx kv.Getter, slot uint64) (*cltypes.BeaconState, error) {
	data, err := tx.GetOne(kv.BeaconState, EncodeSlot(slot))
	if err != nil {
		return nil, err
	}
	state := &cltypes.BeaconState{}

	if len(data) == 0 {
		return nil, nil
	}

	if err := utils.DecodeSSZSnappy(state, data); err != nil {
		return nil, err
	}

	return state, nil
}
