package cldb

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func EncodeSlot(n uint64) []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret[:], uint32(n))
	return ret
}

// TODO: make personalized mdbx table for beacon state
func WriteBeaconState(tx kv.Putter, state *cltypes.BeaconState) error {
	data, err := utils.EncodeSSZSnappy(state)
	if err != nil {
		return err
	}

	fmt.Printf("Bytes written in state: %d bytes\n", len(data)+4)

	return tx.Put(kv.Headers, EncodeSlot(state.Slot), data)
}

// TODO: make personalized mdbx table for beacon state
func ReadBeaconState(tx kv.Getter, slot uint64) (*cltypes.BeaconState, error) {
	data, err := tx.GetOne(kv.Headers, EncodeSlot(slot))
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
