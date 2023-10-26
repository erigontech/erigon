package cltrace

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/abstract/proxystate"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/clvm"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
)

type StateTracer struct {
	enc clvm.Encoder
}

func NewStateTracer(enc clvm.Encoder) *StateTracer {
	return &StateTracer{enc: enc}
}

func (t *StateTracer) TransitionState(s abstract.BeaconState, block *cltypes.SignedBeaconBlock, fullValidation bool) error {
	cvm := &eth2.Impl{FullValidation: fullValidation}
	t.enc.Encode(
		clvm.NewInstruction([]byte("#block"), binary.LittleEndian.AppendUint64(nil, block.Block.Slot)),
	)
	s = &proxystate.BeaconStateProxy{
		Handler: proxystate.InvocationHandlerFunc(func(method string, args []any) (retvals []any, intercept bool) {
			if proxystate.IsWriteOp(method) {
				xss := [][]byte{}
				for _, v := range args {
					bts, err := ssz2.MarshalSSZ(nil, v)
					if err != nil {
						panic(fmt.Sprintf("marshalssz cant encode %s", reflect.TypeOf(v)))
					}
					xss = append(xss, bts)
				}
				t.enc.Encode(clvm.NewInstruction([]byte(method), xss...))
			}
			return nil, false
		}),
		Underlying: s,
	}
	err := machine.TransitionState(cvm, s, block)
	if err != nil {
		t.enc.Discard()
		return err
	}
	return t.enc.NextCycle()
}
