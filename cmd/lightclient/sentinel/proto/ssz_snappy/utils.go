package ssz_snappy

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"gfx.cafe/util/go/bufpool"
	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
)

func EncodePacket(pkt proto.Packet) ([]byte, error) {
	if val, ok := pkt.(ssz.Marshaler); ok {
		sz := val.SizeSSZ()

		bp := bufpool.Get(sz)
		defer bufpool.Put(bp)

		p := bp.Bytes()
		p = append(p, make([]byte, 10)...)

		vin := binary.PutVarint(p, int64(sz))
		_, err := val.MarshalSSZTo(p[vin:])
		if err != nil {
			return nil, fmt.Errorf("marshal ssz: %w", err)
		}
	}

	return nil, fmt.Errorf("packet %s does not implement ssz.Marshaler", reflect.TypeOf(pkt))
}
