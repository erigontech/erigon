package ssz_snappy

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"reflect"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/libp2p/go-libp2p/core/network"
)

func EncodePacket(pkt proto.Packet, stream network.Stream) ([]byte, error) {
	if val, ok := pkt.(ssz.Marshaler); ok {
		wr := bufio.NewWriter(stream)
		p := make([]byte, val.SizeSSZ())
		vin := binary.PutVarint(p, int64(val.SizeSSZ()))
		enc, err := val.MarshalSSZ()
		if err != nil {
			return nil, fmt.Errorf("marshal ssz: %w", err)
		}
		if len(enc) > int(clparams.MaxChunkSize) {
			return nil, fmt.Errorf("chunk size too big")
		}
		_, err = wr.Write(p[:vin])
		if err != nil {
			return nil, fmt.Errorf("write varint: %w", err)
		}
		return enc, nil
	}

	return nil, fmt.Errorf("packet %s does not implement ssz.Marshaler", reflect.TypeOf(pkt))
}
