package ssz_snappy

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sync"

	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/libp2p/go-libp2p/core/network"
)

func EncodePacket(pkt proto.Packet, stream network.Stream) ([]byte, *snappy.Writer, error) {
	if val, ok := pkt.(ssz.Marshaler); ok {
		wr := bufio.NewWriter(stream)
		sw := snappy.NewWriter(wr)
		p := make([]byte, 10)

		vin := binary.PutVarint(p, int64(val.SizeSSZ()))

		enc, err := val.MarshalSSZ()
		if err != nil {
			return nil, nil, fmt.Errorf("marshal ssz: %w", err)
		}

		if len(enc) > int(clparams.MaxChunkSize) {
			return nil, nil, fmt.Errorf("chunk size too big")
		}

		_, err = wr.Write(p[:vin])
		if err != nil {
			return nil, nil, fmt.Errorf("write varint: %w", err)
		}

		return enc, sw, nil
	}

	return nil, nil, fmt.Errorf("packet %s does not implement ssz.Marshaler", reflect.TypeOf(pkt))
}

var bp = new(BufferPool)

const MaxLength = math.MaxInt32

type BufferPool struct {
	pools [32]sync.Pool // a list of singlePools
	ptrs  sync.Pool
}

type bufp struct {
	buf []byte
}

func (p *BufferPool) Get(length int) []byte {
	if length == 0 {
		return nil
	}
	// Calling this function with a negative length is invalid.
	// make will panic if length is negative, so we don't have to.
	if length > MaxLength || length < 0 {
		return make([]byte, length)
	}
	idx := nextLogBase2(uint32(length))
	if ptr := p.pools[idx].Get(); ptr != nil {
		bp := ptr.(*bufp)
		buf := bp.buf[:uint32(length)]
		bp.buf = nil
		p.ptrs.Put(ptr)
		return buf
	}
	return make([]byte, 1<<idx)[:uint32(length)]
}

func (p *BufferPool) Put(buf []byte) {
	capacity := cap(buf)
	if capacity == 0 || capacity > MaxLength {
		return // drop it
	}
	idx := prevLogBase2(uint32(capacity))
	var bp *bufp
	if ptr := p.ptrs.Get(); ptr != nil {
		bp = ptr.(*bufp)
	} else {
		bp = new(bufp)
	}
	bp.buf = buf
	p.pools[idx].Put(bp)
}
func nextLogBase2(v uint32) uint32 {
	return uint32(bits.Len32(v - 1))
}
func prevLogBase2(num uint32) uint32 {
	next := nextLogBase2(num)
	if num == (1 << next) {
		return next
	}
	return next - 1
}
