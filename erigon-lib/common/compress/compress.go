package compress

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// growslice ensures b has the wanted length by either expanding it to its capacity
// or allocating a new slice if b has insufficient capacity.
func growslice(b []byte, wantLength int) []byte {
	if cap(b) >= wantLength {
		return b[:wantLength]
	}
	return make([]byte, wantLength)
}

var (
	zstdEncPool = sync.Pool{
		New: func() interface{} {
			enc, _ := zstd.NewWriter(nil, zstd.WithEncoderCRC(false), zstd.WithZeroFrames(true))
			return enc
		},
	}
	zstdDecPool = sync.Pool{
		New: func() interface{} {
			dec, _ := zstd.NewReader(nil, zstd.IgnoreChecksum(true))
			return dec
		},
	}
)

// EncodeZstdIfNeed compresses v into buf if enabled, otherwise returns buf and v unchanged.
// It pre-allocates buf to ZSTD’s worst-case bound (src + src/255 + 16) and reuses encoders.
func EncodeZstdIfNeed(buf, v []byte, enabled bool) ([]byte, []byte) {
	if !enabled {
		return buf, v
	}
	bound := len(v) + len(v)/255 + 16
	buf = growslice(buf, bound)

	enc := zstdEncPool.Get().(*zstd.Encoder)
	defer zstdEncPool.Put(enc)

	// EncodeAll uses buf[:0] to reuse the backing array
	buf = enc.EncodeAll(v, buf[:0])
	return buf, buf
}

// DecodeZstdIfNeed decompresses v into buf if enabled, otherwise returns buf and v unchanged.
// It reuses decoders from the pool and writes into buf (grown to at least len(v)).
func DecodeZstdIfNeed(buf, v []byte, enabled bool) ([]byte, []byte, error) {
	if !enabled {
		return buf, v, nil
	}
	buf = growslice(buf, len(v))

	dec := zstdDecPool.Get().(*zstd.Decoder)
	defer zstdDecPool.Put(dec)

	out, err := dec.DecodeAll(v, buf[:0])
	if err != nil {
		return buf, nil, fmt.Errorf("snappy.decode3: %w", err)
	}
	return out, out, nil
}
