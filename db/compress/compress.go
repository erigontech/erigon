package compress

import (
	"fmt"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	// Decoder side: saw 2x higher throughput (parallel RPC with much decoding if use `zstdDecPool` (sync.Pool) vs single `zstd.NewReader`. So, keep pool for decoders.
	// Encoder side: saw high mem use when using pool of encoders. And probably we don't need high-throughput on writes (they are usually in background). So, keep 1 encoder - it inside using GOMAXPROCS concurrency limit (see zstd.WithDecoderConcurrency).
	zstdEnc, _  = zstd.NewWriter(nil, zstd.WithEncoderCRC(false), zstd.WithZeroFrames(true), zstd.WithLowerEncoderMem(true))
	zstdDecPool = sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil, zstd.IgnoreChecksum(true))
			return dec
		},
	}
)

func putDec(dec *zstd.Decoder) {
	// Reset fails only on a closed decoder, which can never be revived.
	if err := dec.Reset(nil); err != nil {
		return
	}
	zstdDecPool.Put(dec)
}

// EncodeZstdIfNeed compresses v into buf if enabled, otherwise returns buf and v unchanged.
// It pre-allocates buf to ZSTD’s worst-case bound (src + src/255 + 16) and reuses encoders.
func EncodeZstdIfNeed(buf, v []byte, enabled bool) (outBuf []byte, compressed []byte) {
	if !enabled {
		return buf, v
	}
	bound := len(v) + len(v)/255 + 16
	buf = slices.Grow(buf[:0], bound)[:bound]

	// EncodeAll uses buf[:0] to reuse the backing array
	buf = zstdEnc.EncodeAll(v, buf[:0])
	return buf, buf
}

// DecodeZstdIfNeed decompresses v into buf if enabled, otherwise returns buf and v unchanged.
// Reuses decoders from the pool. buf is handed to DecodeAll as-is: pre-growing it here can
// only ever grow it to the compressed length, which is by construction too small, so zstd
// would discard it and allocate from the frame content size anyway. Reuse comes from the
// caller storing the returned slice back.
func DecodeZstdIfNeed(buf, v []byte, enabled bool) ([]byte, []byte, error) {
	if !enabled {
		return buf, v, nil
	}

	dec := zstdDecPool.Get().(*zstd.Decoder)
	defer putDec(dec)

	out, err := dec.DecodeAll(v, buf[:0])
	if err != nil {
		return buf, nil, fmt.Errorf("zstd.decode: %w", err)
	}
	return out, out, nil
}
