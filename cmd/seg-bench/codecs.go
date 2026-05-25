package main

import (
	"fmt"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// Codec wraps a per-record encode/decode pair. EncodeOne and DecodeOne
// must be independently decodable per call — no shared streaming
// state between records. This preserves the random-access property
// today's seg/compress provides.
type Codec interface {
	Name() string
	EncodeOne(dst, src []byte) ([]byte, error)
	DecodeOne(dst, src []byte) ([]byte, error)
	Close()
}

// SnappyCodec uses snappy's block format (independent per-call coding).
type SnappyCodec struct{}

func (SnappyCodec) Name() string { return "snappy" }
func (SnappyCodec) EncodeOne(dst, src []byte) ([]byte, error) {
	return snappy.Encode(dst, src), nil
}
func (SnappyCodec) DecodeOne(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}
func (SnappyCodec) Close() {}

// ZstdCodec wraps zstd at a fixed level. EncodeAll / DecodeAll
// produce / consume one complete frame per call — independent
// per-record coding, suitable for random access.
type ZstdCodec struct {
	name string
	enc  *zstd.Encoder
	dec  *zstd.Decoder
}

// NewZstdCodec returns a codec at one of the supported levels: 1, 3, 9, 19.
func NewZstdCodec(level int) (*ZstdCodec, error) {
	var lvl zstd.EncoderLevel
	switch level {
	case 1:
		lvl = zstd.SpeedFastest
	case 3:
		lvl = zstd.SpeedDefault
	case 9:
		lvl = zstd.SpeedBetterCompression
	case 19:
		lvl = zstd.SpeedBestCompression
	default:
		return nil, fmt.Errorf("zstd level %d not supported (use 1, 3, 9, 19)", level)
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(lvl),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		enc.Close()
		return nil, err
	}
	return &ZstdCodec{
		name: fmt.Sprintf("zstd-%d", level),
		enc:  enc,
		dec:  dec,
	}, nil
}

func (z *ZstdCodec) Name() string { return z.name }

func (z *ZstdCodec) EncodeOne(dst, src []byte) ([]byte, error) {
	return z.enc.EncodeAll(src, dst), nil
}

func (z *ZstdCodec) DecodeOne(dst, src []byte) ([]byte, error) {
	return z.dec.DecodeAll(src, dst)
}

func (z *ZstdCodec) Close() {
	if z.enc != nil {
		z.enc.Close()
	}
	if z.dec != nil {
		z.dec.Close()
	}
}

// NewZstdDictCodec returns a zstd codec at `level` using a pre-trained
// dictionary. The dictionary should be produced out-of-band via
// `zstd --train` and loaded by the caller (see bench.go's --dict flag).
func NewZstdDictCodec(level int, dict []byte) (*ZstdCodec, error) {
	var lvl zstd.EncoderLevel
	switch level {
	case 1:
		lvl = zstd.SpeedFastest
	case 3:
		lvl = zstd.SpeedDefault
	case 9:
		lvl = zstd.SpeedBetterCompression
	case 19:
		lvl = zstd.SpeedBestCompression
	default:
		return nil, fmt.Errorf("zstd level %d not supported (use 1, 3, 9, 19)", level)
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(lvl),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderDict(dict),
	)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderDicts(dict),
	)
	if err != nil {
		enc.Close()
		return nil, err
	}
	return &ZstdCodec{
		name: fmt.Sprintf("zstd-%d+dict", level),
		enc:  enc,
		dec:  dec,
	}, nil
}

// makeCodec resolves a codec name (e.g. "snappy", "zstd-3", "zstd-3+dict")
// into a Codec instance. `dict` is required for any "+dict" codec name
// and ignored otherwise.
func makeCodec(name string, dict []byte) (Codec, error) {
	switch name {
	case "snappy":
		return SnappyCodec{}, nil
	case "zstd-1":
		return NewZstdCodec(1)
	case "zstd-3":
		return NewZstdCodec(3)
	case "zstd-9":
		return NewZstdCodec(9)
	case "zstd-19":
		return NewZstdCodec(19)
	case "zstd-3+dict":
		if len(dict) == 0 {
			return nil, fmt.Errorf("codec %q requires --dict <path-to-trained-dict>", name)
		}
		return NewZstdDictCodec(3, dict)
	case "zstd-19+dict":
		if len(dict) == 0 {
			return nil, fmt.Errorf("codec %q requires --dict <path-to-trained-dict>", name)
		}
		return NewZstdDictCodec(19, dict)
	default:
		return nil, fmt.Errorf("unknown codec %q (known: snappy, zstd-{1,3,9,19}, zstd-{3,19}+dict)", name)
	}
}
