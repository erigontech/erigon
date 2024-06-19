package cbor

import (
	"io"

	"github.com/ugorji/go/codec"

	"github.com/ledgerwatch/erigon-lib/log/v3"
)

var logger = log.New("package", "cbor")

// Pool of decoders
var decoderPool = make(chan *codec.Decoder, 128)

func Decoder(r io.Reader) *codec.Decoder {
	var d *codec.Decoder
	select {
	case d = <-decoderPool:
		d.Reset(r)
	default:
		{
			var handle codec.CborHandle
			handle.ReaderBufferSize = 64 * 1024
			handle.ZeroCopy = true // if you need access to object outside of db transaction - please copy bytes before deserialization
			d = codec.NewDecoder(r, &handle)
		}
	}
	return d
}

func returnDecoderToPool(d *codec.Decoder) {
	select {
	case decoderPool <- d:
	default:
		logger.Trace("Allowing decoder to be garbage collected, pool is full")
	}
}

// Pool of encoders
var encoderPool = make(chan *codec.Encoder, 128)

func Encoder(w io.Writer) *codec.Encoder {
	var e *codec.Encoder
	select {
	case e = <-encoderPool:
		e.Reset(w)
	default:
		{
			var handle codec.CborHandle
			handle.WriterBufferSize = 64 * 1024
			handle.StructToArray = true
			handle.OptimumSize = true
			handle.StringToRaw = true

			e = codec.NewEncoder(w, &handle)
		}
	}
	return e
}

func returnEncoderToPool(e *codec.Encoder) {
	select {
	case encoderPool <- e:
	default:
		logger.Trace("Allowing encoder to be garbage collected, pool is full")
	}
}
