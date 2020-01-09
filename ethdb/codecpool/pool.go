package codecpool

import (
	"fmt"
	"io"

	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ugorji/go/codec"
)

var logger = log.New("codecpool")

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

func Return(d interface{}) {
	switch toReturn := d.(type) {
	case *codec.Decoder:
		returnDecoderToPool(toReturn)
	case *codec.Encoder:
		returnEncoderToPool(toReturn)
	default:
		panic(fmt.Sprintf("unexpected type: %T", d))
	}
}
