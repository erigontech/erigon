package cbor

import (
	"io"
)

func Marshal(dst io.Writer, v interface{}) error {
	e := Encoder(dst)
	err := e.Encode(v)
	returnEncoderToPool(e)
	return err
}

func Unmarshal(dst interface{}, data io.Reader) error {
	d := Decoder(data)
	err := d.Decode(dst)
	returnDecoderToPool(d)
	return err
}
