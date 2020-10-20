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

func MustMarshal(dst io.Writer, v interface{}) {
	err := Marshal(dst, v)
	if err != nil {
		panic(err)
	}
}

func MustUnmarshal(dst interface{}, data io.Reader) {
	err := Unmarshal(dst, data)
	if err != nil {
		panic(err)
	}
}
