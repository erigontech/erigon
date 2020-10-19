package cbor

import (
	"io"
)

//// Marshal - se
//// you don't need reset buffer if you reusing it - method will does it for you
//func Marshal(dst *[]byte, v interface{}) error {
//	e := EncoderBytes(dst)
//	err := e.Encode(v)
//	returnEncoderToPool(e)
//	return err
//}
//
//// Unmarshal
//// if you unmarshal data from database - and plan to use object outside of transaction - copy data before unmarshal
//func Unmarshal(dst interface{}, data []byte) error {
//	d := DecoderBytes(data)
//	err := d.Decode(dst)
//	returnDecoderToPool(d)
//	return err
//}

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
