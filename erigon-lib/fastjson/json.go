package fastjson

import (
	"io"

	"github.com/goccy/go-json"
)

type (
	Encoder = json.Encoder
	Decoder = json.Decoder
)

func Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	return json.MarshalIndent(v, prefix, indent)
}

func Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func NewEncoder(w io.Writer) *Encoder {
	return json.NewEncoder(w)
}

func NewDecoder(r io.Reader) *Decoder {
	return json.NewDecoder(r)
}
