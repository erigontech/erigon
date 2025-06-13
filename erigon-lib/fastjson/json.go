package fastjson

import (
	"github.com/goccy/go-json"
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
