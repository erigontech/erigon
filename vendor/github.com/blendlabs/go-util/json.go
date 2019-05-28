package util

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/blendlabs/go-exception"
)

var (
	// JSON is a namespace for json utils.
	JSON = jsonUtil{}
)

type jsonUtil struct{}

// Deserialize unmarshals an object from JSON.
func (ju jsonUtil) Deserialize(object interface{}, body string) error {
	decoder := json.NewDecoder(bytes.NewBufferString(body))
	return exception.Wrap(decoder.Decode(object))
}

// DeserializeFromReader unmashals an object from a json Reader.
func (ju jsonUtil) DeserializeFromReader(object interface{}, body io.Reader) error {
	return exception.Wrap(json.NewDecoder(body).Decode(object))
}

// DeserializeFromReadCloser unmashals an object from a json ReadCloser.
func (ju jsonUtil) DeserializeFromReadCloser(object interface{}, body io.ReadCloser) error {
	defer body.Close()

	decoder := json.NewDecoder(body)
	return exception.Wrap(decoder.Decode(object))
}

// Serialize marshals an object to json.
func (ju jsonUtil) Serialize(object interface{}) string {
	b, _ := json.Marshal(object)
	return string(b)
}

// SerializePretty marshals an object to json with formatting whitespace.
func (ju jsonUtil) SerializePretty(object interface{}, prefix, indent string) string {
	b, _ := json.MarshalIndent(object, prefix, indent)
	return string(b)
}

// SerializeAsReader marshals an object to json as a reader.
func (ju jsonUtil) SerializeAsReader(object interface{}) io.Reader {
	b, _ := json.Marshal(object)
	return bytes.NewBuffer(b)
}
