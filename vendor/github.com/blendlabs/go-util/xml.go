package util

import (
	"bytes"
	"encoding/xml"
	"io"
	"regexp"
)

var (
	cdataPrefix = []byte("<![CDATA[")
	cdataSuffix = []byte("]]>")
	cdataRe     = regexp.MustCompile("<!\\[CDATA\\[(.*?)\\]\\]>")
)

var (
	// XML is a namespace for xml utilities.
	XML = xmlUtil{}
)

type xmlUtil struct{}

// EncodeCDATA writes a data blob to a cdata tag.
func (xu xmlUtil) EncodeCDATA(data []byte) []byte {
	return bytes.Join([][]byte{cdataPrefix, data, cdataSuffix}, []byte{})
}

// DecodeCDATA decodes a cdata tag to a byte array.
func (xu xmlUtil) DecodeCDATA(cdata []byte) []byte {
	matches := cdataRe.FindAllSubmatch(cdata, 1)
	if len(matches) == 0 {
		return cdata
	}

	return matches[0][1]
}

// DeserializeXML unmarshals xml to an object.
func (xu xmlUtil) Deserialize(object interface{}, body string) error {
	return xu.DeserializeFromReader(object, bytes.NewBufferString(body))
}

// DeserializeXMLFromReader unmarshals xml to an object from a reader
func (xu xmlUtil) DeserializeFromReader(object interface{}, reader io.Reader) error {
	decoder := xml.NewDecoder(reader)
	return decoder.Decode(object)
}

// DeserializeXMLFromReaderWithCharsetReader uses a charset reader to deserialize xml.
func (xu xmlUtil) DeserializeFromReaderWithCharsetReader(object interface{}, body io.Reader, charsetReader func(string, io.Reader) (io.Reader, error)) error {
	decoder := xml.NewDecoder(body)
	decoder.CharsetReader = charsetReader
	return decoder.Decode(object)
}

// SerializeXML marshals an object to xml.
func (xu xmlUtil) Serialize(object interface{}) string {
	b, _ := xml.Marshal(object)
	return string(b)
}

// SerializeXMLToReader marshals an object to a reader.
func (xu xmlUtil) SerializeToReader(object interface{}) io.Reader {
	b, _ := xml.Marshal(object)
	return bytes.NewBufferString(string(b))
}
