package util

import (
	"encoding/xml"
	"io/ioutil"
	"testing"

	"github.com/blendlabs/go-assert"
)

type mockXMLObject struct {
	XMLName       xml.Name `xml:"Person"`
	ID            string   `xml:"id,attr"`
	Email         string   `xml:"Email"`
	StreetAddress string   `xml:"Address>Street"`
}

func TestXml(t *testing.T) {
	assert := assert.New(t)
	objStr := `<Person id="test"><Email>foo@bar.com</Email><Address><Street>123 Road St</Street></Address></Person>`
	obj := mockXMLObject{}
	XML.Deserialize(&obj, objStr)
	assert.Equal("test", obj.ID)
	assert.Equal("foo@bar.com", obj.Email)
	assert.Equal("123 Road St", obj.StreetAddress)

	serialized := XML.Serialize(obj)
	assert.Equal(objStr, serialized)

	serializedReader := XML.SerializeToReader(obj)
	serializedReaderContents, readerErr := ioutil.ReadAll(serializedReader)
	assert.Nil(readerErr)
	serializedReaderStr := string(serializedReaderContents)
	assert.Equal(objStr, serializedReaderStr)
}

func TestCdata(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("<![CDATA[test]]>", string(XML.EncodeCDATA([]byte("test"))))
	assert.Equal("<![CDATA[test", string(XML.DecodeCDATA([]byte("<![CDATA[test"))))
	assert.Len(XML.DecodeCDATA([]byte("<![CDATA[]]>")), 0)
	assert.Equal("test", string(XML.DecodeCDATA([]byte("<![CDATA[test]]>"))))
	assert.Equal("test", string(XML.DecodeCDATA([]byte(" <![CDATA[test]]>"))))
	assert.Equal("<![CDATA[test", string(XML.DecodeCDATA([]byte("<![CDATA[<![CDATA[test]]>]]>"))))
	assert.Equal("one", string(XML.DecodeCDATA([]byte("<![CDATA[one]]><![CDATA[two]]>"))))
}
