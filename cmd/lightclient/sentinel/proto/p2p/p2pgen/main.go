package main

import (
	"bytes"
	"flag"
	"go/format"
	"os"
	"path"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

var input string
var output string

type Spec struct {
	Aliases map[string]string
	Structs map[string][]Field
}
type Field struct {
	Name string
	Type string
	Tags TagMap
}

type TagMap map[string]string

func (t TagMap) String() string {
	sb := new(strings.Builder)
	sb.WriteString("`")
	for k, v := range t {
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteRune('"')
		sb.WriteString(v)
		sb.WriteString(`" `)
	}
	sb.WriteString("`")
	return sb.String()
}

func main() {
	flag.StringVar(&input, "i", "spec_p2p.yaml", "yaml file to read")
	flag.StringVar(&output, "o", ".", "directory to output")
	flag.Parse()
	b, err := os.ReadFile(input)
	if err != nil {
		panic(err)
	}
	tmp := template.Must(template.New("p2pspec").Parse(tmpl))
	s := &Spec{}
	err = yaml.Unmarshal(b, s)
	if err != nil {
		panic(err)
	}
	buf := new(bytes.Buffer)
	err = tmp.Execute(buf, s)
	if err != nil {
		panic(err)
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}
	os.WriteFile(path.Join(output, "generated.go"), src, 0644)
}

const tmpl = `package p2p

//go:generate go run github.com/ferranbt/fastssz/sszgen -path generated.go -exclude-objs {{range $key, $val := .Aliases}}{{$key}},{{end}}Ignore

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
)

{{range $key, $val := .Aliases}}
type {{$key}} {{$val}}
{{end}}

{{range $key, $val := .Structs}}

type {{$key}} struct {

{{range $name, $field := $val}}
  {{$field.Name}} {{$field.Type}} {{$field.Tags.String}}
{{end}}

}

func (typ *{{$key}}) Clone() proto.Packet {
  return &{{$key}}{}
}
{{end}}
`
