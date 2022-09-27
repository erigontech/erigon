package generator

// clone creates function that make new empty instance of the struct
func (e *env) clone(name string, v *Value) string {
	tmpl := `// Clone creates a new instance of a {{.name}} object
	func (:: *{{.name}}) Clone() (proto.Packet) {
		return &{{.name}}{}
	}
	`

	data := map[string]interface{}{
		"name": name,
	}
	str := execTmpl(tmpl, data)
	return appendObjSignature(str, v)
}
