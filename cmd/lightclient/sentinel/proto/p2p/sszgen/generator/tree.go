package generator

// getTree creates a function that SSZ hashes the structs,
func (e *env) getTree(name string, v *Value) string {
	tmpl := `// GetTree ssz hashes the {{.name}} object
	func (:: *{{.name}}) GetTree() (*ssz.Node, error) {
		return ssz.ProofTree(::)
	}`

	data := map[string]interface{}{
		"name": name,
	}
	str := execTmpl(tmpl, data)
	return appendObjSignature(str, v)
}
