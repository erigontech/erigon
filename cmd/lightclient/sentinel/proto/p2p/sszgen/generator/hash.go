package generator

import (
	"fmt"
	"strings"
)

// hashTreeRoot creates a function that SSZ hashes the structs,
func (e *env) hashTreeRoot(name string, v *Value) string {
	tmpl := `// HashTreeRoot ssz hashes the {{.name}} object
	func (:: *{{.name}}) HashTreeRoot() ([32]byte, error) {
		return ssz.HashWithDefaultHasher(::)
	}
	
	// HashTreeRootWith ssz hashes the {{.name}} object with a hasher	
	func (:: *{{.name}}) HashTreeRootWith(hh ssz.HashWalker) (err error) {
		{{.hashTreeRoot}}
		return
	}`

	data := map[string]interface{}{
		"name":         name,
		"hashTreeRoot": v.hashTreeRootContainer(true),
	}
	str := execTmpl(tmpl, data)
	return appendObjSignature(str, v)
}

func (v *Value) hashRoots(isList bool, elem Type) string {
	subName := "i"
	if v.e.c {
		subName += "[:]"
	}
	inner := ""
	if !v.e.c && elem == TypeBytes {
		inner = `if len(i) != %d {
			err = ssz.ErrBytesLength
			return
		}
		`
		inner = fmt.Sprintf(inner, v.e.s)
	}

	var appendFn string
	var elemSize uint64
	if elem == TypeBytes {
		// [][]byte
		if v.e.s != 32 {
			// we need to use PutBytes in order to hash the result since
			// is higher than 32 bytes
			appendFn = "PutBytes"
		} else {
			appendFn = "Append"
			elemSize = 32
		}
	} else {
		// []uint64
		appendFn = "Append" + uintVToName(v.e)
		elemSize = uint64(v.e.fixedSize())
	}

	var merkleize string
	if isList {
		tmpl := `numItems := uint64(len(::.{{.name}}))
		hh.MerkleizeWithMixin(subIndx, numItems, ssz.CalculateLimit({{.listSize}}, numItems, {{.elemSize}}))`

		merkleize = execTmpl(tmpl, map[string]interface{}{
			"name":     v.name,
			"listSize": v.s,
			"elemSize": elemSize,
		})

		// when doing []uint64 we need to round up the Hasher bytes to 32
		if elem == TypeUint {
			merkleize = "hh.FillUpTo32()\n" + merkleize
		}
	} else {
		merkleize = "hh.Merkleize(subIndx)"
	}

	tmpl := `{
		{{.outer}}subIndx := hh.Index()
		for _, i := range ::.{{.name}} {
			{{.inner}}hh.{{.appendFn}}({{.subName}})
		}
		{{.merkleize}}
	}`
	return execTmpl(tmpl, map[string]interface{}{
		"outer":     v.validate(),
		"inner":     inner,
		"name":      v.name,
		"subName":   subName,
		"appendFn":  appendFn,
		"merkleize": merkleize,
	})
}

// takes a "name" param so that the variable name can be replaced with a local name
// ie within a for loop for a list, the we want to refer to "elem" w/o a receiver variable
// when not specified, name will be set to "::." + v.name. In the final templating pass,
// the output formatter replaces all instances of "::" with the receiver variable for the container.
// appendBytes is a control variable which changes the fastssz.Hasher method used to handle byte slices
// when it is false, the default behavior is to call PutBytes, which merkleizes the buffer after appending
// the bytes. when true, the generated code calls AppendBytes32, which appends the bytes to the buffer
// with padding and leaves the merkleization for a following step. This is because in the case of ByteLists,
// the length of the list needs to be mixed in as part of the merkleization process, which happens in a separate
// call to MerkleizeWithMixin.
func (v *Value) hashTreeRoot(name string, appendBytes bool) string {
	if name == "" {
		name = "::." + v.name
	}
	switch v.t {
	case TypeContainer, TypeReference:
		return v.hashTreeRootContainer(false)

	case TypeBytes:
		if v.c {
			name += "[:]"
		}
		if v.isFixed() {
			tmpl := `{{.validate}}hh.PutBytes({{.name}})`
			return execTmpl(tmpl, map[string]interface{}{
				"validate": v.validate(),
				"name":     name,
				"size":     v.s,
			})
		} else {
			// dynamic bytes require special handling, need length mixed in
			hMethod := "PutBytes"
			if appendBytes {
				hMethod = "AppendBytes32"
			}
			tmpl := `{
	elemIndx := hh.Index()
	byteLen := uint64(len({{.name}}))
	if byteLen > {{.maxLen}} {
		err = ssz.ErrIncorrectListSize
		return
    }
	hh.{{.hashMethod}}({{.name}})
	hh.MerkleizeWithMixin(elemIndx, byteLen, ({{.maxLen}}+31)/32)
}`
			return execTmpl(tmpl, map[string]interface{}{
				"hashMethod": hMethod,
				"name":       name,
				"maxLen":     v.m,
			})
		}

	case TypeUint:
		if v.ref != "" || v.obj != "" {
			// alias to Uint64
			name = fmt.Sprintf("uint64(%s)", name)
		}
		bitLen := v.fixedSize() * 8
		return fmt.Sprintf("hh.PutUint%d(%s)", bitLen, name)

	case TypeBitList:
		tmpl := `if len({{.name}}) == 0 {
			err = ssz.ErrEmptyBitlist
			return
		}
		hh.PutBitlist({{.name}}, {{.size}})
		`
		return execTmpl(tmpl, map[string]interface{}{
			"name": name,
			"size": v.m,
		})

	case TypeBool:
		return fmt.Sprintf("hh.PutBool(%s)", name)

	case TypeVector:
		return v.hashRoots(false, v.e.t)

	case TypeList:
		if v.e.isFixed() {
			if v.e.t == TypeUint || v.e.t == TypeBytes {
				return v.hashRoots(true, v.e.t)
			}
		}

		tmpl := `{
			subIndx := hh.Index()
			num := uint64(len({{.name}}))
			if num > {{.num}} {
				err = ssz.ErrIncorrectListSize
				return
			}
			for _, elem := range {{.name}} {
{{.htrCall}}
			}
			hh.MerkleizeWithMixin(subIndx, num, {{.num}})
		}`
		var htrCall string
		if v.e.t == TypeBytes {
			eName := "elem"
			// ByteLists should be represented as Value with TypeBytes and .m set instead of .s (isFixed == true)
			htrCall = v.e.hashTreeRoot(eName, true)
		} else {
			htrCall = execTmpl(`if err = elem.HashTreeRootWith(hh); err != nil {
	return
}`,
				map[string]interface{}{"name": name})
		}
		return execTmpl(tmpl, map[string]interface{}{
			"name":    name,
			"num":     v.m,
			"htrCall": htrCall,
		})

	case TypeTime:
		return fmt.Sprintf("hh.PutUint64(uint64(%s.Unix()))", name)

	default:
		panic(fmt.Errorf("hash not implemented for type %s", v.t.String()))
	}
}

func (v *Value) hashTreeRootContainer(start bool) string {
	if !start {
		tmpl := `{{ if .check }}if ::.{{.name}} == nil {
			::.{{.name}} = new({{.obj}})
		}
		{{ end }}if err = ::.{{.name}}.HashTreeRootWith(hh); err != nil {
			return
		}`
		// validate only for fixed structs
		check := v.isFixed()
		if v.isListElem() {
			check = false
		}
		if v.noPtr {
			check = false
		}
		return execTmpl(tmpl, map[string]interface{}{
			"name":  v.name,
			"obj":   v.objRef(),
			"check": check,
		})
	}

	out := []string{}
	for indx, i := range v.o {
		// the call to hashTreeRoot below is ugly because it's currently hacked to support ByteLists
		// the first argument allows the element name to be overriden when calling .HashTreeRoot on it
		// used to specify the name "elem" when called as part of a for loop iteration. when the string
		// is empty, it defaults to the .name parameter of the value
		// the second field tells the code generator to specifically generate a call to AppendBytes32
		// this is used by List[List[byte, N]] so that lists of lists of bytes are not double-merkleized.
		str := fmt.Sprintf("// Field (%d) '%s'\n%s\n", indx, i.name, i.hashTreeRoot("", false))
		out = append(out, str)
	}

	tmpl := `indx := hh.Index()

	{{.fields}}

	hh.Merkleize(indx)`

	return execTmpl(tmpl, map[string]interface{}{
		"fields": strings.Join(out, "\n"),
	})
}
