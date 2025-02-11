package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/types"
	"strings"
)

type scope struct {
	named          *types.Named    // to-be generated type
	imports        map[string]bool // imports required by the type
	sizeBuf        bytes.Buffer    // encodingSize buffer
	encodeBuf      bytes.Buffer    // encodeRLP buffer
	decodeBuf      bytes.Buffer    // decodeRLP buffer
	pkgDir         string          // e.g ./core/types
	pkgName        string          // e.g "types"
	pkgPath        string          // e.g "github.com/erigontech/erigon/core/types"
	typeName       string          // e.g Hash
	filename       string          // gen_<typename>_rlp.go or could be gen_multitypes_rlp.go if multiple types being encoded in the same package, so gen_multitypes_rlp.go will contain all methods for the types in the single file
	decodeBufAdded bool            // if buffer was added to this scope
	intSizeAdded   bool            // for encoding size
	intEncodeAdded bool            // for rlp encoding
}

func newScope(pkgdir string, typename string) *scope {

	fmt.Println("NEW SCOPE: ", pkgdir, typename)

	pkg := loadPkg(pkgdir)
	pkgSrc := pkg[0].Types

	fmt.Println("pkgSrc: ", pkgSrc.Name())
	fmt.Println("typename: ", typename)
	fmt.Println("pkgSrc: ", pkgSrc.Path())
	// 1. search for a struct
	typ, err := findType(pkgSrc.Scope(), typename)
	if err != nil {
		_exit(err.Error())
	}

	g := &scope{
		named:     typ,
		imports:   make(map[string]bool),
		sizeBuf:   bytes.Buffer{},
		encodeBuf: bytes.Buffer{},
		decodeBuf: bytes.Buffer{},
		pkgDir:    pkgdir,
		pkgName:   typ.Obj().Pkg().Name(),
		pkgPath:   typ.Obj().Pkg().Path(),
		typeName:  typ.Obj().Name(),
		filename:  "gen_" + strings.ToLower(typ.Obj().Name()) + "_rlp.go",
	}

	return g
}

func (s *scope) generate() {
	fmt.Println("---------------------------------------")
	typename := s.named.Obj().Name()

	// 1. start EncodingSize method on a struct
	fmt.Fprintf(&s.sizeBuf, "func (obj *%s) EncodingSize() (size int) {\n", typename)

	// 2. start EncodeRLP
	fmt.Fprintf(&s.encodeBuf, "func (obj *%s) EncodeRLP(w io.Writer) error {\n", typename)
	fmt.Fprint(&s.encodeBuf, "    var b [32]byte\n")
	fmt.Fprint(&s.encodeBuf, "    if err := rlp.EncodeStructSizePrefix(obj.EncodingSize(), w, b[:]); err != nil {\n")
	fmt.Fprint(&s.encodeBuf, "        return err\n")
	fmt.Fprint(&s.encodeBuf, "    }\n")

	// 3. start DecodeRLP
	fmt.Fprintf(&s.decodeBuf, "func (obj *%s) DecodeRLP(s *rlp.Stream) error {\n", typename)
	fmt.Fprint(&s.decodeBuf, "    _, err := s.List()\n")
	fmt.Fprint(&s.decodeBuf, "    if err != nil {\n")
	fmt.Fprint(&s.decodeBuf, "        return err\n")
	fmt.Fprint(&s.decodeBuf, "    }\n")

	// 4. add encoding/decoding logic
	if err := _handleType(s.named.Obj().Type(), s); err != nil {
		_exit(err.Error())
	}

	// 5. end EncodingSize method
	fmt.Fprintf(&s.sizeBuf, "    return\n}\n\n")

	// 6. end EcnodeRLP
	fmt.Fprintf(&s.encodeBuf, "    return nil\n}\n\n")

	// 7. end DecodeRLP
	fmt.Fprintf(&s.decodeBuf, "    if err = s.ListEnd(); err != nil {\n")
	fmt.Fprintf(&s.decodeBuf, "        return fmt.Errorf(\"error closing %s, err: %%w\", err)\n", typename)
	fmt.Fprintf(&s.decodeBuf, "    }\n")
	fmt.Fprintf(&s.decodeBuf, "    return nil\n}\n")

	fmt.Printf("%s\n", s.sizeBuf.Bytes())
	fmt.Printf("%s\n", s.encodeBuf.Bytes())
	fmt.Printf("%s\n", s.decodeBuf.Bytes())

}

func (s *scope) addDecodeBuf() {
	if !s.decodeBufAdded {
		fmt.Fprint(&s.decodeBuf, "    var b []byte\n")
		s.decodeBufAdded = true
	}
}

// add reusable int for encoding size usage
func (s *scope) addIntSize() {
	if !s.intSizeAdded {
		fmt.Fprint(&s.sizeBuf, "    gidx := 0\n")
		s.intSizeAdded = true
	} else {
		fmt.Fprint(&s.sizeBuf, "    gidx = 0\n")
	}
}

// add reusable int for encoding usage
func (s *scope) addIntEncode() {
	if !s.intEncodeAdded {
		fmt.Fprint(&s.encodeBuf, "    gidx := 0\n")
		s.intEncodeAdded = true
	} else {
		fmt.Fprint(&s.encodeBuf, "    gidx = 0\n")
	}
}

// 1. add package to imports if the to-be encoded field is not in the same package
// e.g do not import "github.com/erigontech/erigon/core/types" if the field is types.BlockNonce
func (s *scope) addToImports(named *types.Named) (typ string) {
	if named.Obj().Pkg().Path() != s.pkgPath {
		s.imports[named.Obj().Pkg().Path()] = true
		typ = named.Obj().Pkg().Name() + "." + named.Obj().Name()
	} else {
		typ = named.Obj().Name()
	}
	return
}

func _handleType(typ types.Type, s *scope) error {
	switch v := typ.(type) {
	case *types.Pointer:
		if v.Elem().String() == "math/big.Int" {
			fmt.Println("POINTER math/big.Int")
			return nil
		}
		if v.Elem().String() == "github.com/holiman/uint256.Int" {
			fmt.Println("POINTER uint256.Int")
			return nil
		}

		return _handleType(v.Elem(), s)
	case *types.Named:
		if v.Obj().Type().String() == "math/big.Int" {
			fmt.Println("ISSSS math/big.Int")
			return nil
		}
		if v.Obj().Type().String() == "github.com/holiman/uint256.Int" {
			fmt.Println("ISSSS uint256.Int")
			return nil
		}

		return _handleType(v.Underlying(), s)

		// pkgDir := getPkgDir(v.Obj().Pkg().Path())
		// typeName := v.Obj().Name()
		// if pkgDir != s.pkgDir && typeName != s.typeName {
		// 	fmt.Println("PKGDIR: ", pkgDir, typeName)
		// 	// _scope := newScope(pkgDir, typeName)
		// 	// _scope.generate()
		// }

		// fmt.Println("NAMED: ", v.Obj().Type().String())
		// fmt.Println("NAMED UNDERLYING: ", v.Obj().Type().Underlying().String())

		// fmt.Println("PKGDIR: ", pkgDir, typeName)
		// fmt.Println("PKGDIR2: ", s.pkgDir, s.typeName)

		// if _struct, ok := v.Underlying().(*types.Struct); ok {
		// 	fmt.Println("\nStruct -> ")
		// 	for i := 0; i < _struct.NumFields(); i++ {
		// 		if err := _handleType(_struct.Field(i).Type(), s); err != nil {
		// 			return err
		// 		}
		// 	}
		// } else {

		// }
	case *types.Basic:
		kind := v.Kind()
		switch {
		case kind == types.Bool:
		case kind >= types.Int && kind <= types.Uint64:
		case kind == types.String:
		}
		fmt.Printf("Basic %v -> \n", v.String())
	case *types.Slice:
		fmt.Printf("Slice %v -> \n", v.Underlying())
		fmt.Printf("Slice %v -> \n", v.Elem().Underlying())
	case *types.Array:
		fmt.Printf("Array %v -> \n", v.Underlying())
	case *types.Struct:
		fmt.Println("\nStruct 1 -> ")
		for i := 0; i < v.NumFields(); i++ {
			if err := _handleType(v.Field(i).Type(), s); err != nil {
				return err
			}
		}
	}

	return nil
}

var scopes = map[string]*scope{}

func main() {
	var (
		pkgdir   = flag.String("dir", ".", "input package")
		typename = flag.String("type", "", "type to generate methods for")
		// writefile = flag.Bool("wfile", true, "set to false if no need to write to the file")
	)
	flag.Parse()

	_scope := newScope(*pkgdir, *typename)
	_scope.generate()
	scopes[_scope.filename] = _scope

	for k, v := range scopes {
		fmt.Println(k, v)
	}
}
