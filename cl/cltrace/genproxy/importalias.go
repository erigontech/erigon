package main

import (
	"fmt"
	"go/types"
	"reflect"
	"sort"
	"strings"
)

type ImportAliaser struct {
	ownPackagePath string

	imports map[string] /* path */ *ImportAlias
	aliases map[string] /* alias */ *ImportAlias
}

type ImportAlias struct {
	Package  *types.Package
	Path     string // full path
	Alias    string
	Implicit bool
}

func (this *ImportAliaser) GetAllAliases() []*ImportAlias {
	paths := []string{}
	for _, imp := range this.imports {
		paths = append(paths, imp.Package.Name())
	}
	sort.Strings(paths)

	imps := []*ImportAlias{}
	for _, path := range paths {
		imps = append(imps, this.imports[path])
	}

	return imps
}

func (this *ImportAliaser) AssignAlias(pkg *types.Package) {
	path := pkg.Path()

	if path == this.ownPackagePath {
		return // same package, no import needed
	}

	if _, ok := this.imports[path]; ok {
		return // already imported, ignore
	}

	var alias string
	for i := 0; ; i += 1 {
		if i == 0 {
			alias = pkg.Name()
		} else {
			alias = fmt.Sprintf("%s_%d", pkg.Name(), i)
		}
		if _, alreadyUsed := this.aliases[alias]; alreadyUsed {
			continue // search next alias
		}
		break
	}

	implicitAlias := alias == path || strings.HasSuffix(path, "/"+alias)

	entry := &ImportAlias{
		Package:  pkg,
		Path:     path,
		Alias:    alias,
		Implicit: implicitAlias,
	}
	this.imports[path] = entry
	this.imports[alias] = entry
}

func (this *ImportAliaser) AssignAliasFromType(typObj types.Type) {
	switch typ := typObj.(type) {
	case *types.Array:
		this.AssignAliasFromType(typ.Elem())
	case *types.Slice:
		this.AssignAliasFromType(typ.Elem())
	case *types.Map:
		this.AssignAliasFromType(typ.Key())
		this.AssignAliasFromType(typ.Elem())
	case *types.Chan:
		this.AssignAliasFromType(typ.Elem())
	case *types.Pointer:
		this.AssignAliasFromType(typ.Elem())
	case *types.Basic: // string, etc.
		// do nothing
	case *types.Named:
		name := typ.Obj()
		if name.IsAlias() {
			panic(fmt.Sprintf("Type name %+v is an alias and not supported. Please file an issue here: https://github.com/eiiches/go-gen-proxy", typ))
		}

		pkg := name.Pkg()
		if pkg != nil { // pkg is null for built-in interface (e.g error)
			this.AssignAlias(pkg)
		}
	case *types.Struct: // anonymous struct
		for i := 0; i < typ.NumFields(); i += 1 {
			field := typ.Field(i)
			this.AssignAliasFromType(field.Type())
		}
	case *types.Interface: // anonymous interface
		for i := 0; i < typ.NumEmbeddeds(); i += 1 {
			this.AssignAliasFromType(typ.EmbeddedType(i))
		}
		for i := 0; i < typ.NumMethods(); i += 1 {
			method := typ.Method(i)
			this.AssignAliasFromType(method.Type())
		}
	case *types.Signature: // unnamed function (and interface method)
		for j := 0; j < typ.Params().Len(); j += 1 {
			this.AssignAliasFromType(typ.Params().At(j).Type())
		}
		for j := 0; j < typ.Results().Len(); j += 1 {
			this.AssignAliasFromType(typ.Results().At(j).Type())
		}
	default:
		panic(fmt.Sprintf("Type %+v (type = %+v, underlying = %+v) is not supported. Please file an issue here: https://github.com/eiiches/go-gen-proxy", typ, reflect.TypeOf(typ), reflect.TypeOf(typ.Underlying())))
	}
}

func (this *ImportAliaser) GetQualifier(pkg *types.Package) string {
	if this.ownPackagePath == pkg.Path() {
		// same package, no qualifier needed
		return ""
	}

	entry, ok := this.imports[pkg.Path()]
	if !ok {
		return pkg.Name() // defaults to package name if missing
	}
	return entry.Alias
}

func NewImportAliaser(ownPackagePath string) *ImportAliaser {
	return &ImportAliaser{
		ownPackagePath: ownPackagePath,
		imports:        make(map[string]*ImportAlias),
		aliases:        make(map[string]*ImportAlias),
	}
}
