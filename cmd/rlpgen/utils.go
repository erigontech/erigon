package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/types"
	"os"
	"strings"

	"golang.org/x/tools/go/packages"
)

func getPkgDir(path string) string {
	r := strings.Split(path, "/")
	if len(r) < 4 {
		panic("expected at least len 4")
	}
	if r[0] != "github.com" {
		panic("expected github.com to be the first item")
	}

	var result string

	if r[2] == "erigon" {
		result = "./" + r[3]
		if len(r) > 4 {
			for i := 4; i < len(r); i++ {
				result += ("/" + r[i])
			}
		}
	} else if r[2] == "erigon-lib" {
		result = "./" + r[2]
		if len(r) > 3 {
			for i := 3; i < len(r); i++ {
				result += ("/" + r[i])
			}
		}
	} else {
		fmt.Println(r)
		panic("getPkgDir: unhnadled")
	}

	return result
}

func findType(scope *types.Scope, typename string) (*types.Named, error) {
	// fmt.Println("TYPENAME: ", typename)
	// names := scope.Names()
	// for _, s := range names {
	// 	fmt.Println("obj: ", s)
	// }
	obj := scope.Lookup(typename)
	if obj == nil {
		return nil, fmt.Errorf("no such identifier: %s", typename)
	}
	typ, ok := obj.(*types.TypeName)
	if !ok {
		return nil, errors.New("not a type")
	}
	if named, ok := typ.Type().(*types.Named); ok {
		return named, nil
	}
	return nil, errors.New("not a named type")
}

func loadPkg(pkgdir string) []*packages.Package {
	pcfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedTypes,
		Dir:  pkgdir,
	}

	fmt.Println("loading package from directory: ", pkgdir)

	ps, err := packages.Load(pcfg, ".")

	if err != nil {
		_exit(fmt.Sprint("error loading package: ", err))
	}
	if len(ps) != 1 {
		_exit(fmt.Sprintf("expected to load package: 1) %v\n \tgot %v", pkgdir, len(ps)))
	}

	if err := checkPackageErrors(ps[0]); err != nil {
		_exit(err.Error())
	}

	return ps
}

func _exit(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}

func checkPackageErrors(pkg *packages.Package) error {
	var b bytes.Buffer
	if len(pkg.Errors) > 0 {
		fmt.Fprintf(&b, "package %s has errors: \n", pkg.PkgPath)
		for _, e := range pkg.Errors {
			fmt.Fprintf(&b, "%s\n", e.Msg)
		}
	}
	if b.Len() > 0 {
		return errors.New(b.String())
	}
	return nil
}
