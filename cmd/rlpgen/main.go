package main

import (
	"flag"
	"fmt"
	"os"

	"golang.org/x/tools/go/packages"
)

func main() {
	var (
		pkgdir     = flag.String("dir", ".", "input package")
		output     = flag.String("out", "-", "output file (default is stdout)")
		genEncoder = flag.Bool("encoder", true, "generate EncodeRLP?")
		genDecoder = flag.Bool("decoder", false, "generate DecodeRLP?")
		typename   = flag.String("type", "", "type to generate methods for")
	)
	flag.Parse()
	fmt.Println("pkgdir: ", *pkgdir)
	fmt.Println("output: ", *output)
	fmt.Println("genEncoder: ", *genEncoder)
	fmt.Println("genDecoder: ", *genDecoder)
	fmt.Println("typename: ", *typename)

	pcfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedTypes,
		Dir:  *pkgdir,
	}
	ps, err := packages.Load(pcfg, ".")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, p := range ps {
		if len(p.Errors) > 0 {
			fmt.Errorf("package %s has errors", p.PkgPath)
			os.Exit(1)
		}
		fmt.Println("p.Types: ", p.Types)
		fmt.Println(p.Types)
	}
}

// func findType(pkgdir string, typename *string) {

// }
