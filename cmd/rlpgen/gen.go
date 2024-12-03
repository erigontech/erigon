package main

import "go/types"

type gen struct {
	pkg     *types.Package
	imports []string
	code    []byte
}
