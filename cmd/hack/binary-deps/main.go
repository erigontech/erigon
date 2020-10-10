// +build trick_go_mod_tidy

// This module is just a hack for 'go mod tidy' command
//
// Problem is - 'go mod tidy' removes from go.mod file lines if you don't use them in source code
//
// But code generators we use as binaries - not by including them into source code
// To provide reproducible builds - go.mod file must be source of truth about code generators version
// 'go mod tidy' will not remove binary deps from go.mod file if you add them here
//
// use `make devtools` - does install all binary deps of right version

// build tag 'trick_go_mod_tidy' - is used to hide warnings of IDEA (because we can't import `main` packages in go)

package main

import (
	_ "github.com/fjl/gencodec"
	_ "github.com/kevinburke/go-bindata"
	_ "github.com/ledgerwatch/lmdb-go/cmd/lmdb_copy"
	_ "github.com/ledgerwatch/lmdb-go/cmd/lmdb_stat"
	_ "github.com/ugorji/go/codec/codecgen"
	_ "golang.org/x/tools/cmd/stringer"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
)
