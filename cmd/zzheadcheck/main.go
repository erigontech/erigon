package main

import (
	"context"
	"fmt"
	"os"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
)

func main() {
	db, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(os.Args[1]).Accede(true).Readonly(true).Open(context.Background())
	if err != nil {
		fmt.Println("OPEN_FAILED", err)
		os.Exit(1)
	}
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		fmt.Println("BEGIN_FAILED", err)
		os.Exit(1)
	}
	defer tx.Rollback()
	hash := rawdb.ReadHeadHeaderHash(tx)
	n := rawdb.ReadHeaderNumber(tx, hash)
	if n == nil {
		fmt.Println("HEAD_UNKNOWN", hash)
		return
	}
	fmt.Printf("HEAD %d %x\n", *n, hash)
}
