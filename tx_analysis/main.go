package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	log_ "github.com/ledgerwatch/log/v3"
)

func main() {
	logger := log_.New()
	db, err := mdbx.NewMDBX(logger).Path("/mnt/mx500_0/goerli/chaindata").Readonly().Open()
	if err != nil {
		fmt.Println(err)

	}
	defer db.Close()

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()
}
