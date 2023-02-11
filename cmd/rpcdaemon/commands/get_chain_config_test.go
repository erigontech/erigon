package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core"
)

func TestGetChainConfig(t *testing.T) {
	db := memdb.NewTestDB(t)
	config, _, err := core.CommitGenesisBlock(db, core.DefaultGenesisBlock(), "")
	if err != nil {
		t.Fatalf("setting up genensis block: %v", err)
	}

	tx, txErr := db.BeginRo(context.Background())
	if txErr != nil {
		t.Fatalf("error starting tx: %v", txErr)
	}
	defer tx.Rollback()

	api := &BaseAPI{}
	config1, err1 := api.chainConfig(tx)
	if err1 != nil {
		t.Fatalf("reading chain config: %v", err1)
	}
	if config.String() != config1.String() {
		t.Fatalf("read different config: %s, expected %s", config1.String(), config.String())
	}
	config2, err2 := api.chainConfig(tx)
	if err2 != nil {
		t.Fatalf("reading chain config: %v", err2)
	}
	if config.String() != config2.String() {
		t.Fatalf("read different config: %s, expected %s", config2.String(), config.String())
	}
}
