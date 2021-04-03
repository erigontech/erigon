package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestGetChainConfig(t *testing.T) {
	db := ethdb.NewMemKV()
	defer db.Close()
	config, _, err := core.SetupGenesisBlock(ethdb.NewObjectDatabase(db), core.DefaultGenesisBlock(), false /* history */, false /* overwrite */)
	if err != nil {
		t.Fatalf("setting up genensis block: %v", err)
	}

	tx, txErr := db.BeginRo(context.Background())
	if txErr != nil {
		t.Fatalf("error starting tx: %v", txErr)
	}
	defer tx.Rollback()

	api := (&BaseAPI{})
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
