package commands

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestGetChainConfig(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	config, _, _, err := core.SetupGenesisBlock(db, core.DefaultGenesisBlock(), false /* history */, false /* overwrite */)
	if err != nil {
		t.Fatalf("setting up genensis block: %v", err)
	}

	config1, err1 := (&APIImpl{}).chainConfig(db)
	if err1 != nil {
		t.Fatalf("reading chain config: %v", err1)
	}
	if config.String() != config1.String() {
		t.Fatalf("read different config: %s, expected %s", config1.String(), config.String())
	}
}
