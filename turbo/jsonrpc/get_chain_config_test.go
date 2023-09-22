package jsonrpc

import (
	"context"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"testing"
)

func TestGetChainConfig(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := newBaseApiForTest(m)
	config := m.ChainConfig

	tx, txErr := db.BeginRo(context.Background())
	if txErr != nil {
		t.Fatalf("error starting tx: %v", txErr)
	}
	defer tx.Rollback()

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
