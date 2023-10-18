package db_config

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
)

type DatabaseConfiguration struct{ PruneDepth uint64 }

var DefaultDatabaseConfiguration = DatabaseConfiguration{PruneDepth: 1000}

// should be 1_000_000

func WriteConfigurationIfNotExist(ctx context.Context, tx kv.RwTx, cfg DatabaseConfiguration) error {
	var b bytes.Buffer
	if err := cbor.Encoder(&b).Encode(cfg); err != nil {
		return err
	}

	return tx.Put(kv.DatabaseInfo, []byte("config"), b.Bytes())
}

func ReadConfiguration(ctx context.Context, tx kv.Tx) (DatabaseConfiguration, error) {
	var cfg DatabaseConfiguration

	cfgEncoded, err := tx.GetOne(kv.DatabaseInfo, []byte("config"))
	if err != nil {
		return cfg, err
	}
	if err := cbor.Decoder(bytes.NewReader(cfgEncoded)).Decode(&cfg); err != nil {
		return cfg, err
	}
	return cfg, err
}
