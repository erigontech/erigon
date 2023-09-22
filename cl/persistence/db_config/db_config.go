package db_config

import (
	"context"
	"database/sql"
)

type DatabaseConfiguration struct{ PruneDepth uint64 }

var DefaultDatabaseConfiguration = DatabaseConfiguration{PruneDepth: 1000}

// should be 1_000_000

func WriteConfigurationIfNotExist(ctx context.Context, tx *sql.Tx, cfg DatabaseConfiguration) error {
	var count int
	err := tx.QueryRow("SELECT COUNT(*) FROM data_config;").Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO data_config (prune_depth) VALUES (?);", cfg.PruneDepth)
	if err != nil {
		return err
	}
	return nil
}

func ReadConfiguration(ctx context.Context, tx *sql.Tx) (DatabaseConfiguration, error) {
	var pruneDepth uint64

	err := tx.QueryRowContext(ctx, "SELECT prune_depth FROM data_config").Scan(&pruneDepth)
	if err != nil {
		return DatabaseConfiguration{}, err
	}
	return DatabaseConfiguration{PruneDepth: pruneDepth}, nil
}
