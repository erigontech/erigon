package main

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/params"
	cli2 "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
	"os"
)

var (
	dataDirFlag = &cli.StringFlag{
		Name:        "datadir",
		Usage:       "Data directory 1",
		Destination: &dataDir,
	}

	dataDir1Flag = &cli.StringFlag{
		Name:        "datadir1",
		Usage:       "Data directory 1",
		Destination: &dataDir1,
	}

	dataDir2Flag = &cli.StringFlag{
		Name:        "datadir2",
		Usage:       "Data directory 2",
		Destination: &dataDir2,
	}

	tableFlag = &cli.StringFlag{
		Name:        "table",
		Usage:       "Database table",
		Destination: &dbTable,
	}

	dataDir  string
	dataDir1 string
	dataDir2 string
	dbTable  string

	Compare = cli.Command{
		Action: runCompare,
		Name:   "compare",
		Usage:  "Compare two MDBX databases",
		Flags: []cli.Flag{
			dataDir1Flag,
			dataDir2Flag,
			tableFlag,
		},
	}

	Tables = cli.Command{
		Action: runTables,
		Name:   "tables",
		Usage:  "Log available MDBX database tables",
		Flags: []cli.Flag{
			dataDirFlag,
		},
	}

	Entries = cli.Command{
		Action: runEntries,
		Name:   "entries",
		Usage:  "Log entries for MDBX database table",
		Flags: []cli.Flag{
			dataDirFlag,
			tableFlag,
		},
	}
)

func main() {
	app := cli2.NewApp(params.GitCommit, "MDBX compare")
	app.Commands = []*cli.Command{
		&Compare,
		&Tables,
		&Entries,
	}

	logging.SetupLogger("mdbx compare")

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runCompare(cliCtx *cli.Context) error {
	db1, err := mdbx.NewMDBX(log.New()).Path(dataDir1).Open(cliCtx.Context)
	if err != nil {
		return err
	}
	defer db1.Close()

	data1 := make(map[string][]byte)
	err = db1.View(cliCtx.Context, func(tx kv.Tx) error {
		c, err := tx.Cursor(dbTable)
		if err != nil {
			return err
		}
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			data1[string(k)] = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		return err
	}

	db2, err := mdbx.NewMDBX(log.New()).Path(dataDir2).Open(cliCtx.Context)
	if err != nil {
		return err
	}
	defer db2.Close()

	data2 := make(map[string][]byte)
	err = db2.View(cliCtx.Context, func(tx kv.Tx) error {
		c, err := tx.Cursor(dbTable)
		if err != nil {
			return err
		}
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			data2[string(k)] = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Info("Comparing database tables...")

	if len(data1) != len(data2) {
		return fmt.Errorf(
			"databases differ in number of records: DB1 has %d records, DB2 has %d",
			len(data1),
			len(data2),
		)
	}

	log.Info("Number of records in both databases match", "records 1", len(data1), "records 2", len(data2))

	for k, v1 := range data1 {
		v2, ok := data2[k]
		if !ok {
			return fmt.Errorf("key '%s' is present in DB1 but not in DB2", k)
		}
		if !bytes.Equal(v1, v2) {
			return fmt.Errorf("value mismatch for key '%s'; DB1: %v, DB2: %v", k, v1, v2)
		}
	}

	log.Info("Success: Both database tables match!")

	return nil
}

func runTables(cliCtx *cli.Context) error {
	db, err := mdbx.NewMDBX(log.New()).Path(dataDir).Open(cliCtx.Context)
	if err != nil {
		return err
	}
	defer db.Close()

	tablesMap := make(map[string]uint64)
	var tables []string
	err = db.View(cliCtx.Context, func(tx kv.Tx) error {
		tables, err = tx.ListBuckets()
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, table := range tables {
		err = db.View(cliCtx.Context, func(tx kv.Tx) error {
			c, err := tx.Cursor(table)
			if err != nil {
				return err
			}
			entries, err := c.Count()
			if err != nil {
				return err
			}
			tablesMap[table] = entries
			return nil
		})
		if err != nil {
			return err
		}
	}

	for table, entries := range tablesMap {
		log.Info("Table", "name", table, "entries", entries)
	}

	return nil
}

func runEntries(cliCtx *cli.Context) error {
	db, err := mdbx.NewMDBX(log.New()).Path(dataDir).Open(cliCtx.Context)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.View(cliCtx.Context, func(tx kv.Tx) error {
		c, err := tx.Cursor(dbTable)
		if err != nil {
			return err
		}
		entries, err := c.Count()
		if err != nil {
			return err
		}
		log.Info("Table", "name", dbTable, "entries", entries)
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			log.Info("Entry", "key", string(k), "value", string(v))
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
