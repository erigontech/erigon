package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/utils"
	types "github.com/ledgerwatch/erigon/zk/rpcdaemon"
)

var (
	// common flags
	verboseFlag = &cli.BoolFlag{
		Name: "verbose",
		Usage: "If verbose output is enabled, it prints all the details about blocks and transactions in the batches, " +
			"otherwise just its hashes",
		Destination: &verboseOutput,
		Value:       false,
	}

	fileOutputFlag = &cli.BoolFlag{
		Name:        "file-output",
		Usage:       "If file output is enabled, all the results are persisted within a file",
		Destination: &fileOutput,
		Value:       false,
	}

	// commands
	getBatchByNumberCmd = &cli.Command{
		Action: dumpBatchesByNumbers,
		Name:   "output-batches",
		Usage:  "Outputs batches by numbers",
		Flags: []cli.Flag{
			&utils.DataDirFlag,
			&cli.Uint64SliceFlag{
				Name:        "bn",
				Usage:       "Batch numbers",
				Destination: batchOrBlockNumbers,
			},
			verboseFlag,
			fileOutputFlag,
		},
	}

	getBlockByNumberCmd = &cli.Command{
		Action: dumpBlocksByNumbers,
		Name:   "output-blocks",
		Usage:  "Outputs blocks by numbers",
		Flags: []cli.Flag{
			&utils.DataDirFlag,
			&cli.Uint64SliceFlag{
				Name:        "bn",
				Usage:       "Block numbers",
				Destination: batchOrBlockNumbers,
			},
			verboseFlag,
			fileOutputFlag,
		},
	}

	getBatchAffiliationCmd = &cli.Command{
		Action: dumpBatchAffiliation,
		Name:   "output-batch-affiliation",
		Usage:  "Outputs batch affiliation for provided block numbers",
		Flags: []cli.Flag{
			&utils.DataDirFlag,
			&cli.Uint64SliceFlag{
				Name:        "bn",
				Usage:       "Block numbers",
				Destination: batchOrBlockNumbers,
			},
			fileOutputFlag,
		},
	}

	// parameters
	chainDataDir        string
	batchOrBlockNumbers *cli.Uint64Slice = cli.NewUint64Slice()
	verboseOutput       bool
	fileOutput          bool
)

// dumpBatchesByNumbers retrieves batches by given numbers and dumps them either on standard output or to a file
func dumpBatchesByNumbers(cliCtx *cli.Context) error {
	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("chain data directory is not provided")
	}

	chainDataDir = cliCtx.String(utils.DataDirFlag.Name)

	tx, cleanup, err := createDbTx(chainDataDir, cliCtx.Context)
	if err != nil {
		return fmt.Errorf("failed to create read-only db transaction: %w", err)
	}
	defer cleanup()

	r := NewDbDataRetriever(tx)
	batches := make([]*types.Batch, 0, len(batchOrBlockNumbers.Value()))
	for _, batchNum := range batchOrBlockNumbers.Value() {
		batch, err := r.GetBatchByNumber(batchNum, verboseOutput)
		if err != nil {
			return fmt.Errorf("failed to retrieve the batch %d: %w", batchNum, err)
		}

		batches = append(batches, batch)
	}

	jsonBatches, err := json.MarshalIndent(batches, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize batches into the JSON format: %w", err)
	}

	if err := outputResults(string(jsonBatches)); err != nil {
		return fmt.Errorf("failed to output results: %w", err)
	}

	return nil
}

// dumpBlocksByNumbers retrieves batches by given numbers and dumps them either on standard output or to a file
func dumpBlocksByNumbers(cliCtx *cli.Context) error {
	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("chain data directory is not provided")
	}

	chainDataDir = cliCtx.String(utils.DataDirFlag.Name)

	tx, cleanup, err := createDbTx(chainDataDir, cliCtx.Context)
	if err != nil {
		return fmt.Errorf("failed to create read-only db transaction: %w", err)
	}
	defer cleanup()

	r := NewDbDataRetriever(tx)
	blocks := make([]*types.Block, 0, len(batchOrBlockNumbers.Value()))
	for _, blockNum := range batchOrBlockNumbers.Value() {
		block, err := r.GetBlockByNumber(blockNum, verboseOutput, verboseOutput)
		if err != nil {
			return fmt.Errorf("failed to retrieve the block %d: %w", blockNum, err)
		}

		blocks = append(blocks, block)
	}

	jsonBlocks, err := json.MarshalIndent(blocks, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize blocks into the JSON format: %w", err)
	}

	if err := outputResults(string(jsonBlocks)); err != nil {
		return fmt.Errorf("failed to output results: %w", err)
	}

	return nil
}

// dumpBatchAffiliation retrieves batch numbers by given block numbers and dumps them either on standard output or to a file
func dumpBatchAffiliation(cliCtx *cli.Context) error {
	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("chain data directory is not provided")
	}

	chainDataDir = cliCtx.String(utils.DataDirFlag.Name)

	tx, cleanup, err := createDbTx(chainDataDir, cliCtx.Context)
	if err != nil {
		return fmt.Errorf("failed to create read-only db transaction: %w", err)
	}
	defer cleanup()

	r := NewDbDataRetriever(tx)
	batchInfo, err := r.GetBatchAffiliation(batchOrBlockNumbers.Value())
	if err != nil {
		return err
	}
	jsonBatchAffiliation, err := json.MarshalIndent(batchInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize batch affiliation info into the JSON format: %w", err)
	}

	if err := outputResults(string(jsonBatchAffiliation)); err != nil {
		return fmt.Errorf("failed to output results: %w", err)
	}
	return nil
}

// createDbTx creates a read-only database transaction, that allows querying it.
func createDbTx(chainDataDir string, ctx context.Context) (kv.Tx, func(), error) {
	db := mdbx.MustOpen(chainDataDir)
	dbTx, err := db.BeginRo(ctx)
	cleanupFn := func() {
		dbTx.Rollback()
		db.Close()
	}

	return dbTx, cleanupFn, err
}

// outputResults prints results either to the terminal or to the file
func outputResults(results string) error {
	// output results to the file
	if fileOutput {
		formattedTime := time.Now().Format("02-01-2006 15:04:05")
		fileName := fmt.Sprintf("output_%s.json", formattedTime)

		file, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}
		defer file.Close()

		_, err = file.Write([]byte(results))
		if err != nil {
			return err
		}

		path, err := filepath.Abs(file.Name())
		if err != nil {
			return err
		}

		fmt.Printf("results are written to the '%s'\n", path)
		return nil
	}

	// output results to the standard output
	fmt.Println(results)

	return nil
}
