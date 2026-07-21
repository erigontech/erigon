// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types"
)

const (
	preimageAddrLen  = 20
	preimageSlotLen  = 32
	preimageCountLen = 4

	preimagesFileName     = "framed.bin"
	preimagesMetaFileName = "preimages.meta.json"
)

var exportPreimagesCommand = cli.Command{
	Name:        "export-preimages",
	Usage:       "Export plain-key state preimages (account addresses + storage slot keys) to a framed binary file",
	Description: "Writes framed.bin plus a preimages.meta.json sidecar with the block/stateRoot pin to --out. The state root is verified against the canonical header; a mismatch aborts the export.",
	Action:      doExportPreimages,
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{Name: "out", Value: ".", Usage: "output directory for the framed file and preimages.meta.json"},
	}),
}

type preimagesMeta struct {
	Block     uint64 `json:"block"`
	StateRoot string `json:"stateRoot"`
	Accounts  uint64 `json:"accounts"`
	Storage   uint64 `json:"storage"`
}

func doExportPreimages(ctx context.Context, cliCtx *cli.Command) error {
	logger := log.Root()
	dirs, err := openExportDirs(cliCtx.String(utils.DataDirFlag.Name))
	if err != nil {
		return err
	}
	outDir := cliCtx.String("out")

	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	agg := openAgg(ctx, dirs, chainDB, logger)
	defer agg.Close()
	aggTx := agg.BeginFilesRo()
	defer aggTx.Close()
	tx, err := chainDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	commitmentState, _, ok, err := aggTx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, tx)
	if err != nil {
		return fmt.Errorf("read commitment state: %w", err)
	}
	if !ok {
		return fmt.Errorf("commitment state record not found in %s", dirs.DataDir)
	}
	rootBytes, blockNum, txNum, err := commitment.HexTrieExtractStateRoot(commitmentState)
	if err != nil {
		return fmt.Errorf("extract state root: %w", err)
	}
	commitmentRoot := common.BytesToHash(rootBytes)
	if err := checkRootPin(commitmentRoot, rawdb.ReadHeaderByNumber(tx, blockNum), blockNum); err != nil {
		return err
	}
	logger.Info("[export-preimages] pin", "block", blockNum, "txNum", txNum, "stateRoot", commitmentRoot.Hex())

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	framedPath, metaPath, err := preparePreimagesOutput(outDir)
	if err != nil {
		return err
	}
	outputFile, err := os.Create(framedPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	bufferedWriter := bufio.NewWriterSize(outputFile, 4<<20)
	countedWriter := &countingWriter{writer: bufferedWriter}

	accounts, err := aggTx.DebugRangeLatest(tx, kv.AccountsDomain, nil, nil, kv.Unlim)
	if err != nil {
		return err
	}
	defer accounts.Close()
	storage, err := aggTx.DebugRangeLatest(tx, kv.StorageDomain, nil, nil, kv.Unlim)
	if err != nil {
		return err
	}
	defer storage.Close()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	reportProgress := func(stats exportPreimagesStats) {
		select {
		case <-logEvery.C:
			logger.Info("[export-preimages] progress", "accounts", stats.Accounts, "slots", stats.Slots, "bytes", countedWriter.written)
		default:
		}
	}

	start := time.Now()
	stats, err := writeFramedPreimages(ctx, accounts, storage, countedWriter, reportProgress)
	if err != nil {
		return fmt.Errorf("export aborted (partial file %s): %w", framedPath, err)
	}
	if err := bufferedWriter.Flush(); err != nil {
		return err
	}
	if err := outputFile.Sync(); err != nil {
		return err
	}
	sizeBytes := stats.sizeBytes()
	if countedWriter.written != sizeBytes {
		return fmt.Errorf("size mismatch: wrote %d bytes, formula says %d", countedWriter.written, sizeBytes)
	}

	metadata := preimagesMeta{
		Block: blockNum, StateRoot: commitmentRoot.Hex(),
		Accounts: stats.Accounts, Storage: stats.Slots,
	}
	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(metaPath, append(metadataJSON, '\n'), 0o644); err != nil {
		return err
	}
	logger.Info("[export-preimages] done", "accounts", stats.Accounts, "slots", stats.Slots, "file", framedPath, "bytes", sizeBytes, "took", time.Since(start).Round(time.Second))
	return nil
}

func openExportDirs(dataDir string) (datadir.Dirs, error) {
	dirs := datadir.Open(dataDir)
	info, err := os.Stat(dirs.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return dirs, fmt.Errorf("datadir does not exist: %s", dirs.DataDir)
		}
		return dirs, err
	}
	if !info.IsDir() {
		return dirs, fmt.Errorf("datadir is not a directory: %s", dirs.DataDir)
	}
	return dirs, nil
}

func preparePreimagesOutput(outDir string) (framedPath, metaPath string, err error) {
	framedPath = filepath.Join(outDir, preimagesFileName)
	metaPath = filepath.Join(outDir, preimagesMetaFileName)
	if err := dir.RemoveFile(metaPath); err != nil && !os.IsNotExist(err) {
		return "", "", fmt.Errorf("remove stale metadata %s: %w", metaPath, err)
	}
	return framedPath, metaPath, nil
}

type countingWriter struct {
	writer  io.Writer
	written uint64
}

func (c *countingWriter) Write(data []byte) (int, error) {
	n, err := c.writer.Write(data)
	c.written += uint64(n)
	return n, err
}

func checkRootPin(commitmentRoot common.Hash, header *types.Header, blockNum uint64) error {
	if header == nil {
		return fmt.Errorf("canonical header for block %d not found; cannot verify state root %s", blockNum, commitmentRoot.Hex())
	}
	if header.Root != commitmentRoot {
		return fmt.Errorf("state root mismatch at block %d: commitment %s vs header %s", blockNum, commitmentRoot.Hex(), header.Root.Hex())
	}
	return nil
}

type exportPreimagesStats struct {
	Accounts uint64
	Slots    uint64
}

func (stats exportPreimagesStats) sizeBytes() uint64 {
	return stats.Accounts*(preimageAddrLen+preimageCountLen) + stats.Slots*preimageSlotLen
}

// writeFramedPreimages writes plain-key ordered records as:
// address[20] | slotCount[4, big-endian] | slotKey[32] * slotCount.
func writeFramedPreimages(
	ctx context.Context,
	accounts, storage stream.KV,
	writer io.Writer,
	onProgress func(exportPreimagesStats),
) (exportPreimagesStats, error) {
	var stats exportPreimagesStats
	slotKeys := make([]byte, 0, 1<<20)

	// A non-blocking Done check is lock-free on the hot path, unlike ctx.Err,
	// which takes a mutex on every call.
	done := ctx.Done()

	var storageKey []byte
	hasStorage := false
	advanceStorage := func() error {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		hasStorage = storage.HasNext()
		if !hasStorage {
			return nil
		}
		var err error
		storageKey, _, err = storage.Next()
		return err
	}
	if err := advanceStorage(); err != nil {
		return stats, err
	}

	var recordHeader [preimageAddrLen + preimageCountLen]byte
	address := recordHeader[:preimageAddrLen]
	for accounts.HasNext() {
		select {
		case <-done:
			return stats, ctx.Err()
		default:
		}
		accountKey, _, err := accounts.Next()
		if err != nil {
			return stats, err
		}
		if len(accountKey) != preimageAddrLen {
			return stats, fmt.Errorf("account: unexpected key length %d: %x", len(accountKey), accountKey)
		}
		// Stream keys are only valid for two Next calls; copy to hold the
		// address across storage iterator advances.
		copy(address, accountKey)

		slotKeys = slotKeys[:0]
		var slotCount uint64
		for hasStorage {
			if len(storageKey) != preimageAddrLen+preimageSlotLen {
				return stats, fmt.Errorf("storage: unexpected key length %d: %x", len(storageKey), storageKey)
			}
			addressOrder := bytes.Compare(storageKey[:preimageAddrLen], address)
			if addressOrder > 0 {
				break
			}
			if addressOrder < 0 {
				return stats, fmt.Errorf("storage key %x has no matching account", storageKey)
			}
			slotKeys = append(slotKeys, storageKey[preimageAddrLen:]...)
			slotCount++
			if err := advanceStorage(); err != nil {
				return stats, err
			}
		}

		if slotCount > math.MaxUint32 {
			return stats, fmt.Errorf("account %x has %d slots (> uint32)", address, slotCount)
		}
		binary.BigEndian.PutUint32(recordHeader[preimageAddrLen:], uint32(slotCount))
		if _, err := writer.Write(recordHeader[:]); err != nil {
			return stats, err
		}
		if _, err := writer.Write(slotKeys); err != nil {
			return stats, err
		}
		stats.Accounts++
		stats.Slots += slotCount
		if onProgress != nil {
			onProgress(stats)
		}
	}
	if hasStorage {
		return stats, fmt.Errorf("storage key %x has no matching account", storageKey)
	}
	return stats, nil
}
