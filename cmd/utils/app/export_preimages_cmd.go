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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
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

	// Records are ordered by keccak256 of the plain key. A file written before that
	// was pinned is shape-identical, so consumers must require this field rather than
	// infer the order from the layout.
	preimagesOrderKeccak256 = "keccak256"

	// Scratch lives in a directory this command owns, so leftovers from a killed run
	// can be cleared without touching anyone else's temp files.
	preimagesScratchDirName = "export-preimages"
)

var exportPreimagesCommand = cli.Command{
	Name:        "export-preimages",
	Usage:       "Export plain-key state preimages (account addresses + storage slot keys) to a framed binary file",
	Description: "Writes framed.bin plus a preimages.meta.json sidecar with the block/stateRoot pin to --out. Records are ordered by keccak256 of the plain key, per EIP-8347. The state root is verified against the canonical header; a mismatch aborts the export.",
	Hidden:      true,
	Action:      doExportPreimages,
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{Name: "out", Value: ".", Usage: "output directory for the framed file and preimages.meta.json"},
		&cli.StringFlag{Name: "tmpdir", Usage: "scratch directory for the external sort, sized for the whole key set (default: <datadir>/temp)"},
	}),
}

type preimagesMeta struct {
	Block     uint64 `json:"block"`
	StateRoot string `json:"stateRoot"`
	Order     string `json:"order"`
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
	tmpDir := cliCtx.String("tmpdir")
	if tmpDir == "" {
		tmpDir = dirs.Tmp
	}
	tmpDir, err = prepareScratchDir(tmpDir)
	if err != nil {
		return err
	}

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

	commitmentState, _, ok, err := aggTx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, tx, kv.GetLatestOptions{})
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

	start := time.Now()
	stats, err := writePreimagesFile(ctx, outputFile, tmpDir, aggTx, tx, logger)
	if err != nil {
		return fmt.Errorf("export aborted (partial file %s): %w", framedPath, err)
	}

	metadata := preimagesMeta{
		Block: blockNum, StateRoot: commitmentRoot.Hex(), Order: preimagesOrderKeccak256,
		Accounts: stats.Accounts, Storage: stats.Slots,
	}
	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(metaPath, append(metadataJSON, '\n'), 0o644); err != nil {
		return err
	}
	logger.Info("[export-preimages] done", "accounts", stats.Accounts, "slots", stats.Slots, "file", framedPath, "bytes", stats.sizeBytes(), "took", time.Since(start).Round(time.Second))
	return nil
}

// writePreimagesFile hashes both domain scans through an ETL sort and writes the
// framed file. Every error it returns leaves outputFile partially written.
func writePreimagesFile(
	ctx context.Context,
	outputFile *os.File,
	tmpDir string,
	aggTx *state.AggregatorRoTx,
	tx kv.Tx,
	logger log.Logger,
) (exportPreimagesStats, error) {
	var stats exportPreimagesStats

	accounts, err := aggTx.DebugRangeLatest(tx, kv.AccountsDomain, nil, nil, kv.Unlim)
	if err != nil {
		return stats, err
	}
	defer accounts.Close()
	storage, err := aggTx.DebugRangeLatest(tx, kv.StorageDomain, nil, nil, kv.Unlim)
	if err != nil {
		return stats, err
	}
	defer storage.Close()

	bufferedWriter := bufio.NewWriterSize(outputFile, 4<<20)
	countedWriter := &countingWriter{writer: bufferedWriter}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	reportHashing := func(stats exportPreimagesStats) {
		select {
		case <-logEvery.C:
			logger.Info("[export-preimages] hashing", "accounts", stats.Accounts, "slots", stats.Slots)
		default:
		}
	}
	reportWriting := func(stats exportPreimagesStats) {
		select {
		case <-logEvery.C:
			logger.Info("[export-preimages] writing", "accounts", stats.Accounts, "slots", stats.Slots, "bytes", countedWriter.written)
		default:
		}
	}

	collector := etl.NewCollector("export-preimages", tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer collector.Close()

	collected, err := collectHashedPreimages(ctx, accounts, storage, collector, reportHashing)
	if err != nil {
		return collected, err
	}
	stats, err = writeHashedPreimages(ctx, collector, countedWriter, reportWriting)
	if err != nil {
		return stats, err
	}
	if stats != collected {
		return stats, fmt.Errorf("sort lost keys: collected %d accounts / %d slots, wrote %d / %d",
			collected.Accounts, collected.Slots, stats.Accounts, stats.Slots)
	}
	if err := bufferedWriter.Flush(); err != nil {
		return stats, err
	}
	if err := outputFile.Sync(); err != nil {
		return stats, err
	}
	if sizeBytes := stats.sizeBytes(); countedWriter.written != sizeBytes {
		return stats, fmt.Errorf("size mismatch: wrote %d bytes, formula says %d", countedWriter.written, sizeBytes)
	}
	return stats, nil
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

// prepareScratchDir gives the sort a directory of its own under tmpDir and empties it first.
// Only collector.Close() unlinks the spill, so a SIGKILL or OOM mid-export strands it -- on
// mainnet that is >100 GB, and the retry would then hit ENOSPC partway through.
func prepareScratchDir(tmpDir string) (string, error) {
	scratch := filepath.Join(tmpDir, preimagesScratchDirName)
	if err := dir.RemoveAll(scratch); err != nil {
		return "", fmt.Errorf("clear scratch %s: %w", scratch, err)
	}
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		return "", err
	}
	return scratch, nil
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

// collectHashedPreimages keys every plain key by its MPT path: keccak256(address)
// for an account, keccak256(address)||keccak256(slotKey) for a storage slot. An
// account's key is a strict prefix of its own slots' keys and of nothing else, so
// the merge-sorted load emits each account immediately ahead of its slots, both
// in the EIP-8347 order.
//
// Both domain scans are ascending by plain key and a storage key starts with its
// address, so walking them together keeps the account for the storage run at hand:
// that both reuses its hash across the run and rejects an orphaned slot here,
// rather than after the whole key set has spilled to disk.
func collectHashedPreimages(
	ctx context.Context,
	accounts, storage stream.KV,
	collector *etl.Collector,
	onProgress func(exportPreimagesStats),
) (exportPreimagesStats, error) {
	var stats exportPreimagesStats

	// A non-blocking Done check is lock-free on the hot path, unlike ctx.Err,
	// which takes a mutex on every call.
	done := ctx.Done()

	var accountAddr [preimageAddrLen]byte
	var accountHash common.Hash
	var hashedKey [2 * length.Hash]byte
	haveAccount := false

	nextAccount := func() error {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if !accounts.HasNext() {
			haveAccount = false
			return nil
		}
		accountKey, _, err := accounts.Next()
		if err != nil {
			return err
		}
		if len(accountKey) != preimageAddrLen {
			return fmt.Errorf("account: unexpected key length %d: %x", len(accountKey), accountKey)
		}
		copy(accountAddr[:], accountKey)
		accountHash = crypto.Keccak256Hash(accountAddr[:])
		haveAccount = true
		if err := collector.Collect(accountHash[:], accountAddr[:]); err != nil {
			return err
		}
		stats.Accounts++
		if onProgress != nil {
			onProgress(stats)
		}
		return nil
	}

	if err := nextAccount(); err != nil {
		return stats, err
	}
	for storage.HasNext() {
		select {
		case <-done:
			return stats, ctx.Err()
		default:
		}
		storageKey, _, err := storage.Next()
		if err != nil {
			return stats, err
		}
		if len(storageKey) != preimageAddrLen+preimageSlotLen {
			return stats, fmt.Errorf("storage: unexpected key length %d: %x", len(storageKey), storageKey)
		}
		address, slotKey := storageKey[:preimageAddrLen], storageKey[preimageAddrLen:]
		for haveAccount && bytes.Compare(accountAddr[:], address) < 0 {
			if err := nextAccount(); err != nil {
				return stats, err
			}
		}
		if !haveAccount || !bytes.Equal(accountAddr[:], address) {
			return stats, fmt.Errorf("storage slot %x under address %x has no matching account", slotKey, address)
		}
		slotHash := crypto.Keccak256Hash(slotKey)
		copy(hashedKey[:length.Hash], accountHash[:])
		copy(hashedKey[length.Hash:], slotHash[:])
		if err := collector.Collect(hashedKey[:], slotKey); err != nil {
			return stats, err
		}
		stats.Slots++
	}
	for haveAccount {
		if err := nextAccount(); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

// writeHashedPreimages drains the collector into records of
// address[20] | slotCount[4, big-endian] | slotKey[32] * slotCount, so a record
// is 24+32*slotCount bytes and only its fields are fixed width.
func writeHashedPreimages(
	ctx context.Context,
	collector *etl.Collector,
	writer io.Writer,
	onProgress func(exportPreimagesStats),
) (exportPreimagesStats, error) {
	var stats exportPreimagesStats
	var recordHeader [preimageAddrLen + preimageCountLen]byte
	var accountHash common.Hash
	slotKeys := make([]byte, 0, 1<<20)
	pending := false

	// slotCount precedes the slots, so a record can only be written once its last
	// slot has arrived.
	flushRecord := func() error {
		if !pending {
			return nil
		}
		slotCount := uint64(len(slotKeys) / preimageSlotLen)
		if slotCount > math.MaxUint32 {
			return fmt.Errorf("account %x has %d slots (> uint32)", recordHeader[:preimageAddrLen], slotCount)
		}
		binary.BigEndian.PutUint32(recordHeader[preimageAddrLen:], uint32(slotCount))
		if _, err := writer.Write(recordHeader[:]); err != nil {
			return err
		}
		if _, err := writer.Write(slotKeys); err != nil {
			return err
		}
		stats.Accounts++
		stats.Slots += slotCount
		pending = false
		if onProgress != nil {
			onProgress(stats)
		}
		return nil
	}

	loadFunc := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		switch len(k) {
		case length.Hash:
			if len(v) != preimageAddrLen {
				return fmt.Errorf("account hash %x: expected a %d-byte address, got %d bytes", k, preimageAddrLen, len(v))
			}
			if err := flushRecord(); err != nil {
				return err
			}
			copy(recordHeader[:preimageAddrLen], v)
			accountHash = common.BytesToHash(k)
			slotKeys = slotKeys[:0]
			pending = true
			return nil
		case 2 * length.Hash:
			if len(v) != preimageSlotLen {
				return fmt.Errorf("slot under account hash %x: expected a %d-byte key, got %d bytes", k[:length.Hash], preimageSlotLen, len(v))
			}
			if !pending || !bytes.Equal(k[:length.Hash], accountHash[:]) {
				return fmt.Errorf("storage slot %x under account hash %x has no matching account", v, k[:length.Hash])
			}
			slotKeys = append(slotKeys, v...)
			return nil
		default:
			return fmt.Errorf("collector: unexpected key length %d: %x", len(k), k)
		}
	}

	if err := collector.Load(nil, "", loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		// Load wraps a cancellation as "loadIntoTable : stopped", which no longer
		// matches errors.Is(err, context.Canceled).
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stats, ctxErr
		}
		return stats, err
	}
	// flushRecord mutates stats, so it cannot share a return statement with it.
	err := flushRecord()
	return stats, err
}
