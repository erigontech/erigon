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
	"github.com/erigontech/erigon/db/fromdb"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
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
	preimageSpillThreshold  = 1 << 20
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
	Block          uint64 `json:"block"`
	BlockHash      string `json:"blockHash"`
	StateRoot      string `json:"stateRoot"`
	Order          string `json:"order"`
	Accounts       uint64 `json:"accounts"`
	Storage        uint64 `json:"storage"`
	PreimageDigest string `json:"preimageDigest"`
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

	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()
	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	agg := openAgg(ctx, dirs, chainDB, logger)
	defer agg.Close()
	blockSnaps := blocksnapshots.NewRoSnapshots(cfg, dirs.Snap, logger)
	if err := blockSnaps.OpenFolder(); err != nil {
		return err
	}
	defer blockSnaps.Close()
	db, err := temporal.New(chainDB, agg, blockSnaps)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	br := freezeblocks.NewBlockReader(blockSnaps)
	headerAt := func(blockNum uint64) (*types.Header, error) {
		return br.HeaderByNumber(ctx, tx, blockNum)
	}
	return runExport(ctx, tx, headerAt, outDir, tmpDir, logger)
}

func runExport(ctx context.Context, tx kv.TemporalTx, headerAt func(uint64) (*types.Header, error), outDir, tmpDir string, logger log.Logger) error {
	pin, err := sharedExportPin(ctx, tx, headerAt, logger)
	if err != nil {
		return err
	}
	header, err := headerAt(pin.Block)
	if err != nil {
		return fmt.Errorf("read canonical header for block %d: %w", pin.Block, err)
	}
	if err := checkRootPin(pin.Root, header, pin.Block); err != nil {
		return err
	}
	logger.Info("[export-preimages] pin", "block", pin.Block, "txNum", pin.TxNum, "stateRoot", pin.Root.Hex(), "domain", pin.Domain)

	tmpDir, err = prepareScratchDir(tmpDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	framedPath, metaPath, err := preparePreimagesOutput(outDir)
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = dir.RemoveFile(framedPath)
		_ = dir.RemoveFile(metaPath)
	}()
	outputFile, err := os.Create(framedPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	start := time.Now()
	aggTx := state.AggTx(tx)
	stats, err := writePreimagesFile(ctx, outputFile, tmpDir, aggTx, tx, logger)
	if err != nil {
		return fmt.Errorf("export aborted (partial file %s): %w", framedPath, err)
	}
	info, err := outputFile.Stat()
	if err != nil {
		return err
	}
	if err := artifact.CheckPreimageSetAt(outputFile, info.Size(), func(yield func([]byte) error) error {
		return state.ForEachPBinLeaf(aggTx, tx, false, func(leaf state.PBinLeaf) error {
			if len(leaf.Key) == 0 {
				return nil
			}
			switch leaf.Key[0] {
			case eip8297.AccountZone:
				subIndex := leaf.Key[len(leaf.Key)-1]
				if subIndex != eip8297.BasicDataLeafKey && subIndex < eip8297.HeaderStorageOffset {
					return nil
				}
			case eip8297.CodeZone:
				return nil
			}
			return yield(leaf.Key)
		})
	}, eip8297.HashBytes); err != nil {
		return fmt.Errorf("export aborted (preimage set): %w", err)
	}
	preimageDigest, err := digestPBTFile(outputFile)
	if err != nil {
		return err
	}

	metadata := preimagesMeta{
		Block: pin.Block, BlockHash: header.Hash().Hex(), StateRoot: pin.Root.Hex(), Order: preimagesOrderKeccak256,
		Accounts: stats.Accounts, Storage: stats.Slots, PreimageDigest: preimageDigest.Hex(),
	}
	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	metaTemp, err := os.CreateTemp(outDir, ".preimages-meta-*.tmp")
	if err != nil {
		return err
	}
	metaTempName := metaTemp.Name()
	defer func() {
		_ = metaTemp.Close()
		_ = dir.RemoveFile(metaTempName)
	}()
	if _, err := metaTemp.Write(append(metadataJSON, '\n')); err != nil {
		return err
	}
	if err := metaTemp.Sync(); err != nil {
		return err
	}
	if err := metaTemp.Close(); err != nil {
		return err
	}
	if err := os.Rename(metaTempName, metaPath); err != nil {
		return err
	}
	if err := dir.FsyncDir(outDir); err != nil {
		return err
	}
	completed = true
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

	collector := etl.NewCollector("export-preimages", tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger).SortAndFlushInBackground(true)
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
	slotKeys := make([]byte, 0, preimageSpillThreshold)
	var slotSpill *os.File
	var slotSpillName string
	var slotCount uint64
	pending := false
	cleanupSlots := func() {
		if slotSpill != nil {
			_ = slotSpill.Close()
			slotSpill = nil
		}
		if slotSpillName != "" {
			_ = dir.RemoveFile(slotSpillName)
			slotSpillName = ""
		}
	}
	defer cleanupSlots()
	appendSlot := func(value []byte) error {
		if slotSpill == nil && len(slotKeys)+len(value) <= preimageSpillThreshold {
			slotKeys = append(slotKeys, value...)
			slotCount++
			return nil
		}
		if slotSpill == nil {
			var err error
			slotSpill, err = os.CreateTemp("", "pbt-preimage-slots-")
			if err != nil {
				return err
			}
			slotSpillName = slotSpill.Name()
			if _, err := slotSpill.Write(slotKeys); err != nil {
				return err
			}
			slotKeys = slotKeys[:0]
		}
		if _, err := slotSpill.Write(value); err != nil {
			return err
		}
		slotCount++
		return nil
	}

	// slotCount precedes the slots, so a record can only be written once its last
	// slot has arrived.
	flushRecord := func() error {
		if !pending {
			return nil
		}
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
		if slotSpill != nil {
			if _, err := slotSpill.Seek(0, io.SeekStart); err != nil {
				return err
			}
			if _, err := io.Copy(writer, slotSpill); err != nil {
				return err
			}
		}
		stats.Accounts++
		stats.Slots += slotCount
		if onProgress != nil {
			onProgress(stats)
		}
		cleanupSlots()
		slotKeys = slotKeys[:0]
		slotCount = 0
		pending = false
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
			slotCount = 0
			pending = true
			return nil
		case 2 * length.Hash:
			if len(v) != preimageSlotLen {
				return fmt.Errorf("slot under account hash %x: expected a %d-byte key, got %d bytes", k[:length.Hash], preimageSlotLen, len(v))
			}
			if !pending || !bytes.Equal(k[:length.Hash], accountHash[:]) {
				return fmt.Errorf("storage slot %x under account hash %x has no matching account", v, k[:length.Hash])
			}
			return appendSlot(v)
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
