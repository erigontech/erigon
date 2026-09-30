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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/fromdb"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

const (
	pbtSnapshotFileName  = "pbt-snapshot.bin"
	pbtPreimagesFileName = "framed.bin"
	pbtMetaFileName      = "pbt-snapshot.meta.json"
)

var exportPBTCommand = cli.Command{
	Name:   "export-pbt",
	Usage:  "Export a PBT snapshot and its preimages",
	Action: doExportPBT,
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{Name: "out", Value: ".", Usage: "output directory for the PBT snapshot and preimages"},
	}),
}

type pbtExportMeta struct {
	ChainID        string `json:"chainId"`
	Block          uint64 `json:"block"`
	BlockHash      string `json:"blockHash"`
	TxNum          uint64 `json:"txNum"`
	HashSuite      string `json:"hashSuite"`
	StateRoot      string `json:"stateRoot"`
	PBTRoot        string `json:"pbtRoot"`
	HeaderCount    uint64 `json:"headerCount"`
	CodeGroupCount uint64 `json:"codeGroupCount"`
	StorageCount   uint64 `json:"storageCount"`
	SnapshotDigest string `json:"snapshotDigest"`
	PreimageDigest string `json:"preimageDigest"`
	Finalized      bool   `json:"finalized"`
}

func doExportPBT(ctx context.Context, cliCtx *cli.Command) error {
	logger := log.Root()
	dirs, err := openExportDirs(cliCtx.String(utils.DataDirFlag.Name))
	if err != nil {
		return err
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
	return runExportPBT(ctx, tx, func(blockNum uint64) (*types.Header, error) {
		return br.HeaderByNumber(ctx, tx, blockNum)
	}, cliCtx.String("out"), logger)
}

func runExportPBT(ctx context.Context, tx kv.TemporalTx, headerAt func(uint64) (*types.Header, error), outDir string, logger log.Logger) error {
	return runExportPBTWithReadbackHook(ctx, tx, headerAt, outDir, logger, nil)
}

func runExportPBTWithReadbackHook(ctx context.Context, tx kv.TemporalTx, headerAt func(uint64) (*types.Header, error), outDir string, logger log.Logger, beforeReadback func(string) error) error {
	if tx == nil || headerAt == nil {
		return fmt.Errorf("export-pbt: missing input")
	}
	pin, err := sharedExportPin(ctx, tx, headerAt, logger)
	if err != nil {
		return err
	}
	header, err := headerAt(pin.Block)
	if err != nil {
		return fmt.Errorf("export-pbt: read header: %w", err)
	}
	if err := checkRootPin(pin.Root, header, pin.Block); err != nil {
		return err
	}
	root, err := exportPBTStreamRoot(tx)
	if err != nil {
		return err
	}
	binRoot, found, err := exportPBTBinRootAtPin(ctx, tx, pin, logger)
	if err != nil {
		return err
	}
	if err := checkExportPBTStreamRoot(root, pin, binRoot, found); err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	snapshotPath := filepath.Join(outDir, pbtSnapshotFileName)
	preimagePath := filepath.Join(outDir, pbtPreimagesFileName)
	metaPath := filepath.Join(outDir, pbtMetaFileName)
	_ = dir.RemoveFile(metaPath)
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = dir.RemoveFile(snapshotPath)
		_ = dir.RemoveFile(preimagePath)
		_ = dir.RemoveFile(metaPath)
	}()
	snapshotFile, err := os.Create(snapshotPath)
	if err != nil {
		return err
	}
	snapshotDigest, writeErr := artifact.WriteSnapshot(snapshotFile, root, func(emit func([]byte, []byte) error) error {
		return state.ForEachPBinLeaf(state.AggTx(tx), tx, false, func(leaf state.PBinLeaf) error {
			return emit(leaf.Key, leaf.Value)
		})
	})
	closeErr := snapshotFile.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	preimageFile, err := os.Create(preimagePath)
	if err != nil {
		return err
	}
	preimageTemp, err := os.CreateTemp(tx.Debug().Dirs().Tmp, "pbt-preimages-")
	if err != nil {
		_ = preimageFile.Close()
		return err
	}
	_, writeErr = writePreimagesFile(ctx, preimageTemp, tx.Debug().Dirs().Tmp, state.AggTx(tx), tx, logger)
	if writeErr == nil {
		if _, writeErr = preimageTemp.Seek(0, io.SeekStart); writeErr == nil {
			info, statErr := preimageTemp.Stat()
			if statErr != nil {
				writeErr = statErr
			} else {
				writeErr = artifact.WritePreimagesStream(preimageFile, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
					return artifact.ReadPreimagesStream(preimageTemp, info.Size(), yield)
				})
			}
		}
	}
	_ = preimageTemp.Close()
	_ = dir.RemoveFile(preimageTemp.Name())
	closeErr = preimageFile.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if beforeReadback != nil {
		if err := beforeReadback(snapshotPath); err != nil {
			return err
		}
	}
	snapshotRead, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer snapshotRead.Close()
	snapshotInfo, err := snapshotRead.Stat()
	if err != nil {
		return err
	}
	snapshotMeta, err := artifact.ReadSnapshotStreamAt(snapshotRead, snapshotInfo.Size(), artifact.SnapshotStreamCallbacks{})
	if err != nil {
		return fmt.Errorf("export-pbt: read back snapshot: %w", err)
	}
	preimageRead, err := os.Open(preimagePath)
	if err != nil {
		return err
	}
	defer preimageRead.Close()
	preimageInfo, err := preimageRead.Stat()
	if err != nil {
		return err
	}
	if err := artifact.JoinAt(snapshotRead, snapshotInfo.Size(), preimageRead, preimageInfo.Size(), eip8297.HashBytes, nil); err != nil {
		return fmt.Errorf("export-pbt: join preimages: %w", err)
	}
	if snapshotMeta.SnapshotDigest != snapshotDigest {
		return fmt.Errorf("export-pbt: snapshot digest changed during read-back")
	}
	preimageDigest, err := digestPBTFile(preimageRead)
	if err != nil {
		return err
	}
	chainConfig, err := exportChainConfig(tx)
	if err != nil {
		return err
	}
	chainID := "0"
	if chainConfig.ChainID != nil {
		chainID = chainConfig.ChainID.String()
	}
	meta := pbtExportMeta{
		ChainID: chainID, Block: pin.Block, BlockHash: header.Hash().Hex(), TxNum: pin.TxNum,
		HashSuite: commitment.PBinHashSuiteName(), StateRoot: header.Root.Hex(), PBTRoot: root.Hex(),
		HeaderCount: snapshotMeta.HeaderCount, CodeGroupCount: snapshotMeta.CodeGroupCount,
		StorageCount: snapshotMeta.StorageCount, SnapshotDigest: snapshotDigest.Hex(),
		PreimageDigest: preimageDigest.Hex(),
		Finalized:      rawdb.ReadForkchoiceFinalizedNum(tx) >= pin.Block,
	}
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	metaTemp, err := os.CreateTemp(outDir, ".pbt-snapshot-meta-*.tmp")
	if err != nil {
		return err
	}
	metaTempName := metaTemp.Name()
	defer func() {
		_ = metaTemp.Close()
		_ = dir.RemoveFile(metaTempName)
	}()
	if _, err := metaTemp.Write(append(metaBytes, '\n')); err != nil {
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
	return nil
}

func checkExportPBTStreamRoot(root common.Hash, pin exportPin, binRoot common.Hash, found bool) error {
	if pin.Variant == commitment.VariantBinPatriciaTrie {
		if root != pin.Root {
			return fmt.Errorf("export-pbt: stream root %s differs from bin root %s", root.Hex(), pin.Root.Hex())
		}
		return nil
	}
	if found && root != binRoot {
		return fmt.Errorf("export-pbt: stream root %s differs from bin root %s", root.Hex(), binRoot.Hex())
	}
	return nil
}

func digestPBTFile(file *os.File) (common.Hash, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return common.Hash{}, err
	}
	hash := keccak.NewFastKeccak()
	if _, err := io.Copy(hash, file); err != nil {
		return common.Hash{}, err
	}
	return common.BytesToHash(hash.Sum(nil)), nil
}

func exportPBTStreamRoot(tx kv.TemporalTx) (common.Hash, error) {
	builder, err := eip8297.NewStreamRootBuilder(eip8297.HashBytes)
	if err != nil {
		return common.Hash{}, err
	}
	err = state.ForEachPBinLeaf(state.AggTx(tx), tx, false, func(leaf state.PBinLeaf) error {
		return builder.Add(leaf.Key, leaf.Value)
	})
	if err != nil {
		return common.Hash{}, err
	}
	return builder.RootHash()
}

func exportPBTBinRootAtPin(ctx context.Context, tx kv.TemporalTx, pin exportPin, logger log.Logger) (common.Hash, bool, error) {
	domains := state.AggTx(tx).CommitmentDomains()
	if !slices.Contains(domains, kv.CommitmentBinDomain) {
		return common.Hash{}, false, nil
	}
	checkpointBlock, checkpointTx, found, err := exportCheckpoint(tx, kv.CommitmentBinDomain)
	if err != nil || !found || checkpointBlock != pin.Block || checkpointTx != pin.TxNum {
		return common.Hash{}, false, err
	}
	view, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithCommitmentDomainOnly(kv.CommitmentBinDomain))
	if err != nil {
		return common.Hash{}, false, err
	}
	defer view.Close()
	txNum, blockNum, err := view.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).SeekCommitment(ctx, tx)
	if err != nil {
		return common.Hash{}, false, err
	}
	if blockNum != pin.Block || txNum != pin.TxNum {
		return common.Hash{}, false, nil
	}
	root, err := view.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
	if err != nil {
		return common.Hash{}, false, err
	}
	return common.BytesToHash(root), true, nil
}
