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
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/urfave/cli/v3"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

var errVerifyPBTInvalid = errors.New("verify-pbt: artifact rejected")

var errVerifyPBTConfig = errors.New("verify-pbt: invalid configuration")

var verifyPBTCommand = cli.Command{
	Name:   "verify-pbt",
	Usage:  "Verify a BLAKE3 PBT snapshot and preimages against the canonical MPT state",
	Action: doVerifyPBT,
	OnUsageError: func(_ context.Context, _ *cli.Command, err error, _ bool) error {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return cli.Exit("", 2)
	},
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{Name: "snapshot", Usage: "PBT snapshot artifact"},
		&cli.StringFlag{Name: "preimages", Usage: "PBT preimages artifact"},
		&cli.StringFlag{Name: "tmpdir", Usage: "scratch directory"},
		&cli.Uint64Flag{Name: "block", Usage: "canonical anchor block"},
		&cli.Uint64Flag{Name: "max-code-size", Value: params.MaxCodeSizeAmsterdam, Usage: "maximum accepted code size in bytes"},
	}),
}

func doVerifyPBT(ctx context.Context, cliCtx *cli.Command) error {
	if args := cliCtx.Args(); args != nil && args.Len() != 0 {
		return pbtVerifyUsageError(fmt.Errorf("verify-pbt: unexpected positional arguments: %s", cliCtx.Args().Slice()))
	}
	if cliCtx.String("snapshot") == "" || cliCtx.String("preimages") == "" || !cliCtx.IsSet("block") {
		return pbtVerifyUsageError(errors.New("verify-pbt: --snapshot, --preimages and --block are required"))
	}
	maxCodeSize := uint64(params.MaxCodeSizeAmsterdam)
	if cliCtx.IsSet("max-code-size") {
		maxCodeSize = cliCtx.Uint64("max-code-size")
	}
	if maxCodeSize > uint64(^uint32(0)) {
		return pbtVerifyUsageError(fmt.Errorf("%w: --max-code-size must be at most %d", errVerifyPBTConfig, ^uint32(0)))
	}
	err := verifyPBTFilesWithMaxCodeSize(ctx, cliCtx.String(utils.DataDirFlag.Name), cliCtx.String("snapshot"), cliCtx.String("preimages"), cliCtx.Uint64("block"), maxCodeSize, cliCtx.String("tmpdir"))
	if err == nil {
		return nil
	}
	_, _ = fmt.Fprintln(os.Stderr, err)
	if errors.Is(err, errVerifyPBTInvalid) {
		return cli.Exit(err, 1)
	}
	return cli.Exit(err, 2)
}

func pbtVerifyUsageError(err error) error {
	_, _ = fmt.Fprintln(os.Stderr, err)
	return cli.Exit(err, 2)
}

func verifyPBTFiles(ctx context.Context, dataDir, snapshotPath, preimagesPath string, block uint64, scratchDirs ...string) (err error) {
	return verifyPBTFilesWithMaxCodeSize(ctx, dataDir, snapshotPath, preimagesPath, block, uint64(params.MaxCodeSizeAmsterdam), scratchDirs...)
}

func verifyPBTFilesWithMaxCodeSize(ctx context.Context, dataDir, snapshotPath, preimagesPath string, block, maxCodeSize uint64, scratchDirs ...string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredErr, ok := recovered.(error); ok {
				err = fmt.Errorf("%w: malformed input: %w", errVerifyPBTInvalid, recoveredErr)
			} else {
				err = fmt.Errorf("%w: malformed input: %v", errVerifyPBTInvalid, recovered)
			}
		}
	}()

	dirs, err := openExportDirs(dataDir)
	if err != nil {
		return fmt.Errorf("verify-pbt: %w", err)
	}
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer snapshot.Close()
	preimages, err := os.Open(preimagesPath)
	if err != nil {
		return err
	}
	defer preimages.Close()
	snapshotInfo, err := snapshot.Stat()
	if err != nil {
		return err
	}
	if !snapshotInfo.Mode().IsRegular() {
		return fmt.Errorf("verify-pbt: snapshot is not a regular file: %s", snapshotPath)
	}
	preimageInfo, err := preimages.Stat()
	if err != nil {
		return err
	}
	if !preimageInfo.Mode().IsRegular() {
		return fmt.Errorf("verify-pbt: preimages are not a regular file: %s", preimagesPath)
	}
	headerRoot, err := readPBTHeaderRoot(ctx, dataDir, block)
	if err != nil {
		return err
	}
	tmpRoot := dirs.Tmp
	if len(scratchDirs) > 0 && scratchDirs[0] != "" {
		tmpRoot = scratchDirs[0]
		if err := os.MkdirAll(tmpRoot, 0o755); err != nil {
			return err
		}
	} else {
		info, err := os.Stat(tmpRoot)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("verify-pbt: scratch path is not a directory: %s", tmpRoot)
		}
	}
	tmp, err := os.MkdirTemp(tmpRoot, "verify-pbt-")
	if err != nil {
		return err
	}
	defer func() {
		if removeErr := dir.RemoveAll(tmp); err == nil {
			err = removeErr
		}
	}()
	root, err := verifyPBTStreamingState(snapshot, snapshotInfo.Size(), preimages, preimageInfo.Size(), tmp, maxCodeSize)
	if err != nil {
		if errors.Is(err, errVerifyPBTConfig) {
			return err
		}
		if !pbtVerifyIsIOError(err) {
			return fmt.Errorf("%w: %w", errVerifyPBTInvalid, err)
		}
		return err
	}
	if headerRoot != root {
		return fmt.Errorf("%w: artifact MPT root %x differs from header root %x", errVerifyPBTInvalid, root, headerRoot)
	}
	return nil
}

func pbtVerifyIsIOError(err error) bool {
	var pathErr *os.PathError
	var linkErr *os.LinkError
	return errors.As(err, &pathErr) || errors.As(err, &linkErr) || errors.Is(err, errPBTVerifyScratchIO) || errors.Is(err, syscall.EFBIG) || errors.Is(err, syscall.ENOSPC)
}

func readPBTHeaderRoot(ctx context.Context, dataDir string, block uint64) (common.Hash, error) {
	dirs := datadir.Open(dataDir)
	db, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return common.Hash{}, err
	}
	defer db.Close()
	var chainName string
	err = db.View(ctx, func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		config, err := rawdb.ReadChainConfig(tx, genesisHash)
		if err != nil {
			return err
		}
		if config == nil {
			return errors.New("verify-pbt: chain config is missing")
		}
		chainName = config.ChainName
		return nil
	})
	if err != nil {
		return common.Hash{}, err
	}
	snapshots := blocksnapshots.NewRoSnapshots(ethconfig.NewSnapCfg(false, true, true, chainName), dirs.Snap, log.Root())
	if err := snapshots.OpenFolder(); err != nil {
		snapshots.Close()
		return common.Hash{}, err
	}
	defer snapshots.Close()
	view := snapshots.View()
	defer view.Close()
	reader := freezeblocks.NewBlockReader(snapshots)
	var header *types.Header
	err = db.View(ctx, func(tx kv.Tx) error {
		blockTx := pbtVerifyBlockFilesTx{Tx: tx, view: view}
		canonicalHash, ok, err := reader.CanonicalHash(ctx, blockTx, block)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("verify-pbt: block %d header is missing", block)
		}
		header, err = reader.HeaderByHash(ctx, blockTx, canonicalHash)
		return err
	})
	if err != nil {
		return common.Hash{}, err
	}
	if header == nil {
		return common.Hash{}, fmt.Errorf("verify-pbt: block %d header is missing", block)
	}
	return header.Root, nil
}

type pbtVerifyBlockFilesTx struct {
	kv.Tx
	view *blocksnapshots.View
}

func (tx pbtVerifyBlockFilesTx) BlockFilesRoTx() *blocksnapshots.View { return tx.view }

func pbtVerifyHash(value []byte) common.Hash { return common.Hash(blake3.Sum256(value)) }
