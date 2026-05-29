// Copyright 2024 The Erigon Authors
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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/holiman/uint256"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cmd/erigon/node"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/eth"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

const (
	importBatchSize = 2500
)

var importCommand = cli.Command{
	Action:    MigrateFlags(importChain),
	Name:      "import",
	Usage:     "Import a blockchain file",
	ArgsUsage: "<filename> (<filename 2> ... <filename N>) ",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&utils.ChainFlag,
		&utils.NetworkIdFlag,
	},
	//Category: "BLOCKCHAIN COMMANDS",
	Description: `
The import command imports blocks from an RLP-encoded form. The form can be one file
with several RLP-encoded blocks, or several files can be used.

If only one file is used, import error will result in failure. If several files are used,
processing will proceed even if an individual RLP-file import failure occurs.`,
}

func importChain(cliCtx *cli.Context) error {
	if cliCtx.NArg() < 1 {
		utils.Fatalf("This command requires an argument.")
	}

	// Force-disable subsystems the one-shot import doesn't need, via the normal
	// flag path so the downstream config build skips their setup entirely.
	// --nat=none in particular avoids a ~2s UPnP/NAT-PMP autodiscovery probe in
	// downloadernat.DoNat that otherwise dominates per-run startup (observable
	// in hive eest/consume-rlp).
	for flag, value := range map[string]string{
		utils.NATFlag.Name:               "none",
		utils.NoDownloaderFlag.Name:      "true",
		utils.ExternalConsensusFlag.Name: "true",
	} {
		if err := cliCtx.Set(flag, value); err != nil {
			return fmt.Errorf("importChain: set %s=%s: %w", flag, value, err)
		}
	}

	logger, tracer, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}

	nodeCfg, err := node.NewNodConfigUrfave(cliCtx, nil, logger)
	if err != nil {
		return err
	}

	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg, logger)
	stack := makeConfigNode(cliCtx.Context, nodeCfg, logger)
	defer stack.Close()

	ethereum, err := eth.New(cliCtx.Context, stack, ethCfg, logger, tracer)
	if err != nil {
		return err
	}
	err = ethereum.Init(stack, ethCfg, ethCfg.Genesis.Config)
	if err != nil {
		return err
	}

	return importFiles(cliCtx.Args().Slice(), logger, func(fn string) error {
		return ImportChain(ethereum, ethereum.ChainDB(), fn, logger)
	})
}

// importFiles imports each file in order; with more than one file, per-file
// failures are logged and skipped (matching go-ethereum) rather than aborting.
func importFiles(files []string, logger log.Logger, importOne func(fn string) error) error {
	var importErr error
	for _, fn := range files {
		if err := importOne(fn); err != nil {
			importErr = err
			if len(files) > 1 {
				logger.Error("Import error", "file", fn, "err", err)
			}
		}
	}
	return importErr
}

func ImportChain(ethereum *eth.Ethereum, chainDB kv.RwDB, fn string, logger log.Logger) error {
	// Watch for Ctrl-C while the import is running.
	// If a signal is received, the import will stop at the next batch.
	interrupt := make(chan os.Signal, 1)
	stop := make(chan struct{})
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			logger.Info("Interrupted during import, stopping at next batch")
		}
		close(stop)
	}()
	checkInterrupt := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	logger.Info("Importing blockchain", "file", fn)

	// Open the file handle and potentially unwrap the gzip stream
	fh, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer fh.Close()

	var reader io.Reader = fh
	if strings.HasSuffix(fn, ".gz") {
		if reader, err = gzip.NewReader(reader); err != nil {
			return err
		}
	}
	stream := rlp.NewStream(reader, 0)

	// Run actual the import.
	blocks := make(types.Blocks, importBatchSize)
	n := 0
	for batch := 0; ; batch++ {
		// Load a batch of RLP blocks.
		if checkInterrupt() {
			return errors.New("interrupted")
		}
		i := 0
		for ; i < importBatchSize; i++ {
			var b types.Block
			if err := stream.Decode(&b); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return fmt.Errorf("at block %d: %v", n, err)
			}
			// don't import first block
			if b.NumberU64() == 0 {
				i--
				continue
			}
			blocks[i] = &b
			n++
		}
		if i == 0 {
			break
		}
		// Import the batch.
		if checkInterrupt() {
			return errors.New("interrupted")
		}

		br, _ := ethereum.BlockIO()
		missing := missingBlocks(chainDB, blocks[:i], br)
		if len(missing) == 0 {
			logger.Info("Skipping batch as all blocks present", "batch", batch, "first", blocks[0].Hash(), "last", blocks[i-1].Hash())
			continue
		}

		// RLP decoding worked, try to insert into chain:
		missingChain := &blockgen.ChainPack{
			Blocks:   missing,
			TopBlock: missing[len(missing)-1],
		}

		if err := InsertChain(ethereum, missingChain, true); err != nil {
			return err
		}
	}
	return nil
}

func ChainHasBlock(chainDB kv.RwDB, block *types.Block) bool {
	var chainHasBlock bool

	chainDB.View(context.Background(), func(tx kv.Tx) (err error) {
		chainHasBlock = rawdb.HasBlock(tx, block.Hash(), block.NumberU64())
		return nil
	})

	return chainHasBlock
}

func missingBlocks(chainDB kv.RwDB, blocks []*types.Block, blockReader services.FullBlockReader) []*types.Block {
	var headBlock *types.Block
	chainDB.View(context.Background(), func(tx kv.Tx) (err error) {
		headBlock, err = blockReader.CurrentBlock(tx)
		return err
	})

	for i, block := range blocks {
		// If we're behind the chain head, only check block, state is available at head
		if headBlock.NumberU64() > block.NumberU64() {
			if !ChainHasBlock(chainDB, block) {
				return blocks[i:]
			}
			continue
		}

		if !ChainHasBlock(chainDB, block) {
			return blocks[i:]
		}
	}

	return nil
}

type stateChangesClient interface {
	StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error)
}

func InsertChain(ethereum *eth.Ethereum, chain *blockgen.ChainPack, setHead bool) error {
	if len(chain.Blocks) == 0 {
		return nil
	}
	for _, block := range chain.Blocks {
		if err := block.HashCheck(true); err != nil {
			return err
		}
	}

	ctx := context.Background()

	// Compare the imported chain's total difficulty against the current canonical
	// head's via the same PoW fork-choice rule as the legacy header sync
	// (ethash.ShouldReorg). If the imported chain doesn't win, persist
	// the blocks as a side chain (without changing head or executing them), so
	// future imports extending this branch can still be validated. Once a
	// side-chain extension surpasses the canonical TD, the regular
	// InsertBlocks + UpdateForkChoice path triggers the reorg and executes
	// those blocks. This matches what ethereum/tests BlockchainTests with
	// multi-chain layouts (lotsOfLeafs, ChainAtoChainB, ForkStressTest, etc.)
	// expect when Hive imports one block per file.
	firstBlock := chain.Blocks[0]
	tipBlock := chain.TopBlock
	var parentTd, currentHeadTd *uint256.Int
	var currentHeadHash common.Hash
	var currentHeadNumber uint64
	if err := ethereum.ChainDB().View(ctx, func(tx kv.Tx) error {
		if firstBlock.NumberU64() > 0 {
			td, readErr := rawdb.ReadTd(tx, firstBlock.ParentHash(), firstBlock.NumberU64()-1)
			if readErr != nil {
				return fmt.Errorf("read parent TD: %w", readErr)
			}
			parentTd = td
		} else {
			parentTd = new(uint256.Int)
		}
		if hash := rawdb.ReadHeadBlockHash(tx); hash != (common.Hash{}) {
			if num := rawdb.ReadHeaderNumber(tx, hash); num != nil {
				td, readErr := rawdb.ReadTd(tx, hash, *num)
				if readErr != nil {
					return fmt.Errorf("read head TD: %w", readErr)
				}
				currentHeadTd = td
				currentHeadHash = hash
				currentHeadNumber = *num
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// Apply the side-chain path only for PoW imports where both TDs are known.
	// If parent TD is missing (orphan import) or there is no current head yet,
	// fall through to the regular insert path so any error surfaces normally.
	// PoS blocks have difficulty=0 so TD never grows; ShouldReorg's PoW
	// tie-break (shorter chain wins on equal TD) would prevent the head from
	// ever advancing, so PoS imports skip the side-chain branch and rely on
	// UpdateForkChoice as before.
	isPoW := tipBlock.Header().Difficulty.Sign() > 0
	if setHead && isPoW && parentTd != nil && currentHeadTd != nil {
		importedTipTd := new(uint256.Int).Set(parentTd)
		for _, b := range chain.Blocks {
			if _, overflow := importedTipTd.AddOverflow(importedTipTd, &b.Header().Difficulty); overflow {
				return fmt.Errorf("imported tip TD overflows uint256 at block %d", b.NumberU64())
			}
		}
		if !ethash.ShouldReorg(currentHeadTd, currentHeadNumber, currentHeadHash, importedTipTd, tipBlock.NumberU64(), tipBlock.Hash()) {
			// Side chain — write headers/bodies/TDs directly without executing
			// or changing head.
			return ethereum.ChainDB().Update(ctx, func(tx kv.RwTx) error {
				td := new(uint256.Int).Set(parentTd)
				for _, b := range chain.Blocks {
					if _, overflow := td.AddOverflow(td, &b.Header().Difficulty); overflow {
						return fmt.Errorf("side-chain TD overflows uint256 at block %d", b.NumberU64())
					}
					if err := rawdb.WriteHeader(tx, b.Header()); err != nil {
						return fmt.Errorf("write side-chain header: %w", err)
					}
					if err := rawdb.WriteTd(tx, b.Hash(), b.NumberU64(), *td); err != nil {
						return fmt.Errorf("write side-chain TD: %w", err)
					}
					if _, err := rawdb.WriteRawBodyIfNotExists(tx, b.Hash(), b.NumberU64(), b.RawBody()); err != nil {
						return fmt.Errorf("write side-chain body: %w", err)
					}
				}
				return nil
			})
		}
	}

	streamCtx, cancel := context.WithCancel(ethereum.SentryCtx())
	defer cancel()
	stream, err := ethereum.StateDiffClient().StateChanges(streamCtx, &remoteproto.StateChangeRequest{WithStorage: false, WithTransactions: false}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}

	insertedBlocks := map[uint64]struct{}{}
	for _, block := range chain.Blocks {
		insertedBlocks[block.NumberU64()] = struct{}{}
	}

	chainRW := chainreader.NewChainReaderEth1(ethereum.ChainConfig(), ethereum.ExecutionModule(), time.Hour)

	if err := chainRW.InsertBlocksAndWait(ctx, chain.Blocks); err != nil {
		return err
	}

	if !setHead {
		return nil
	}

	tipHash := chain.TopBlock.Hash()
	status, validationErr, lvh, err := chainRW.UpdateForkChoice(ctx, tipHash, tipHash, tipHash)
	if err != nil {
		return err
	}
	// On any non-success status the commit is skipped and no state-diff events
	// will be emitted, so skip the wait below — otherwise stream.Recv() hangs.
	if status != execmodule.ExecutionStatusSuccess {
		blockNum := chain.Blocks[chain.Length()-1].NumberU64()
		if validationErr != nil {
			return fmt.Errorf("fork-choice rejected block %d, status: %s: %s", blockNum, status.String(), *validationErr)
		}
		return fmt.Errorf("fork-choice rejected block %d, status: %s", blockNum, status.String())
	}

	// UpdateForkChoice has an async commit so we need to wait to make sure
	// it is completed before assuming all state changes etc are inserted
	var lastSeenBlock uint64
	for len(insertedBlocks) > 0 {
		req, err := stream.Recv()
		if err != nil {
			if ethereum.SentryCtx().Err() != nil {
				return nil
			}
			if streamCtx.Err() != nil {
				return fmt.Errorf("block insert recv timed out: %d remaining", len(insertedBlocks))
			}
			return fmt.Errorf("block insert recv failed: %w, %d remaining", err, len(insertedBlocks))
		}

		for _, change := range req.ChangeBatch {
			if change.Direction == remoteproto.Direction_UNWIND {
				insertedBlocks = nil
			}
			for lastSeenBlock <= change.BlockHeight {
				delete(insertedBlocks, lastSeenBlock)
				lastSeenBlock++
			}
			if len(insertedBlocks) == 0 {
				break
			}
		}
	}

	return ethereum.ChainDB().Update(ethereum.SentryCtx(), func(tx kv.RwTx) error {
		rawdb.WriteHeadBlockHash(tx, lvh)
		return nil
	})
}
