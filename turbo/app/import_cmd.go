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

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/debug"
	turboNode "github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/erigon/turbo/stages"
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

	logger, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}

	nodeCfg := turboNode.NewNodConfigUrfave(cliCtx, logger)
	ethCfg := turboNode.NewEthConfigUrfave(cliCtx, nodeCfg, logger)

	stack := makeConfigNode(nodeCfg, logger)
	defer stack.Close()

	ethereum, err := eth.New(stack, ethCfg, logger)
	if err != nil {
		return err
	}
	err = ethereum.Init(stack, ethCfg)
	if err != nil {
		return err
	}

	if err := ImportChain(ethereum, ethereum.ChainDB(), cliCtx.Args().First(), logger); err != nil {
		return err
	}

	return nil
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
			return fmt.Errorf("interrupted")
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
			return fmt.Errorf("interrupted")
		}

		br, _ := ethereum.BlockIO()
		missing := missingBlocks(chainDB, blocks[:i], br)
		if len(missing) == 0 {
			logger.Info("Skipping batch as all blocks present", "batch", batch, "first", blocks[0].Hash(), "last", blocks[i-1].Hash())
			continue
		}

		// RLP decoding worked, try to insert into chain:
		missingChain := &core.ChainPack{
			Blocks:   missing,
			TopBlock: missing[len(missing)-1],
		}

		if err := InsertChain(ethereum, missingChain, logger); err != nil {
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

func InsertChain(ethereum *eth.Ethereum, chain *core.ChainPack, logger log.Logger) error {
	sentryControlServer := ethereum.SentryControlServer()
	initialCycle := false

	for _, b := range chain.Blocks {
		sentryControlServer.Hd.AddMinedHeader(b.Header())
		sentryControlServer.Bd.AddToPrefetch(b.Header(), b.RawBody())
	}

	sentryControlServer.Hd.MarkAllVerified()
	blockReader, _ := ethereum.BlockIO()

	hook := stages.NewHook(ethereum.SentryCtx(), ethereum.Notifications(), ethereum.StagedSync(), blockReader, ethereum.ChainConfig(), logger, sentryControlServer.UpdateHead)
	err := stages.StageLoopIteration(ethereum.SentryCtx(), ethereum.ChainDB(), nil, ethereum.StagedSync(), initialCycle, logger, blockReader, hook)
	if err != nil {
		return err
	}

	return nil
}
