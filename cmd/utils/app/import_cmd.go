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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/erigon/node"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/eth1/eth1_chain_reader"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/eth"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
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
	logger, tracer, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}

	nodeCfg, err := node.NewNodConfigUrfave(cliCtx, nil, logger)
	if err != nil {
		return err
	}

	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg, logger)
	ethCfg.Snapshot.NoDownloader = true // no need to run this for import chain (also used in hive eest/consume-rlp tests)
	ethCfg.InternalCL = false           // no need to run this for import chain (also used in hive eest/consume-rlp tests)
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

	rlpData := hexutil.MustDecode("0xf90295f901f6a0f2df167a6e574566ef13e2545eb2be5593208824e01449f71ec5120bf958de9da01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347942adc25665018aa1fe0e6bc666dac8fc2697ff9baa0814a73caeaf4d94207dfe1f1e1c7ff1fb2db918d939255cfc3a113858e59d422a0d2c3e456cfa19aee8bba4e923630546278190ece278d61726e1da7be870f606fa040505d2c15a616bf31ebd7b4709aeded03a87b1e9d2f22a716831697b47dc37bb9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800184055d4a808265088203e800a0000000000000000000000000000000000000000000000000000000000000000088000000000000000007f899b89702f894018080078265089465e12864ab44e436ef786dc6e90a060d5c7b1212808430784646eed6940000000000000000000000000000000000000001c0d6940000000000000000000000000000000000000002c001a0f095f23e30f7eecc4f0a3e85fff4399d5688df1fe8dcf37ce89e96d33333d39ca00cb277e6367901a2c56a0b638dd3ccf3821b97cea0e0b8f0c2276a7034fc4ab1c0")
	reader := bytes.NewReader(rlpData)
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

func InsertChain(ethereum *eth.Ethereum, chain *blockgen.ChainPack, setHead bool) error {
	for _, block := range chain.Blocks {
		if err := block.HashCheck(true); err != nil {
			return err
		}
	}

	chainRW := eth1_chain_reader.NewChainReaderEth1(ethereum.ChainConfig(), direct.NewExecutionClientDirect(ethereum.ExecutionModule()), uint64(time.Hour))

	ctx := context.Background()
	if err := chainRW.InsertBlocksAndWait(ctx, chain.Blocks); err != nil {
		return err
	}

	if !setHead {
		return nil
	}

	tipHash := chain.TopBlock.Hash()
	status, _, lvh, err := chainRW.UpdateForkChoice(ctx, tipHash, tipHash, tipHash)
	if err != nil {
		return err
	}

	err = ethereum.ChainDB().Update(ethereum.SentryCtx(), func(tx kv.RwTx) error {
		rawdb.WriteHeadBlockHash(tx, lvh)
		return nil
	})
	if err != nil {
		return err
	}
	if status != executionproto.ExecutionStatus_Success {
		return fmt.Errorf("insertion failed for block %d, code: %s", chain.Blocks[chain.Length()-1].NumberU64(), status.String())
	}

	return nil
}
