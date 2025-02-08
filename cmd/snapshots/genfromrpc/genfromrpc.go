package genfromrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/urfave/cli/v2"
)

var RpcAddr = cli.BoolFlag{
	Name:     "torrents",
	Usage:    `Include torrent files in copy`,
	Required: false,
}

var Command = cli.Command{
	Action: func(cliCtx *cli.Context) error {
		return torrents(cliCtx, "list")
	},
	Name:  "torrent",
	Usage: "torrent utilities",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&utils.ChainFlag,
		&RpcAddr,
	},
	Description: ``,
}

type BlockJson struct {
	types.Header

	Uncles       []*types.Header          `json:"uncles"`
	Withdrawals  types.Withdrawals        `json:"withdrawals"`
	Transactions []map[string]interface{} `json:"transactions"`
}

func unMarshalTransactions(raw []map[string]interface{}) (types.Transactions, error) {
	var txs types.Transactions
	for _, tx := range rawTx {
		tx := types.Transaction{}
		status := tx["status"].(string)
		switch status {
		case "0x0": // legacy
		case "0x1": // access list
		case "0x2": // eip1559
		case "0x3": // eip4844
		case "0x4": // eip7702

}


// Function to fetch a block by number
func getBlockByNumber(client *rpc.Client, blockNumber *big.Int) (*types.Block, error) {
	var block types.Block
	err := client.CallContext(context.Background(), &block, "eth_getBlockByNumber", fmt.Sprintf("0x%x", blockNumber), true)
	if err != nil {
		return nil, err
	}
	return &block, nil
}

func torrents(cliCtx *cli.Context, command string) error {
	jsonRpcAddr := cliCtx.String(RpcAddr.Name)
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(log.LvlInfo), log.StderrHandler))
	// Connect to Arbitrum One RPC
	client, err := rpc.Dial(jsonRpcAddr, log.Root())
	if err != nil {
		log.Warn("Error connecting to RPC", "err", err)
		return err
	}

	// Query latest block number
	var latestBlockHex string
	err = client.CallContext(context.Background(), &latestBlockHex, "eth_blockNumber")
	if err != nil {
		log.Warn("Error fetching latest block number", "err", err)
		return err
	}

	// Convert block number from hex to integer
	latestBlock := new(big.Int)
	latestBlock.SetString(latestBlockHex[2:], 16)

	fmt.Printf("Latest Block: %d\n", latestBlock)

	// Loop through last 10 blocks
	for i := uint64(0); i < latestBlock.Uint64(); i++ {
		blockNumber := new(big.Int).Sub(latestBlock, big.NewInt(int64(i)))
		block, err := getBlockByNumber(client, blockNumber)
		if err != nil {
			log.Error("Error fetching block", "blockNumber", blockNumber, "err", err)
			return err
		}

		// Print block details
		blockJSON, _ := json.MarshalIndent(block, "", "  ")
		fmt.Println(string(blockJSON))
	}
}
