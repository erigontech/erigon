package apis

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func RegisterRetraceAPI(router *gin.RouterGroup, e *Env) error {
	router.GET(":chain/:number", e.GetWritesReads)
	return nil
}

func (e *Env) GetWritesReads(c *gin.Context) {
	results, err := Retrace(c.Param("number"), c.Param("chain"), e.KV, e.DB)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}

type AccountWritesReads struct {
	Reads  []string `json:"reads"`
	Writes []string `json:"writes"`
}
type StorageWriteReads struct {
	Reads  map[string][]string
	Writes map[string][]string
}
type RetraceResponse struct {
	Storage StorageWriteReads  `json:"storage"`
	Account AccountWritesReads `json:"accounts"`
}

func Retrace(blockNumber, chain string, kv ethdb.KV, db ethdb.Getter) (RetraceResponse, error) {
	chainConfig, err := ReadChainConfig(kv, chain)
	if err != nil {
		return RetraceResponse{}, err
	}
	noOpWriter := state.NewNoopWriter()
	bn, err := strconv.Atoi(blockNumber)
	if err != nil {
		return RetraceResponse{}, err
	}
	block, err := rawdb.ReadBlockByNumber(db, uint64(bn))
	if err != nil {
		return RetraceResponse{}, err
	}
	chainCtx := NewRemoteContext(kv, db)
	writer := state.NewChangeSetWriterPlain(uint64(bn - 1))
	reader := NewRemoteReader(kv, uint64(bn))
	intraBlockState := state.New(reader)

	if err = runBlock(intraBlockState, noOpWriter, writer, chainConfig, chainCtx, block); err != nil {
		return RetraceResponse{}, err
	}

	var output RetraceResponse
	accountChanges, _ := writer.GetAccountChanges()
	if err != nil {
		return RetraceResponse{}, err
	}
	for _, ch := range accountChanges.Changes {
		output.Account.Writes = append(output.Account.Writes, common.Bytes2Hex(ch.Key))
	}
	for _, ch := range reader.GetAccountReads() {
		output.Account.Reads = append(output.Account.Reads, common.Bytes2Hex(ch))
	}

	storageChanges, _ := writer.GetStorageChanges()
	output.Storage.Writes = make(map[string][]string)
	for _, ch := range storageChanges.Changes {
		addrKey := common.Bytes2Hex(ch.Key[:common.AddressLength])
		l := output.Storage.Writes[addrKey]
		l = append(l, common.Bytes2Hex(ch.Key[common.AddressLength+common.IncarnationLength:]))
		output.Storage.Writes[addrKey] = l
	}
	output.Storage.Reads = make(map[string][]string)
	for _, key := range reader.GetStorageReads() {
		addrKey := common.Bytes2Hex(key[:common.AddressLength])
		l := output.Storage.Reads[addrKey]
		l = append(l, common.Bytes2Hex(key[common.AddressLength+common.IncarnationLength:]))
		output.Storage.Reads[addrKey] = l
	}
	return output, nil
}

func runBlock(ibs *state.IntraBlockState, txnWriter state.StateWriter, blockWriter state.StateWriter,
	chainConfig *params.ChainConfig, bcb core.ChainContext, block *types.Block,
) error {
	header := block.Header()
	vmConfig := vm.Config{}
	engine := ethash.NewFullFaker()
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	for _, tx := range block.Transactions() {
		receipt, err := core.ApplyTransaction(chainConfig, bcb, nil, gp, ibs, txnWriter, header, tx, usedGas, vmConfig)
		if err != nil {
			return fmt.Errorf("tx %x failed: %v", tx.Hash(), err)
		}
		receipts = append(receipts, receipt)
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	if _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts); err != nil {
		return fmt.Errorf("finalize of block %d failed: %v", block.NumberU64(), err)
	}

	ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
	if err := ibs.CommitBlock(ctx, blockWriter); err != nil {
		return fmt.Errorf("committing block %d failed: %v", block.NumberU64(), err)
	}
	return nil
}

// ReadChainConfig retrieves the consensus settings based on the given genesis hash.
func ReadChainConfig(db ethdb.KV, chain string) (*params.ChainConfig, error) {
	var k []byte
	var data []byte
	switch chain {
	case "mainnet":
		k = params.MainnetGenesisHash[:]
	case "testnet":
		k = params.RopstenGenesisHash[:]
	case "rinkeby":
		k = params.RinkebyGenesisHash[:]
	case "goerli":
		k = params.GoerliGenesisHash[:]
	}
	if err := db.View(context.Background(), func(tx ethdb.Tx) error {
		d, err := tx.Get(dbutils.ConfigPrefix, k)
		if err != nil {
			return err
		}
		data = common.CopyBytes(d)
		return nil
	}); err != nil {
		return nil, err
	}
	var config params.ChainConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return &config, nil
}
