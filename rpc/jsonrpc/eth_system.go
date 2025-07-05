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

package jsonrpc

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/gasprice"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// BlockNumber implements eth_blockNumber. Returns the block number of most recent block.
func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	blockNum, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(blockNum), nil
}

// Syncing implements eth_syncing. Returns a data object detailing the status of the sync process or false if not syncing.
func (api *APIImpl) Syncing(ctx context.Context) (interface{}, error) {
	reply, err := api.ethBackend.Syncing(ctx)
	if err != nil {
		return false, err
	}
	if !reply.Syncing {
		return false, nil
	}

	// Still sync-ing, gather the block sync stats
	highestBlock := reply.LastNewBlockSeen
	currentBlock := reply.CurrentBlock
	type S struct {
		StageName   string         `json:"stage_name"`
		BlockNumber hexutil.Uint64 `json:"block_number"`
	}
	stagesMap := make([]S, len(reply.Stages))
	for i, stage := range reply.Stages {
		stagesMap[i].StageName = stage.StageName
		stagesMap[i].BlockNumber = hexutil.Uint64(stage.BlockNumber)
	}

	return map[string]interface{}{
		"startingBlock": "0x0", // 0x0 is a placeholder, I do not think it matters what we return here
		"currentBlock":  hexutil.Uint64(currentBlock),
		"highestBlock":  hexutil.Uint64(highestBlock),
		"stages":        stagesMap,
	}, nil
}

// ChainId implements eth_chainId. Returns the current ethereum chainId.
func (api *APIImpl) ChainId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(chainConfig.ChainID.Uint64()), nil
}

// ChainID alias of ChainId - just for convenience
func (api *APIImpl) ChainID(ctx context.Context) (hexutil.Uint64, error) {
	return api.ChainId(ctx)
}

// ProtocolVersion implements eth_protocolVersion. Returns the current ethereum protocol version.
func (api *APIImpl) ProtocolVersion(ctx context.Context) (hexutil.Uint, error) {
	ver, err := api.ethBackend.ProtocolVersion(ctx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint(ver), nil
}

// GasPrice implements eth_gasPrice. Returns the current price per gas in wei.
func (api *APIImpl) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, api.logger.New("app", "gasPriceOracle"))
	tipcap, err := oracle.SuggestTipCap(ctx)
	gasResult := big.NewInt(0)

	gasResult.Set(tipcap)
	if err != nil {
		return nil, err
	}
	if head := rawdb.ReadCurrentHeader(tx); head != nil && head.BaseFee != nil {
		gasResult.Add(tipcap, head.BaseFee)
	}

	return (*hexutil.Big)(gasResult), err
}

// MaxPriorityFeePerGas returns a suggestion for a gas tip cap for dynamic fee transactions.
func (api *APIImpl) MaxPriorityFeePerGas(ctx context.Context) (*hexutil.Big, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, api.logger.New("app", "gasPriceOracle"))
	tipcap, err := oracle.SuggestTipCap(ctx)
	if err != nil {
		return nil, err
	}
	return (*hexutil.Big)(tipcap), err
}

type feeHistoryResult struct {
	OldestBlock      *hexutil.Big     `json:"oldestBlock"`
	Reward           [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee          []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio     []float64        `json:"gasUsedRatio"`
	BlobBaseFee      []*hexutil.Big   `json:"baseFeePerBlobGas,omitempty"`
	BlobGasUsedRatio []float64        `json:"blobGasUsedRatio,omitempty"`
}

func (api *APIImpl) FeeHistory(ctx context.Context, blockCount rpc.DecimalOrHex, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (*feeHistoryResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, api.logger.New("app", "gasPriceOracle"))

	oldest, reward, baseFee, gasUsed, blobBaseFee, blobGasUsedRatio, err := oracle.FeeHistory(ctx, int(blockCount), lastBlock, rewardPercentiles)
	if err != nil {
		return nil, err
	}
	results := &feeHistoryResult{
		OldestBlock:  (*hexutil.Big)(oldest),
		GasUsedRatio: gasUsed,
	}
	if reward != nil {
		results.Reward = make([][]*hexutil.Big, len(reward))
		for i, w := range reward {
			results.Reward[i] = make([]*hexutil.Big, len(w))
			for j, v := range w {
				results.Reward[i][j] = (*hexutil.Big)(v)
			}
		}
	}
	if baseFee != nil {
		results.BaseFee = make([]*hexutil.Big, len(baseFee))
		for i, v := range baseFee {
			results.BaseFee[i] = (*hexutil.Big)(v)
		}
	}
	if blobBaseFee != nil {
		results.BlobBaseFee = make([]*hexutil.Big, len(blobBaseFee))
		for i, v := range blobBaseFee {
			results.BlobBaseFee[i] = (*hexutil.Big)(v)
		}
	}
	if blobGasUsedRatio != nil {
		results.BlobGasUsedRatio = blobGasUsedRatio
	}
	return results, nil
}

// BlobBaseFee returns the base fee for blob gas at the current head.
func (api *APIImpl) BlobBaseFee(ctx context.Context) (*hexutil.Big, error) {
	// read current header
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil || header.BlobGasUsed == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	config, err := api.BaseAPI.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	nextBlockTime := header.Time + config.SecondsPerSlot()
	ret256, err := misc.GetBlobGasPrice(config, misc.CalcExcessBlobGas(config, header, nextBlockTime), nextBlockTime)
	if err != nil {
		return nil, err
	}
	return (*hexutil.Big)(ret256.ToBig()), nil
}

// BaseFee returns the base fee at the current head.
func (api *APIImpl) BaseFee(ctx context.Context) (*hexutil.Big, error) {
	// read current header
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	config, err := api.BaseAPI.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	if !config.IsLondon(header.Number.Uint64() + 1) {
		return (*hexutil.Big)(common.Big0), nil
	}
	return (*hexutil.Big)(misc.CalcBaseFee(config, header)), nil
}

// EthHardForkConfig represents config of a hard-fork
type EthHardForkConfig struct {
	ActivationTime  hexutil.Uint              `json:"activationTime"`
	BlobSchedule    params.BlobConfig         `json:"blobSchedule"`
	ChainId         hexutil.Uint              `json:"chainId"`
	Precompiles     map[common.Address]string `json:"precompiles"`
	SystemContracts map[string]common.Address `json:"systemContracts"`
}

// EthConfigResp is the response type of eth_config
type EthConfigResp struct {
	Current     *EthHardForkConfig `json:"current"`
	CurrentHash string             `json:"currentHash"`
	Next        *EthHardForkConfig `json:"next"`
	NextHash    *string            `json:"nextHash"`
}

// SystemContractsMap maps system contract addresses to names expected in eth_config
var SystemContractsMap = map[common.Address]string{
	params.BeaconRootsAddress:          "BEACON_ROOTS_ADDRESS",
	params.ConsolidationRequestAddress: "CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS",
	params.HistoryStorageAddress:       "HISTORY_STORAGE_ADDRESS",
	params.WithdrawalRequestAddress:    "WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS",
}

// PrecompileNamesMap maps object names used in the codebase to expected names in the output of eth_config
// DO NOT RENAME THE UNDERLYING PRECOMPILED CONTRACTS WITHOUT CHANGING THIS OR VICE-VERSA
var PrecompileNamesMap = map[string]string{
	"ecrecover":              "ECREC",
	"sha256hash":             "SHA256",
	"ripemd160hash":          "RIPEMD160",
	"dataCopy":               "ID",
	"bigModExp":              "MODEXP",
	"bn256AddIstanbul":       "BN256_ADD",
	"bn256ScalarMulIstanbul": "BN256_MUL",
	"bn256PairingIstanbul":   "BN256_PAIRING",
	"blake2F":                "BLAKE2F",
	"pointEvaluation":        "KZG_POINT_EVALUATION",
	"bls12381G1Add":          "BLS12_G1ADD",
	"bls12381G1MultiExp":     "BLS12_G1MSM",
	"bls12381G2Add":          "BLS12_G2ADD",
	"bls12381G2MultiExp":     "BLS12_G2MSM",
	"bls12381Pairing":        "BLS12_PAIRING_CHECK",
	"bls12381MapFpToG1":      "BLS12_MAP_FP_TO_G1",
	"bls12381MapFp2ToG2":     "BLS12_MAP_FP2_TO_G2",
}

// Config returns the HardFork config for current and upcoming forks:
// assuming linear fork progression and ethereum-like schedule
func (api *APIImpl) Config(ctx context.Context) (*EthConfigResp, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	config, err := api.BaseAPI.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	tt := uint64(time.Now().UnixMilli()) / 1000

	ret := &EthConfigResp{
		Current: &EthHardForkConfig{},
	}
	switch {
	case config.IsOsaka(tt):
		fillOsakaForkConfig(ret.Current, config)
		ret.Next = nil
	case config.IsPrague(tt):
		fillPragueForkConfig(ret.Current, config)
		if config.OsakaTime != nil {
			ret.Next = &EthHardForkConfig{}
			fillOsakaForkConfig(ret.Next, config)
		}
	case config.IsCancun(tt):
		fillCancunForkConfig(ret.Current, config)
		if config.PragueTime != nil {
			ret.Next = &EthHardForkConfig{}
			fillPragueForkConfig(ret.Next, config)
		}
	default:
	}
	if ret.Current != nil {
		ret.CurrentHash = checkSumConfig(ret.Current)
	}
	if ret.Next != nil {
		cs := checkSumConfig(ret.Next)
		ret.NextHash = &cs
	}
	return ret, nil
}

func fillPragueForkConfig(ret *EthHardForkConfig, config *chain.Config) {
	ret.ActivationTime = hexutil.Uint(config.PragueTime.Uint64())
	ret.BlobSchedule = *config.GetBlobConfig(config.PragueTime.Uint64())
	ret.ChainId = hexutil.Uint(config.ChainID.Uint64())
	ret.SystemContracts = makeSystemContractsConfigMap([]common.Address{
		params.BeaconRootsAddress,
		params.ConsolidationRequestAddress,
		params.HistoryStorageAddress,
		params.WithdrawalRequestAddress,
	}, &config.DepositContract)

	ret.Precompiles = make(map[common.Address]string)
	for k, v := range vm.PrecompiledContractsPrague {
		ret.Precompiles[k] = PrecompileNamesMap[reflect.TypeOf(v).Elem().Name()]
	}
}

func fillOsakaForkConfig(ret *EthHardForkConfig, config *chain.Config) {
	ret.ActivationTime = hexutil.Uint(config.OsakaTime.Uint64())
	ret.BlobSchedule = *config.GetBlobConfig(config.OsakaTime.Uint64())
	ret.ChainId = hexutil.Uint(config.ChainID.Uint64())
	ret.SystemContracts = makeSystemContractsConfigMap([]common.Address{
		params.BeaconRootsAddress,
		params.ConsolidationRequestAddress,
		params.HistoryStorageAddress,
		params.WithdrawalRequestAddress,
	}, &config.DepositContract)

	ret.Precompiles = make(map[common.Address]string)
	for k, v := range vm.PrecompiledContractsOsaka {
		ret.Precompiles[k] = PrecompileNamesMap[reflect.TypeOf(v).Elem().Name()]
	}
}

func fillCancunForkConfig(ret *EthHardForkConfig, config *chain.Config) {
	ret.ActivationTime = hexutil.Uint(config.CancunTime.Uint64())
	ret.BlobSchedule = *config.GetBlobConfig(config.CancunTime.Uint64())
	ret.ChainId = hexutil.Uint(config.ChainID.Uint64())
	ret.SystemContracts = makeSystemContractsConfigMap(
		[]common.Address{},
		&config.DepositContract,
	)
	ret.Precompiles = make(map[common.Address]string)
	for k, v := range vm.PrecompiledContractsCancun {
		ret.Precompiles[k] = PrecompileNamesMap[reflect.TypeOf(v).Elem().Name()]
	}
}

func checkSumConfig(ehfc *EthHardForkConfig) string {
	ms, err := json.Marshal(ehfc)
	if err != nil {
		log.Error("checkSumConfig: Error occurred while json Marshalling config", "err", err)
		return ""
	}
	cs := uint64(crc32.ChecksumIEEE(ms))
	return strconv.FormatUint(cs, 16)
}

func makeSystemContractsConfigMap(contracts []common.Address, depositContract *common.Address) map[string]common.Address {
	ret := make(map[string]common.Address)
	for _, c := range contracts {
		ret[SystemContractsMap[c]] = c
	}
	if depositContract != nil {
		ret["DEPOSIT_CONTRACT_ADDRESS"] = *depositContract
	}
	return ret
}

type GasPriceOracleBackend struct {
	tx      kv.TemporalTx
	baseApi *BaseAPI
}

func NewGasPriceOracleBackend(tx kv.TemporalTx, baseApi *BaseAPI) *GasPriceOracleBackend {
	return &GasPriceOracleBackend{tx: tx, baseApi: baseApi}
}

func (b *GasPriceOracleBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	header, err := b.baseApi.headerByRPCNumber(ctx, number, b.tx)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	return header, nil
}
func (b *GasPriceOracleBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	return b.baseApi.blockByRPCNumber(ctx, number, b.tx)
}
func (b *GasPriceOracleBackend) ChainConfig() *chain.Config {
	cc, _ := b.baseApi.chainConfig(context.Background(), b.tx)
	return cc
}
func (b *GasPriceOracleBackend) GetReceipts(ctx context.Context, block *types.Block) (types.Receipts, error) {
	return b.baseApi.getReceipts(ctx, b.tx, block)
}
func (b *GasPriceOracleBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return nil, nil
}

func (b *GasPriceOracleBackend) GetReceiptsGasUsed(ctx context.Context, block *types.Block) (types.Receipts, error) {
	return b.baseApi.getReceiptsGasUsed(ctx, b.tx, block)
}
