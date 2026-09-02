// Copyright 2021 The Erigon Authors
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

package chain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/generics"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Config is the core config which determines the blockchain settings.
//
// Config is stored in the database on a per block basis. This means
// that any network, identified by its genesis block, can have its own
// set of configuration options.
//
// Config holds a sync.Once, so it must never be copied by assignment. Use Copy, which
// leaves that Once zeroed and shares every pointer, map and slice with the source.
type Config struct {
	ChainName string       `json:"chainName"` // chain name, eg: mainnet, sepolia, gnosis
	ChainID   *uint256.Int `json:"chainId"`   // chainId identifies the current chain and is used for replay protection

	Rules RulesName `json:"consensus,omitempty"` // aura or ethash

	// *Block fields activate the corresponding hard fork at a certain block number,
	// while *Time fields do so based on the block's time stamp.
	// nil means that the hard-fork is not scheduled,
	// while 0 means that it's already activated from genesis.

	// ETH mainnet upgrades
	// See https://github.com/ethereum/execution-specs/blob/master/network-upgrades/mainnet-upgrades
	HomesteadBlock        *uint64 `json:"homesteadBlock,omitempty"`
	DAOForkBlock          *uint64 `json:"daoForkBlock,omitempty"`
	TangerineWhistleBlock *uint64 `json:"eip150Block,omitempty"`
	SpuriousDragonBlock   *uint64 `json:"eip155Block,omitempty"`
	ByzantiumBlock        *uint64 `json:"byzantiumBlock,omitempty"`
	ConstantinopleBlock   *uint64 `json:"constantinopleBlock,omitempty"`
	PetersburgBlock       *uint64 `json:"petersburgBlock,omitempty"`
	IstanbulBlock         *uint64 `json:"istanbulBlock,omitempty"`
	MuirGlacierBlock      *uint64 `json:"muirGlacierBlock,omitempty"`
	BerlinBlock           *uint64 `json:"berlinBlock,omitempty"`
	LondonBlock           *uint64 `json:"londonBlock,omitempty"`
	ArrowGlacierBlock     *uint64 `json:"arrowGlacierBlock,omitempty"`
	GrayGlacierBlock      *uint64 `json:"grayGlacierBlock,omitempty"`

	// EIP-3675: Upgrade consensus to Proof-of-Stake (a.k.a. "Paris", "The Merge")
	TerminalTotalDifficulty       *uint256.Int `json:"terminalTotalDifficulty,omitempty"`       // The merge happens when terminal total difficulty is reached
	TerminalTotalDifficultyPassed bool         `json:"terminalTotalDifficultyPassed,omitempty"` // Disable PoW sync for networks that have already passed through the Merge
	MergeNetsplitBlock            *uint64      `json:"mergeNetsplitBlock,omitempty"`            // Virtual fork after The Merge to use as a network splitter; see FORK_NEXT_VALUE in EIP-3675
	MergeHeight                   *uint64      `json:"mergeBlock,omitempty"`                    // The Merge block number

	// Mainnet fork scheduling switched from block numbers to timestamps after The Merge
	ShanghaiTime  *uint64 `json:"shanghaiTime,omitempty"`
	CancunTime    *uint64 `json:"cancunTime,omitempty"`
	PragueTime    *uint64 `json:"pragueTime,omitempty"`
	OsakaTime     *uint64 `json:"osakaTime,omitempty"`
	AmsterdamTime *uint64 `json:"amsterdamTime,omitempty"`

	// Optional EIP-4844 parameters (see also EIP-7691, EIP-7840, EIP-7892)
	MinBlobGasPrice       *uint64                       `json:"minBlobGasPrice,omitempty"`
	BlobSchedule          map[string]*params.BlobConfig `json:"blobSchedule,omitempty"`
	Bpo1Time              *uint64                       `json:"bpo1Time,omitempty"`
	Bpo2Time              *uint64                       `json:"bpo2Time,omitempty"`
	Bpo3Time              *uint64                       `json:"bpo3Time,omitempty"`
	Bpo4Time              *uint64                       `json:"bpo4Time,omitempty"`
	Bpo5Time              *uint64                       `json:"bpo5Time,omitempty"`
	parseBlobScheduleOnce sync.Once                     `copier:"-"`
	parsedBlobSchedule    map[uint64]*params.BlobConfig

	// Balancer fork (Gnosis Chain). See https://hackmd.io/@filoozom/rycoQITlWl
	BalancerTime            *uint64                          `json:"balancerTime,omitempty"`
	BalancerRewriteBytecode map[common.Address]hexutil.Bytes `json:"balancerRewriteBytecode,omitempty"`

	// (Optional) governance contract where EIP-1559 fees will be sent to, which otherwise would be burnt since the London fork.
	// A key corresponds to the block number, starting from which the fees are sent to the address (map value).
	// Starting from Prague, EIP-4844 fees might be collected as well:
	// see https://github.com/gnosischain/specs/blob/master/network-upgrades/pectra.md#eip-4844-pectra.
	BurntContract map[string]common.Address `json:"burntContract,omitempty"`

	// (Optional) deposit contract of PoS chains
	// See also EIP-6110: Supply validator deposits on chain
	DepositContract common.Address `json:"depositContractAddress,omitempty"`

	// (Optional) EIP-7002: Execution layer triggerable withdrawals
	WithdrawalRequestContract *common.Address `json:"withdrawalRequestContractAddress,omitempty"`

	// (Optional) EIP-7251: Increase the MAX_EFFECTIVE_BALANCE
	ConsolidationRequestContract *common.Address `json:"consolidationRequestContractAddress,omitempty"`

	// (Optional) EIP-8282: The Builder Deposit Addresses
	BuilderDepositContract *common.Address `json:"builderDepositContractAddress,omitempty"`

	// (Optional) EIP-8282: The Builder Exit Addresses
	BuilderExitContract *common.Address `json:"builderExitContractAddress,omitempty"`

	DefaultBlockGasLimit *uint64 `json:"defaultBlockGasLimit,omitempty"`

	// Various rules engines
	Ethash *EthashConfig `json:"ethash,omitempty"`
	Aura   *AuRaConfig   `json:"aura,omitempty"`

	// L2 carries opaque L2-chain-specific config. L2JSON is decoded from the
	// chainspec JSON verbatim; the registering L2 package unmarshals it into
	// L2 at spec-registration time.
	L2     L2Config        `json:"-"`
	L2JSON json.RawMessage `json:"l2,omitempty"`

	// DisabledEIPs lists EIPs that are disabled for this chain, even when
	// their parent fork is active. Used for devnets where the reference
	// client doesn't yet implement certain EIPs (e.g. [7708, 7778, 7928]).
	DisabledEIPs []int `json:"disabledEIPs,omitempty"`

	// Account Abstraction
	AllowAA bool
}

// IsEIPEnabled reports whether the given EIP is active at the given block time:
// its parent fork is active AND it is not listed in DisabledEIPs. This is the
// complete gate — call sites must not add a separate fork check.
func (c *Config) IsEIPEnabled(eip int, time uint64) bool {
	if slices.Contains(c.DisabledEIPs, eip) {
		return false
	}
	switch eip {
	case 7708, 7928:
		return c.IsAmsterdam(time)
	default:
		panic(fmt.Sprintf("IsEIPEnabled: EIP %d is not mapped to a fork", eip))
	}
}

// IsL2 returns whether this chain config carries L2-chain-specific config,
// either already resolved (L2) or still opaque (L2JSON).
func (c *Config) IsL2() bool {
	return c != nil && (c.L2 != nil || (len(c.L2JSON) > 0 && !bytes.Equal(c.L2JSON, jsonNull)))
}

var jsonNull = []byte("null")

var (
	TestChainAuraConfig = &Config{
		ChainID:               uint256.NewInt(1),
		Rules:                 AuRaRules,
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		MuirGlacierBlock:      common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		LondonBlock:           common.NewUint64(0),
		Aura:                  &AuRaConfig{},
		DisabledEIPs:          []int{170},
	}

	TestChainBerlinConfig = &Config{
		ChainID:               uint256.NewInt(1337),
		Rules:                 EtHashRules,
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		MuirGlacierBlock:      common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		Ethash:                new(EthashConfig),
	}

	TestChainOsakaConfig = &Config{
		ChainID:                       uint256.NewInt(1337),
		Rules:                         EtHashRules,
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		MuirGlacierBlock:              common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		ArrowGlacierBlock:             common.NewUint64(0),
		GrayGlacierBlock:              common.NewUint64(0),
		TerminalTotalDifficulty:       uint256.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		CancunTime:                    common.NewUint64(0),
		PragueTime:                    common.NewUint64(0),
		OsakaTime:                     common.NewUint64(0),
		DepositContract:               common.HexToAddress("0x00000000219ab540356cBB839Cbe05303d7705Fa"),
		Ethash:                        new(EthashConfig),
	}

	// AllProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the main net protocol.
	AllProtocolChanges = &Config{
		ChainID:                       uint256.NewInt(1337),
		Rules:                         EtHashRules,
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		MuirGlacierBlock:              common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		ArrowGlacierBlock:             common.NewUint64(0),
		GrayGlacierBlock:              common.NewUint64(0),
		TerminalTotalDifficulty:       uint256.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		CancunTime:                    common.NewUint64(0),
		PragueTime:                    common.NewUint64(0),
		OsakaTime:                     common.NewUint64(0),
		AmsterdamTime:                 common.NewUint64(0),
		DepositContract:               common.HexToAddress("0x00000000219ab540356cBB839Cbe05303d7705Fa"),
		Ethash:                        new(EthashConfig),
	}
)

// L2Config is the resolved implementation of an L2 stack's chain-specific
// config, registered by the L2 package at spec-registration time.
type L2Config interface {
	// Name returns the short identifier of the L2 stack (e.g. used to select
	// a registered rules engine).
	Name() string

	// ResolveRules lets an L2 stack finalize the per-block Rules after the
	// standard fork resolution: set L2Version and flip any EVM-fork booleans
	// that the L2 gates on its own version ladder instead of L1 time/number.
	ResolveRules(l2Version, blockNum, blockTime uint64, rules *Rules)
}

func timestampToTime(unixSec uint64) *time.Time {
	t := time.Unix(int64(unixSec), 0).UTC()
	return &t
}

func (c *Config) String() string {
	engine := c.getEngine()

	var b strings.Builder
	fmt.Fprintf(&b, "{ChainID: %v, Terminal Total Difficulty: %v", c.ChainID, c.TerminalTotalDifficulty)
	if c.ShanghaiTime != nil {
		fmt.Fprintf(&b, ", Shapella: %v", timestampToTime(*c.ShanghaiTime))
	}
	if c.CancunTime != nil {
		fmt.Fprintf(&b, ", Dencun: %v", timestampToTime(*c.CancunTime))
	}
	if c.PragueTime != nil {
		fmt.Fprintf(&b, ", Pectra: %v", timestampToTime(*c.PragueTime))
	}
	if c.OsakaTime != nil {
		fmt.Fprintf(&b, ", Fusaka: %v", timestampToTime(*c.OsakaTime))
	}
	if c.Bpo1Time != nil {
		fmt.Fprintf(&b, ", BPO1: %v", timestampToTime(*c.Bpo1Time))
	}
	if c.Bpo2Time != nil {
		fmt.Fprintf(&b, ", BPO2: %v", timestampToTime(*c.Bpo2Time))
	}
	if c.Bpo3Time != nil {
		fmt.Fprintf(&b, ", BPO3: %v", timestampToTime(*c.Bpo3Time))
	}
	if c.Bpo4Time != nil {
		fmt.Fprintf(&b, ", BPO4: %v", timestampToTime(*c.Bpo4Time))
	}
	if c.Bpo5Time != nil {
		fmt.Fprintf(&b, ", BPO5: %v", timestampToTime(*c.Bpo5Time))
	}
	if c.BalancerTime != nil {
		fmt.Fprintf(&b, ", Balancer: %v", timestampToTime(*c.BalancerTime))
	}
	if c.AmsterdamTime != nil {
		fmt.Fprintf(&b, ", Glamsterdam: %v", timestampToTime(*c.AmsterdamTime))
	}
	fmt.Fprintf(&b, ", Engine: %v}", engine)
	return b.String()
}

func (c *Config) getEngine() string {
	switch {
	case c.Ethash != nil:
		return c.Ethash.String()
	case c.Aura != nil:
		return c.Aura.String()
	default:
		return "unknown"
	}
}

// IsHomestead returns whether num is either equal to the homestead block or greater.
func (c *Config) IsHomestead(num uint64) bool {
	return isForked(c.HomesteadBlock, num)
}

// IsDAOFork returns whether num is either equal to the DAO fork block or greater.
func (c *Config) IsDAOFork(num uint64) bool {
	return isForked(c.DAOForkBlock, num)
}

// IsTangerineWhistle returns whether num is either equal to the Tangerine Whistle (EIP150) fork block or greater.
func (c *Config) IsTangerineWhistle(num uint64) bool {
	return isForked(c.TangerineWhistleBlock, num)
}

// IsSpuriousDragon returns whether num is either equal to the Spurious Dragon fork block or greater.
func (c *Config) IsSpuriousDragon(num uint64) bool {
	return isForked(c.SpuriousDragonBlock, num)
}

// IsEIP161Enabled reports whether EIP-161 empty-account clearing applies at num:
// Spurious Dragon is active and EIP-161 has not been disabled.
func (c *Config) IsEIP161Enabled(num uint64) bool {
	return c.IsSpuriousDragon(num) && !slices.Contains(c.DisabledEIPs, 161)
}

// IsByzantium returns whether num is either equal to the Byzantium fork block or greater.
func (c *Config) IsByzantium(num uint64) bool {
	return isForked(c.ByzantiumBlock, num)
}

// IsConstantinople returns whether num is either equal to the Constantinople fork block or greater.
func (c *Config) IsConstantinople(num uint64) bool {
	return isForked(c.ConstantinopleBlock, num)
}

// IsMuirGlacier returns whether num is either equal to the Muir Glacier (EIP-2384) fork block or greater.
func (c *Config) IsMuirGlacier(num uint64) bool {
	return isForked(c.MuirGlacierBlock, num)
}

// IsPetersburg returns whether num is either equal to the Petersburg fork block or greater.
func (c *Config) IsPetersburg(num uint64) bool {
	return isForked(c.PetersburgBlock, num)
}

// IsIstanbul returns whether num is either equal to the Istanbul fork block or greater.
func (c *Config) IsIstanbul(num uint64) bool {
	return isForked(c.IstanbulBlock, num)
}

// IsBerlin returns whether num is either equal to the Berlin fork block or greater.
func (c *Config) IsBerlin(num uint64) bool {
	return isForked(c.BerlinBlock, num)
}

// IsLondon returns whether num is either equal to the London fork block or greater.
func (c *Config) IsLondon(num uint64) bool {
	return isForked(c.LondonBlock, num)
}

// IsArrowGlacier returns whether num is either equal to the Arrow Glacier (EIP-4345) fork block or greater.
func (c *Config) IsArrowGlacier(num uint64) bool {
	return isForked(c.ArrowGlacierBlock, num)
}

// IsGrayGlacier returns whether num is either equal to the Gray Glacier (EIP-5133) fork block or greater.
func (c *Config) IsGrayGlacier(num uint64) bool {
	return isForked(c.GrayGlacierBlock, num)
}

// IsShanghai returns whether time is either equal to the Shanghai fork time or greater.
func (c *Config) IsShanghai(time uint64) bool {
	return isForked(c.ShanghaiTime, time)
}

// IsCancun returns whether time is either equal to the Cancun fork time or greater.
func (c *Config) IsCancun(time uint64) bool {
	return isForked(c.CancunTime, time)
}

// IsAmsterdam returns whether time is either equal to the Amsterdam fork time or greater.
func (c *Config) IsAmsterdam(time uint64) bool {
	return isForked(c.AmsterdamTime, time)
}

// IsPrague returns whether time is either equal to the Prague fork time or greater.
func (c *Config) IsPrague(time uint64) bool {
	return isForked(c.PragueTime, time)
}

// IsOsaka returns whether time is either equal to the Osaka fork time or greater.
func (c *Config) IsOsaka(time uint64) bool {
	return isForked(c.OsakaTime, time)
}

func (c *Config) GetBurntContract(num uint64) accounts.Address {
	if len(c.BurntContract) == 0 {
		return accounts.NilAddress
	}
	addr := ConfigValueLookup(common.ParseMapKeysIntoUint64(c.BurntContract), num)
	return accounts.InternAddress(addr)
}

func (c *Config) GetMinBlobGasPrice() uint64 {
	if c != nil && c.MinBlobGasPrice != nil {
		return *c.MinBlobGasPrice
	}
	return 1 // MIN_BLOB_GASPRICE (EIP-4844)
}

func (c *Config) GetBlobConfig(time uint64) *params.BlobConfig {
	c.parseBlobScheduleOnce.Do(func() {
		// Populate with default values
		c.parsedBlobSchedule = make(map[uint64]*params.BlobConfig)
		if c.CancunTime != nil {
			c.parsedBlobSchedule[*c.CancunTime] = &params.DefaultCancunBlobConfig
		}
		if c.PragueTime != nil {
			c.parsedBlobSchedule[*c.PragueTime] = &params.DefaultPragueBlobConfig
		}

		// Override with supplied values
		val, ok := c.BlobSchedule["cancun"]
		if ok && c.CancunTime != nil {
			c.parsedBlobSchedule[*c.CancunTime] = val
		}
		val, ok = c.BlobSchedule["prague"]
		if ok && c.PragueTime != nil {
			c.parsedBlobSchedule[*c.PragueTime] = val
		}
		val, ok = c.BlobSchedule["osaka"]
		if ok && c.OsakaTime != nil {
			c.parsedBlobSchedule[*c.OsakaTime] = val
		}
		val, ok = c.BlobSchedule["gloas"]
		if ok && c.AmsterdamTime != nil {
			c.parsedBlobSchedule[*c.AmsterdamTime] = val
		}
		val, ok = c.BlobSchedule["bpo1"]
		if ok && c.Bpo1Time != nil {
			c.parsedBlobSchedule[*c.Bpo1Time] = val
		}
		val, ok = c.BlobSchedule["bpo2"]
		if ok && c.Bpo2Time != nil {
			c.parsedBlobSchedule[*c.Bpo2Time] = val
		}
		val, ok = c.BlobSchedule["bpo3"]
		if ok && c.Bpo3Time != nil {
			c.parsedBlobSchedule[*c.Bpo3Time] = val
		}
		val, ok = c.BlobSchedule["bpo4"]
		if ok && c.Bpo4Time != nil {
			c.parsedBlobSchedule[*c.Bpo4Time] = val
		}
		val, ok = c.BlobSchedule["bpo5"]
		if ok && c.Bpo5Time != nil {
			c.parsedBlobSchedule[*c.Bpo5Time] = val
		}
	})

	return ConfigValueLookup(c.parsedBlobSchedule, time)
}

func (c *Config) GetMaxBlobsPerBlock(time uint64) uint64 {
	if blobConfig := c.GetBlobConfig(time); blobConfig != nil {
		return blobConfig.Max
	}
	return 0
}

func (c *Config) GetMaxBlobGasPerBlock(time uint64) uint64 {
	return c.GetMaxBlobsPerBlock(time) * params.GasPerBlob
}

func (c *Config) GetTargetBlobsPerBlock(time uint64) uint64 {
	if blobConfig := c.GetBlobConfig(time); blobConfig != nil {
		return blobConfig.Target
	}
	return 0
}

func (c *Config) GetBlobGasPriceUpdateFraction(time uint64) uint64 {
	if blobConfig := c.GetBlobConfig(time); blobConfig != nil {
		return blobConfig.BaseFeeUpdateFraction
	}
	return 0
}

func (c *Config) GetMaxRlpBlockSize(time uint64) int {
	if c.IsOsaka(time) {
		return params.MaxRlpBlockSize
	}
	return math.MaxInt
}

func (c *Config) SecondsPerSlot() uint64 {
	if c.Aura != nil {
		return 5 // Gnosis
	}
	return 12 // Ethereum
}

func (c *Config) SystemContracts(time uint64) map[string]accounts.Address {
	contracts := map[string]accounts.Address{}
	if c.IsAmsterdam(time) {
		contracts["BUILDER_DEPOSIT_CONTRACT_ADDRESS"] = c.GetBuilderDepositContract()
		contracts["BUILDER_EXIT_CONTRACT_ADDRESS"] = c.GetBuilderExitContract()
	}
	if c.IsCancun(time) {
		contracts["BEACON_ROOTS_ADDRESS"] = params.BeaconRootsAddress
	}
	if c.IsPrague(time) {
		contracts["CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS"] = c.GetConsolidationRequestContract()
		contracts["DEPOSIT_CONTRACT_ADDRESS"] = accounts.InternAddress(c.DepositContract)
		contracts["HISTORY_STORAGE_ADDRESS"] = params.HistoryStorageAddress
		contracts["WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS"] = c.GetWithdrawalRequestContract()
	}
	return contracts
}

// GetWithdrawalRequestContract returns the configured EIP-7002 withdrawal request contract address,
// falling back to the default if not set in the chain config.
func (c *Config) GetWithdrawalRequestContract() accounts.Address {
	if c.WithdrawalRequestContract != nil {
		return accounts.InternAddress(*c.WithdrawalRequestContract)
	}
	return params.WithdrawalRequestAddress
}

// GetConsolidationRequestContract returns the configured EIP-7251 consolidation request contract address,
// falling back to the default if not set in the chain config.
func (c *Config) GetConsolidationRequestContract() accounts.Address {
	if c.ConsolidationRequestContract != nil {
		return accounts.InternAddress(*c.ConsolidationRequestContract)
	}
	return params.ConsolidationRequestAddress
}

// GetBuilderDepositContract returns the configured EIP-8282 builder deposit contract address,
// falling back to the default if not set in the chain config.
func (c *Config) GetBuilderDepositContract() accounts.Address {
	if c.BuilderDepositContract != nil {
		return accounts.InternAddress(*c.BuilderDepositContract)
	}
	return params.BuilderDepositAddress
}

// GetBuilderExitContract returns the configured EIP-8282 builder exit contract address,
// falling back to the default if not set in the chain config.
func (c *Config) GetBuilderExitContract() accounts.Address {
	if c.BuilderExitContract != nil {
		return accounts.InternAddress(*c.BuilderExitContract)
	}
	return params.BuilderExitAddress
}

// CheckCompatible checks whether scheduled fork transitions have been imported
// with a mismatching chain configuration.
func (c *Config) CheckCompatible(newcfg *Config, height, headTime uint64) *ConfigCompatError {
	// The axes are iterated separately. checkCompatibleBlocks returns at its first
	// conflict, so sharing one loop lets a block fork the chain cannot rewind past --
	// an EIP155 chain ID change, whose target is block 0 on every modern chain -- hide
	// every timestamp conflict behind it.
	var blockErr *ConfigCompatError
	for bhead := height; ; {
		err := c.checkCompatibleBlocks(newcfg, bhead)
		if err == nil || (blockErr != nil && err.RewindTo == blockErr.RewindTo) {
			break
		}
		blockErr, bhead = err, err.RewindTo
	}

	var timeErr *ConfigCompatError
	for btime := headTime; ; {
		err := c.checkCompatibleTimestamps(newcfg, btime)
		if err == nil || (timeErr != nil && err.RewindToTime == timeErr.RewindToTime) {
			break
		}
		timeErr, btime = err, err.RewindToTime
	}

	switch {
	case blockErr == nil:
		return timeErr
	case timeErr == nil:
		return blockErr
	default:
		blockErr.WhatTime = timeErr.WhatTime
		blockErr.StoredTime, blockErr.NewTime = timeErr.StoredTime, timeErr.NewTime
		blockErr.RewindToTime = timeErr.RewindToTime
		return blockErr
	}
}

type forkBlockNumber struct {
	name        string
	blockNumber *uint64
	optional    bool // if true, the fork may be nil and next fork is still allowed
	outOfOrder  bool // if true, the fork is exempt from the ordering check (one-off fork, e.g. DAO)
}

func (c *Config) forkBlockNumbers() []forkBlockNumber {
	return []forkBlockNumber{
		{name: "homesteadBlock", blockNumber: c.HomesteadBlock},
		{name: "daoForkBlock", blockNumber: c.DAOForkBlock, optional: true, outOfOrder: true},
		{name: "eip150Block", blockNumber: c.TangerineWhistleBlock},
		{name: "eip155Block", blockNumber: c.SpuriousDragonBlock},
		{name: "byzantiumBlock", blockNumber: c.ByzantiumBlock},
		{name: "constantinopleBlock", blockNumber: c.ConstantinopleBlock},
		{name: "petersburgBlock", blockNumber: c.PetersburgBlock},
		{name: "istanbulBlock", blockNumber: c.IstanbulBlock},
		{name: "muirGlacierBlock", blockNumber: c.MuirGlacierBlock, optional: true},
		{name: "berlinBlock", blockNumber: c.BerlinBlock},
		{name: "londonBlock", blockNumber: c.LondonBlock},
		{name: "arrowGlacierBlock", blockNumber: c.ArrowGlacierBlock, optional: true},
		{name: "grayGlacierBlock", blockNumber: c.GrayGlacierBlock, optional: true},
		{name: "mergeNetsplitBlock", blockNumber: c.MergeNetsplitBlock, optional: true},
	}
}

type forkTimestamp struct {
	name       string // the config field, as it is spelled in JSON
	what       string // how the fork is named in a compatibility error
	timestamp  *uint64
	outOfOrder bool // exempt from the ordering check
}

// forkTimestamps is the one inventory of time-based forks. CheckConfigForkOrder reads it
// as a monotonic sequence, so an entry belongs in its chronological slot rather than at
// the end, and anything whose slot is not settled is marked outOfOrder -- a wrong guess
// there refuses a valid schedule at startup, which is worse than a missed inversion.
func (c *Config) forkTimestamps() []forkTimestamp {
	return []forkTimestamp{
		{name: "shanghaiTime", what: "Shanghai fork timestamp", timestamp: c.ShanghaiTime},
		{name: "cancunTime", what: "Cancun fork timestamp", timestamp: c.CancunTime},
		{name: "pragueTime", what: "Prague fork timestamp", timestamp: c.PragueTime},
		{name: "osakaTime", what: "Osaka fork timestamp", timestamp: c.OsakaTime},
		{name: "bpo1Time", what: "BPO1 fork timestamp", timestamp: c.Bpo1Time},
		{name: "bpo2Time", what: "BPO2 fork timestamp", timestamp: c.Bpo2Time},
		{name: "bpo3Time", what: "BPO3 fork timestamp", timestamp: c.Bpo3Time},
		{name: "bpo4Time", what: "BPO4 fork timestamp", timestamp: c.Bpo4Time},
		{name: "bpo5Time", what: "BPO5 fork timestamp", timestamp: c.Bpo5Time},
		{name: "amsterdamTime", what: "Amsterdam fork timestamp", timestamp: c.AmsterdamTime, outOfOrder: true},
		{name: "balancerTime", what: "Balancer fork timestamp", timestamp: c.BalancerTime, outOfOrder: true},
	}
}

// SameTimestampForks reports whether every time-based fork is scheduled identically.
// When they are, no head time can produce a timestamp conflict, so the caller need not
// establish one.
func (c *Config) SameTimestampForks(newcfg *Config) bool {
	newTimes := newcfg.forkTimestamps()
	for i, f := range c.forkTimestamps() {
		if !numEqual(f.timestamp, newTimes[i].timestamp) {
			return false
		}
	}
	return true
}

// CheckConfigForkOrder checks that we don't "skip" any forks
func (c *Config) CheckConfigForkOrder() error {
	if c != nil && c.ChainID != nil && c.ChainID.Uint64() == 77 {
		return nil
	}

	var lastFork forkBlockNumber

	for _, fork := range c.forkBlockNumbers() {
		if lastFork.name != "" && !fork.outOfOrder {
			// Next one must be higher number
			if lastFork.blockNumber == nil && fork.blockNumber != nil {
				return fmt.Errorf("unsupported fork ordering: %v not enabled, but %v enabled at %v",
					lastFork.name, fork.name, *fork.blockNumber)
			}
			if lastFork.blockNumber != nil && fork.blockNumber != nil {
				if *lastFork.blockNumber > *fork.blockNumber {
					return fmt.Errorf("unsupported fork ordering: %v enabled at %v, but %v enabled at %v",
						lastFork.name, *lastFork.blockNumber, fork.name, *fork.blockNumber)
				}
			}
			// If it was optional and not set, then ignore it
		}
		if (!fork.optional || fork.blockNumber != nil) && !fork.outOfOrder {
			lastFork = fork
		}
	}

	// Time-based forks are all optional -- every one is still ahead of some supported
	// chain -- so only their ordering relative to each other is checked. Amsterdam is
	// exempt because no shipped spec schedules it yet and BPO3-5 may well follow it;
	// Balancer because Gnosis schedules it below its own osakaTime.
	var lastTime forkTimestamp
	for _, fork := range c.forkTimestamps() {
		if fork.timestamp == nil || fork.outOfOrder {
			continue
		}
		if lastTime.timestamp != nil && *lastTime.timestamp > *fork.timestamp {
			return fmt.Errorf("unsupported fork ordering: %v enabled at %v, but %v enabled at %v",
				lastTime.name, *lastTime.timestamp, fork.name, *fork.timestamp)
		}
		lastTime = fork
	}
	return nil
}

// incompatible reports whether a fork scheduled at s1 cannot be rescheduled to s2
// because head is already past the fork. head is a block number or a timestamp
// depending on the axis.
func incompatible(s1, s2 *uint64, head uint64) bool {
	return (isForked(s1, head) || isForked(s2, head)) && !numEqual(s1, s2)
}

func (c *Config) checkCompatibleBlocks(newcfg *Config, head uint64) *ConfigCompatError {
	// Ethereum mainnet forks
	if incompatible(c.HomesteadBlock, newcfg.HomesteadBlock, head) {
		return newCompatError("Homestead fork block", c.HomesteadBlock, newcfg.HomesteadBlock)
	}
	if incompatible(c.DAOForkBlock, newcfg.DAOForkBlock, head) {
		return newCompatError("DAO fork block", c.DAOForkBlock, newcfg.DAOForkBlock)
	}
	if incompatible(c.TangerineWhistleBlock, newcfg.TangerineWhistleBlock, head) {
		return newCompatError("Tangerine Whistle fork block", c.TangerineWhistleBlock, newcfg.TangerineWhistleBlock)
	}
	if incompatible(c.SpuriousDragonBlock, newcfg.SpuriousDragonBlock, head) {
		return newCompatError("Spurious Dragon fork block", c.SpuriousDragonBlock, newcfg.SpuriousDragonBlock)
	}
	if c.IsSpuriousDragon(head) && !uint256Equal(c.ChainID, newcfg.ChainID) {
		return newCompatError("EIP155 chain ID", c.SpuriousDragonBlock, newcfg.SpuriousDragonBlock)
	}
	if incompatible(c.ByzantiumBlock, newcfg.ByzantiumBlock, head) {
		return newCompatError("Byzantium fork block", c.ByzantiumBlock, newcfg.ByzantiumBlock)
	}
	if incompatible(c.ConstantinopleBlock, newcfg.ConstantinopleBlock, head) {
		return newCompatError("Constantinople fork block", c.ConstantinopleBlock, newcfg.ConstantinopleBlock)
	}
	if incompatible(c.PetersburgBlock, newcfg.PetersburgBlock, head) {
		// the only case where we allow Petersburg to be set in the past is if it is equal to Constantinople
		// mainly to satisfy fork ordering requirements which state that Petersburg fork be set if Constantinople fork is set
		if incompatible(c.ConstantinopleBlock, newcfg.PetersburgBlock, head) {
			return newCompatError("Petersburg fork block", c.PetersburgBlock, newcfg.PetersburgBlock)
		}
	}
	if incompatible(c.IstanbulBlock, newcfg.IstanbulBlock, head) {
		return newCompatError("Istanbul fork block", c.IstanbulBlock, newcfg.IstanbulBlock)
	}
	if incompatible(c.MuirGlacierBlock, newcfg.MuirGlacierBlock, head) {
		return newCompatError("Muir Glacier fork block", c.MuirGlacierBlock, newcfg.MuirGlacierBlock)
	}
	if incompatible(c.BerlinBlock, newcfg.BerlinBlock, head) {
		return newCompatError("Berlin fork block", c.BerlinBlock, newcfg.BerlinBlock)
	}
	if incompatible(c.LondonBlock, newcfg.LondonBlock, head) {
		return newCompatError("London fork block", c.LondonBlock, newcfg.LondonBlock)
	}
	if incompatible(c.ArrowGlacierBlock, newcfg.ArrowGlacierBlock, head) {
		return newCompatError("Arrow Glacier fork block", c.ArrowGlacierBlock, newcfg.ArrowGlacierBlock)
	}
	if incompatible(c.GrayGlacierBlock, newcfg.GrayGlacierBlock, head) {
		return newCompatError("Gray Glacier fork block", c.GrayGlacierBlock, newcfg.GrayGlacierBlock)
	}
	if incompatible(c.MergeNetsplitBlock, newcfg.MergeNetsplitBlock, head) {
		return newCompatError("Merge netsplit block", c.MergeNetsplitBlock, newcfg.MergeNetsplitBlock)
	}

	return nil
}

// checkCompatibleTimestamps compares the post-merge forks, which are scheduled by
// timestamp and so cannot be compared against a block number.
func (c *Config) checkCompatibleTimestamps(newcfg *Config, headTime uint64) *ConfigCompatError {
	newTimes := newcfg.forkTimestamps()
	for i, f := range c.forkTimestamps() {
		if incompatible(f.timestamp, newTimes[i].timestamp, headTime) {
			return newTimestampCompatError(f.what, f.timestamp, newTimes[i].timestamp)
		}
	}
	return nil
}

func numEqual(x, y *uint64) bool {
	if x == nil {
		return y == nil
	}
	if y == nil {
		return false
	}
	return *x == *y
}

func uint256Equal(x, y *uint256.Int) bool {
	if x == nil {
		return y == nil
	}
	if y == nil {
		return false
	}
	return x.Cmp(y) == 0
}

// ConfigCompatError is raised if the locally-stored blockchain is initialised with a
// ChainConfig that would alter the past. The two fork axes are independent, so one
// error can carry a conflict on each, and correcting only one still leaves the node
// on an incompatible schedule.
type ConfigCompatError struct {
	// What names the conflicting block-based fork, empty when only timestamps conflict.
	What string
	// block numbers of the stored and new configurations, for a block-based fork
	StoredConfig, NewConfig *uint64
	// the block number to which the local chain must be rewound to correct the error
	RewindTo uint64

	// WhatTime names the conflicting time-based fork, empty when only blocks conflict.
	WhatTime string
	// timestamps of the stored and new configurations, for a time-based fork
	StoredTime, NewTime *uint64
	// the timestamp to which the local chain must be rewound to correct the error
	RewindToTime uint64
}

func newCompatError(what string, storedblock, newblock *uint64) *ConfigCompatError {
	rew := rewindTarget(storedblock, newblock)
	err := &ConfigCompatError{What: what, StoredConfig: storedblock, NewConfig: newblock}
	if rew != nil && *rew > 0 {
		err.RewindTo = *rew - 1
	}
	return err
}

func newTimestampCompatError(what string, storedtime, newtime *uint64) *ConfigCompatError {
	rew := rewindTarget(storedtime, newtime)
	err := &ConfigCompatError{WhatTime: what, StoredTime: storedtime, NewTime: newtime}
	if rew != nil && *rew > 0 {
		err.RewindToTime = *rew - 1
	}
	return err
}

// rewindTarget is the earlier of the two schedules: rewinding past it is what makes the
// two configurations agree again.
func rewindTarget(stored, scheduled *uint64) *uint64 {
	switch {
	case stored == nil:
		return scheduled
	case scheduled == nil || *stored < *scheduled:
		return stored
	default:
		return scheduled
	}
}

// Copy returns a config whose fields can be reassigned without the original seeing it.
// Every pointer, map and slice is shared with the source, so a caller that mutates
// through one still writes into the original -- reassigning a field is what this is for.
//
// Deliberately not a deep copy. jinzhu/copier's DeepCopy turns a nil map or slice into an
// empty one at every nesting depth, and Aura.Validators tells the two apart: a nil List
// with Multi set is a multi validator set, an empty non-nil List is a set with no
// validators at all. Only parseBlobScheduleOnce and its cache are left zeroed, so the
// copy parses its own blob schedule.
func (c *Config) Copy() *Config {
	cp := new(Config)
	src, dst := reflect.ValueOf(c).Elem(), reflect.ValueOf(cp).Elem()
	for i := range src.NumField() {
		if dst.Field(i).CanSet() {
			dst.Field(i).Set(src.Field(i))
		}
	}
	return cp
}

func uint64PtrStr(p *uint64) string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", *p)
}

func (err *ConfigCompatError) Error() string {
	blocks := fmt.Sprintf("mismatching %s in database (have %s, want %s, rewindto %d)",
		err.What, uint64PtrStr(err.StoredConfig), uint64PtrStr(err.NewConfig), err.RewindTo)
	times := fmt.Sprintf("mismatching %s in database (have timestamp %s, want timestamp %s, rewindto timestamp %d)",
		err.WhatTime, uint64PtrStr(err.StoredTime), uint64PtrStr(err.NewTime), err.RewindToTime)
	switch {
	case !err.HasTimestampConflict():
		return blocks
	case !err.HasBlockConflict():
		return times
	default:
		return blocks + "; " + times
	}
}

// HasBlockConflict reports whether a block-based fork conflicts.
func (err *ConfigCompatError) HasBlockConflict() bool { return err.What != "" }

// HasTimestampConflict reports whether a time-based fork conflicts. A fork activating at
// timestamp 0 or 1 rewinds to 0, so a zero rewind target cannot stand in for "no conflict".
func (err *ConfigCompatError) HasTimestampConflict() bool { return err.WhatTime != "" }

// EthashConfig is the rules engine configs for proof-of-work based sealing.
type EthashConfig struct{}

// String implements the stringer interface, returning the rules engine details.
func (c *EthashConfig) String() string {
	return "ethash"
}

// Looks up a config value as of a given block number (or time).
// The assumption here is that config is a càdlàg map of starting_from_block -> value.
// For example, config of {5: "A", 10: "B", 20: "C"}
// means that the config value is "A" for blocks 5–9,
// "B" for blocks 10–19, and "C" for block 20 and above.
// For blocks 0–4 an empty string will be returned.
func ConfigValueLookup[T any](field map[uint64]T, number uint64) T {
	keys := common.SortedKeys(field)
	if len(keys) == 0 || number < keys[0] {
		return generics.Zero[T]()
	}
	for i := 0; i < len(keys)-1; i++ {
		if number >= keys[i] && number < keys[i+1] {
			return field[keys[i]]
		}
	}
	return field[keys[len(keys)-1]]
}

// Rules is syntactic sugar over Config. It can be used for functions
// that do not have or require information about the block.
//
// Rules is a one time interface meaning that it shouldn't be used in between transition
// phases.
type Rules struct {
	ChainID                                           *uint256.Int
	IsHomestead, IsTangerineWhistle, IsSpuriousDragon bool
	IsByzantium, IsConstantinople, IsPetersburg       bool
	IsIstanbul, IsBerlin, IsLondon, IsShanghai        bool
	IsCancun                                          bool
	IsPrague, IsOsaka, IsAmsterdam                    bool
	DisabledEIPs                                      []int
	IsAura                                            bool

	// L2Version is the L2 stack's own upgrade version (e.g. an ArbOS-style
	// version ladder), resolved per block by the chain's L2Config oracle.
	// Zero for L1 chains.
	L2Version uint64
}

// IsEIPEnabled reports whether the given EIP is active for this chain: its
// parent fork is active AND it is not listed in DisabledEIPs. Complete gate —
// no separate fork check needed at call sites.
func (r *Rules) IsEIPEnabled(eip int) bool {
	if slices.Contains(r.DisabledEIPs, eip) {
		return false
	}
	switch eip {
	case 7708, 7928:
		return r.IsAmsterdam
	case 161, 170:
		return r.IsSpuriousDragon
	default:
		panic(fmt.Sprintf("IsEIPEnabled: EIP %d is not mapped to a fork", eip))
	}
}

// IsEIP161Enabled reports whether EIP-161 is in effect: the Spurious Dragon fork
// is active and EIP-161 is not disabled (genesis/pre-state loads disable it via
// DisabledEIPs to retain declared empty accounts).
func (r *Rules) IsEIP161Enabled() bool {
	return r.IsEIPEnabled(161)
}

// IsEIP170Enabled reports whether EIP-170 (the contract code-size limit) is
// enabled: Spurious Dragon is active and EIP-170 is not disabled (Gnosis/Chiado
// disable it via DisabledEIPs).
func (r *Rules) IsEIP170Enabled() bool {
	return r.IsEIPEnabled(170)
}

// isForked returns whether a fork scheduled at block s is active at the given head block.
func isForked(s *uint64, head uint64) bool {
	if s == nil {
		return false
	}
	return *s <= head
}

func (c *Config) IsPreMerge(blockNumber uint64) bool {
	return c.MergeHeight != nil && blockNumber < *c.MergeHeight
}
