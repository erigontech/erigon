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
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon/execution/chain/params"
)

// Config is the core config which determines the blockchain settings.
//
// Config is stored in the database on a per block basis. This means
// that any network, identified by its genesis block, can have its own
// set of configuration options.
//
// Config must be copied only with jinzhu/copier since it contains a sync.Once.
type Config struct {
	ChainName string   `json:"chainName"` // chain name, eg: mainnet, sepolia, bor-mainnet
	ChainID   *big.Int `json:"chainId"`   // chainId identifies the current chain and is used for replay protection

	Consensus ConsensusName `json:"consensus,omitempty"` // aura, ethash or clique

	// *Block fields activate the corresponding hard fork at a certain block number,
	// while *Time fields do so based on the block's time stamp.
	// nil means that the hard-fork is not scheduled,
	// while 0 means that it's already activated from genesis.

	// ETH mainnet upgrades
	// See https://github.com/ethereum/execution-specs/blob/master/network-upgrades/mainnet-upgrades
	HomesteadBlock        *big.Int `json:"homesteadBlock,omitempty"`
	DAOForkBlock          *big.Int `json:"daoForkBlock,omitempty"`
	TangerineWhistleBlock *big.Int `json:"eip150Block,omitempty"`
	SpuriousDragonBlock   *big.Int `json:"eip155Block,omitempty"`
	ByzantiumBlock        *big.Int `json:"byzantiumBlock,omitempty"`
	ConstantinopleBlock   *big.Int `json:"constantinopleBlock,omitempty"`
	PetersburgBlock       *big.Int `json:"petersburgBlock,omitempty"`
	IstanbulBlock         *big.Int `json:"istanbulBlock,omitempty"`
	MuirGlacierBlock      *big.Int `json:"muirGlacierBlock,omitempty"`
	BerlinBlock           *big.Int `json:"berlinBlock,omitempty"`
	LondonBlock           *big.Int `json:"londonBlock,omitempty"`
	ArrowGlacierBlock     *big.Int `json:"arrowGlacierBlock,omitempty"`
	GrayGlacierBlock      *big.Int `json:"grayGlacierBlock,omitempty"`

	// EIP-3675: Upgrade consensus to Proof-of-Stake (a.k.a. "Paris", "The Merge")
	TerminalTotalDifficulty       *big.Int `json:"terminalTotalDifficulty,omitempty"`       // The merge happens when terminal total difficulty is reached
	TerminalTotalDifficultyPassed bool     `json:"terminalTotalDifficultyPassed,omitempty"` // Disable PoW sync for networks that have already passed through the Merge
	MergeNetsplitBlock            *big.Int `json:"mergeNetsplitBlock,omitempty"`            // Virtual fork after The Merge to use as a network splitter; see FORK_NEXT_VALUE in EIP-3675
	MergeHeight                   *big.Int `json:"mergeBlock,omitempty"`                    // The Merge block number

	// Mainnet fork scheduling switched from block numbers to timestamps after The Merge
	ShanghaiTime *big.Int `json:"shanghaiTime,omitempty"`
	CancunTime   *big.Int `json:"cancunTime,omitempty"`
	PragueTime   *big.Int `json:"pragueTime,omitempty"`
	OsakaTime    *big.Int `json:"osakaTime,omitempty"`

	// Optional EIP-4844 parameters (see also EIP-7691, EIP-7840, EIP-7892)
	MinBlobGasPrice       *uint64                       `json:"minBlobGasPrice,omitempty"`
	BlobSchedule          map[string]*params.BlobConfig `json:"blobSchedule,omitempty"`
	Bpo1Time              *big.Int                      `json:"bpo1Time,omitempty"`
	Bpo2Time              *big.Int                      `json:"bpo2Time,omitempty"`
	Bpo3Time              *big.Int                      `json:"bpo3Time,omitempty"`
	Bpo4Time              *big.Int                      `json:"bpo4Time,omitempty"`
	Bpo5Time              *big.Int                      `json:"bpo5Time,omitempty"`
	parseBlobScheduleOnce sync.Once                     `copier:"-"`
	parsedBlobSchedule    map[uint64]*params.BlobConfig

	// (Optional) governance contract where EIP-1559 fees will be sent to, which otherwise would be burnt since the London fork.
	// A key corresponds to the block number, starting from which the fees are sent to the address (map value).
	// Starting from Prague, EIP-4844 fees might be collected as well:
	// see https://github.com/gnosischain/specs/blob/master/network-upgrades/pectra.md#eip-4844-pectra.
	BurntContract map[string]common.Address `json:"burntContract,omitempty"`

	// (Optional) deposit contract of PoS chains
	// See also EIP-6110: Supply validator deposits on chain
	DepositContract common.Address `json:"depositContractAddress,omitempty"`

	DefaultBlockGasLimit *uint64 `json:"defaultBlockGasLimit,omitempty"`

	// Various consensus engines
	Ethash *EthashConfig `json:"ethash,omitempty"`
	Clique *CliqueConfig `json:"clique,omitempty"`
	Aura   *AuRaConfig   `json:"aura,omitempty"`

	Bor     BorConfig       `json:"-"`
	BorJSON json.RawMessage `json:"bor,omitempty"`

	// Account Abstraction
	AllowAA bool
}

var (
	TestChainConfig = &Config{
		ChainID:               big.NewInt(1337),
		Consensus:             EtHashConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		Ethash:                new(EthashConfig),
	}

	TestChainAuraConfig = &Config{
		ChainID:               big.NewInt(1),
		Consensus:             AuRaConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		Aura:                  &AuRaConfig{},
	}

	// AllProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the main net protocol.
	AllProtocolChanges = &Config{
		ChainID:                       big.NewInt(1337),
		Consensus:                     EtHashConsensus,
		HomesteadBlock:                big.NewInt(0),
		TangerineWhistleBlock:         big.NewInt(0),
		SpuriousDragonBlock:           big.NewInt(0),
		ByzantiumBlock:                big.NewInt(0),
		ConstantinopleBlock:           big.NewInt(0),
		PetersburgBlock:               big.NewInt(0),
		IstanbulBlock:                 big.NewInt(0),
		MuirGlacierBlock:              big.NewInt(0),
		BerlinBlock:                   big.NewInt(0),
		LondonBlock:                   big.NewInt(0),
		ArrowGlacierBlock:             big.NewInt(0),
		GrayGlacierBlock:              big.NewInt(0),
		TerminalTotalDifficulty:       big.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  big.NewInt(0),
		CancunTime:                    big.NewInt(0),
		PragueTime:                    big.NewInt(0),
		Ethash:                        new(EthashConfig),
	}
)

type BorConfig interface {
	fmt.Stringer
	IsAgra(num uint64) bool
	GetAgraBlock() *big.Int
	IsNapoli(num uint64) bool
	GetNapoliBlock() *big.Int
	IsAhmedabad(number uint64) bool
	GetAhmedabadBlock() *big.Int
	IsBhilai(num uint64) bool
	GetBhilaiBlock() *big.Int
	IsVeBlop(num uint64) bool
	GetVeBlopBlock() *big.Int
	StateReceiverContractAddress() common.Address
	CalculateSprintNumber(number uint64) uint64
	CalculateSprintLength(number uint64) uint64
}

func timestampToTime(unixTime *big.Int) *time.Time {
	if unixTime == nil {
		return nil
	}
	t := time.Unix(unixTime.Int64(), 0).UTC()
	return &t
}

func (c *Config) String() string {
	engine := c.getEngine()

	if c.Bor != nil {
		return fmt.Sprintf("{ChainID: %v, Agra: %v, Napoli: %v, Ahmedabad: %v, Bhilai: %v, VeBlop: %v, Engine: %v}",
			c.ChainID,
			c.Bor.GetAgraBlock(),
			c.Bor.GetNapoliBlock(),
			c.Bor.GetAhmedabadBlock(),
			c.Bor.GetBhilaiBlock(),
			c.Bor.GetVeBlopBlock(),
			engine,
		)
	}

	return fmt.Sprintf("{ChainID: %v, Terminal Total Difficulty: %v, Shapella: %v, Dencun: %v, Pectra: %v, Fusaka: %v, BPO1: %v, BPO2: %v, BPO3: %v, BPO4: %v, BPO5: %v, Engine: %v}",
		c.ChainID,
		c.TerminalTotalDifficulty,
		timestampToTime(c.ShanghaiTime),
		timestampToTime(c.CancunTime),
		timestampToTime(c.PragueTime),
		timestampToTime(c.OsakaTime),
		timestampToTime(c.Bpo1Time),
		timestampToTime(c.Bpo2Time),
		timestampToTime(c.Bpo3Time),
		timestampToTime(c.Bpo4Time),
		timestampToTime(c.Bpo5Time),
		engine,
	)
}

func (c *Config) getEngine() string {
	switch {
	case c.Ethash != nil:
		return c.Ethash.String()
	case c.Clique != nil:
		return c.Clique.String()
	case c.Bor != nil:
		return c.Bor.String()
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

// IsPetersburg returns whether num is either
// - equal to or greater than the PetersburgBlock fork block,
// - OR is nil, and Constantinople is active
func (c *Config) IsPetersburg(num uint64) bool {
	return isForked(c.PetersburgBlock, num) || c.PetersburgBlock == nil && isForked(c.ConstantinopleBlock, num)
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

// IsAgra returns whether num is either equal to the Agra fork block or greater.
// The Agra hard fork is based on the Shanghai hard fork, but it doesn't include withdrawals.
// Also Agra is activated based on the block number rather than the timestamp.
// Refer to https://forum.polygon.technology/t/pip-28-agra-hardfork
func (c *Config) IsAgra(num uint64) bool {
	return (c != nil) && (c.Bor != nil) && c.Bor.IsAgra(num)
}

// Refer to https://forum.polygon.technology/t/pip-33-napoli-upgrade
func (c *Config) IsNapoli(num uint64) bool {
	return (c != nil) && (c.Bor != nil) && c.Bor.IsNapoli(num)
}

// Refer to https://forum.polygon.technology/t/pip-63-bhilai-hardfork
func (c *Config) IsBhilai(num uint64) bool {
	return (c != nil) && (c.Bor != nil) && c.Bor.IsBhilai(num)
}

// IsCancun returns whether time is either equal to the Cancun fork time or greater.
func (c *Config) IsCancun(time uint64) bool {
	return isForked(c.CancunTime, time)
}

// IsPrague returns whether time is either equal to the Prague fork time or greater.
func (c *Config) IsPrague(time uint64) bool {
	return isForked(c.PragueTime, time)
}

// IsOsaka returns whether time is either equal to the Osaka fork time or greater.
func (c *Config) IsOsaka(time uint64) bool {
	return isForked(c.OsakaTime, time)
}

func (c *Config) GetBurntContract(num uint64) *common.Address {
	if len(c.BurntContract) == 0 {
		return nil
	}
	addr := ConfigValueLookup(common.ParseMapKeysIntoUint64(c.BurntContract), num)
	return &addr
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
		c.parsedBlobSchedule = map[uint64]*params.BlobConfig{
			0: {},
		}
		if c.CancunTime != nil {
			c.parsedBlobSchedule[c.CancunTime.Uint64()] = &params.DefaultCancunBlobConfig
		}
		if c.PragueTime != nil {
			c.parsedBlobSchedule[c.PragueTime.Uint64()] = &params.DefaultPragueBlobConfig
		}
		if c.OsakaTime != nil {
			c.parsedBlobSchedule[c.OsakaTime.Uint64()] = &params.DefaultOsakaBlobConfig
		}

		// Override with supplied values
		val, ok := c.BlobSchedule["cancun"]
		if ok && c.CancunTime != nil {
			c.parsedBlobSchedule[c.CancunTime.Uint64()] = val
		}
		val, ok = c.BlobSchedule["prague"]
		if ok && c.PragueTime != nil {
			c.parsedBlobSchedule[c.PragueTime.Uint64()] = val
		}
		val, ok = c.BlobSchedule["osaka"]
		if ok && c.OsakaTime != nil {
			c.parsedBlobSchedule[c.OsakaTime.Uint64()] = val
		}
		val, ok = c.BlobSchedule["bpo1"]
		if ok && c.Bpo1Time != nil {
			c.parsedBlobSchedule[c.Bpo1Time.Uint64()] = val
		}
		val, ok = c.BlobSchedule["bpo2"]
		if ok && c.Bpo2Time != nil {
			c.parsedBlobSchedule[c.Bpo2Time.Uint64()] = val
		}
		val, ok = c.BlobSchedule["bpo3"]
		if ok && c.Bpo3Time != nil {
			c.parsedBlobSchedule[c.Bpo3Time.Uint64()] = val
		}
		val, ok = c.BlobSchedule["bpo4"]
		if ok && c.Bpo4Time != nil {
			c.parsedBlobSchedule[c.Bpo4Time.Uint64()] = val
		}
		val, ok = c.BlobSchedule["bpo5"]
		if ok && c.Bpo5Time != nil {
			c.parsedBlobSchedule[c.Bpo5Time.Uint64()] = val
		}
	})

	return ConfigValueLookup(c.parsedBlobSchedule, time)
}

func (c *Config) GetMaxBlobsPerBlock(time uint64) uint64 {
	return c.GetBlobConfig(time).Max
}

func (c *Config) GetMaxBlobGasPerBlock(time uint64) uint64 {
	return c.GetMaxBlobsPerBlock(time) * params.GasPerBlob
}

func (c *Config) GetTargetBlobsPerBlock(time uint64) uint64 {
	return c.GetBlobConfig(time).Target
}

func (c *Config) GetBlobGasPriceUpdateFraction(time uint64) uint64 {
	return c.GetBlobConfig(time).BaseFeeUpdateFraction
}

func (c *Config) GetMaxRlpBlockSize(time uint64) int {
	if c.IsOsaka(time) {
		return params.MaxRlpBlockSize
	}
	return math.MaxInt
}

func (c *Config) SecondsPerSlot() uint64 {
	if c.Bor != nil {
		return 2 // Polygon
	}
	if c.Aura != nil {
		return 5 // Gnosis
	}
	return 12 // Ethereum
}

func (c *Config) SystemContracts(time uint64) map[string]common.Address {
	contracts := map[string]common.Address{}
	if c.IsCancun(time) {
		contracts["BEACON_ROOTS_ADDRESS"] = params.BeaconRootsAddress
	}
	if c.IsPrague(time) {
		contracts["CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS"] = params.ConsolidationRequestAddress
		contracts["DEPOSIT_CONTRACT_ADDRESS"] = c.DepositContract
		contracts["HISTORY_STORAGE_ADDRESS"] = params.HistoryStorageAddress
		contracts["WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS"] = params.WithdrawalRequestAddress
	}
	return contracts
}

// CheckCompatible checks whether scheduled fork transitions have been imported
// with a mismatching chain configuration.
func (c *Config) CheckCompatible(newcfg *Config, height uint64) *ConfigCompatError {
	bhead := height

	// Iterate checkCompatible to find the lowest conflict.
	var lasterr *ConfigCompatError
	for {
		err := c.checkCompatible(newcfg, bhead)
		if err == nil || (lasterr != nil && err.RewindTo == lasterr.RewindTo) {
			break
		}
		lasterr = err
		bhead = err.RewindTo
	}
	return lasterr
}

type forkBlockNumber struct {
	name        string
	blockNumber *big.Int
	optional    bool // if true, the fork may be nil and next fork is still allowed
}

func (c *Config) forkBlockNumbers() []forkBlockNumber {
	return []forkBlockNumber{
		{name: "homesteadBlock", blockNumber: c.HomesteadBlock},
		{name: "daoForkBlock", blockNumber: c.DAOForkBlock, optional: true},
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

// CheckConfigForkOrder checks that we don't "skip" any forks
func (c *Config) CheckConfigForkOrder() error {
	if c != nil && c.ChainID != nil && c.ChainID.Uint64() == 77 {
		return nil
	}

	var lastFork forkBlockNumber

	for _, fork := range c.forkBlockNumbers() {
		if lastFork.name != "" {
			// Next one must be higher number
			if lastFork.blockNumber == nil && fork.blockNumber != nil {
				return fmt.Errorf("unsupported fork ordering: %v not enabled, but %v enabled at %v",
					lastFork.name, fork.name, fork.blockNumber)
			}
			if lastFork.blockNumber != nil && fork.blockNumber != nil {
				if lastFork.blockNumber.Cmp(fork.blockNumber) > 0 {
					return fmt.Errorf("unsupported fork ordering: %v enabled at %v, but %v enabled at %v",
						lastFork.name, lastFork.blockNumber, fork.name, fork.blockNumber)
				}
			}
			// If it was optional and not set, then ignore it
		}
		if !fork.optional || fork.blockNumber != nil {
			lastFork = fork
		}
	}
	return nil
}

func (c *Config) checkCompatible(newcfg *Config, head uint64) *ConfigCompatError {
	// returns true if a fork scheduled at s1 cannot be rescheduled to block s2 because head is already past the fork.
	incompatible := func(s1, s2 *big.Int, head uint64) bool {
		return (isForked(s1, head) || isForked(s2, head)) && !numEqual(s1, s2)
	}

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
	if c.IsSpuriousDragon(head) && !numEqual(c.ChainID, newcfg.ChainID) {
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

func numEqual(x, y *big.Int) bool {
	if x == nil {
		return y == nil
	}
	if y == nil {
		return x == nil
	}
	return x.Cmp(y) == 0
}

// ConfigCompatError is raised if the locally-stored blockchain is initialised with a
// ChainConfig that would alter the past.
type ConfigCompatError struct {
	What string
	// block numbers of the stored and new configurations
	StoredConfig, NewConfig *big.Int
	// the block number to which the local chain must be rewound to correct the error
	RewindTo uint64
}

func newCompatError(what string, storedblock, newblock *big.Int) *ConfigCompatError {
	var rew *big.Int
	switch {
	case storedblock == nil:
		rew = newblock
	case newblock == nil || storedblock.Cmp(newblock) < 0:
		rew = storedblock
	default:
		rew = newblock
	}
	err := &ConfigCompatError{what, storedblock, newblock, 0}
	if rew != nil && rew.Sign() > 0 {
		err.RewindTo = rew.Uint64() - 1
	}
	return err
}

func (err *ConfigCompatError) Error() string {
	return fmt.Sprintf("mismatching %s in database (have %d, want %d, rewindto %d)", err.What, err.StoredConfig, err.NewConfig, err.RewindTo)
}

// EthashConfig is the consensus engine configs for proof-of-work based sealing.
type EthashConfig struct{}

// String implements the stringer interface, returning the consensus engine details.
func (c *EthashConfig) String() string {
	return "ethash"
}

// CliqueConfig is the consensus engine configs for proof-of-authority based sealing.
type CliqueConfig struct {
	Period uint64 `json:"period"` // Number of seconds between blocks to enforce
	Epoch  uint64 `json:"epoch"`  // Epoch length to reset votes and checkpoint
}

// String implements the stringer interface, returning the consensus engine details.
func (c *CliqueConfig) String() string {
	return "clique"
}

// Looks up a config value as of a given block number (or time).
// The assumption here is that config is a càdlàg map of starting_from_block -> value.
// For example, config of {5: "A", 10: "B", 20: "C"}
// means that the config value is "A" for blocks 5–9,
// "B" for blocks 10–19, and "C" for block 20 and above.
// For blocks 0–4 an empty string will be returned.
func ConfigValueLookup[T any](field map[uint64]T, number uint64) T {
	keys := common.SortedKeys(field)
	if number < keys[0] {
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
	ChainID                                           *big.Int
	IsHomestead, IsTangerineWhistle, IsSpuriousDragon bool
	IsByzantium, IsConstantinople, IsPetersburg       bool
	IsIstanbul, IsBerlin, IsLondon, IsShanghai        bool
	IsCancun, IsNapoli, IsBhilai                      bool
	IsPrague, IsOsaka                                 bool
	IsAura                                            bool
}

// isForked returns whether a fork scheduled at block s is active at the given head block.
func isForked(s *big.Int, head uint64) bool {
	if s == nil {
		return false
	}
	return s.Uint64() <= head
}

func (c *Config) IsPreMerge(blockNumber uint64) bool {
	return c.MergeHeight != nil && blockNumber < c.MergeHeight.Uint64()
}
