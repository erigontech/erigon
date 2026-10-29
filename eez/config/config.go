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
// Package config holds the configuration of an EEZ network that the protocol
// does not fix: the l2 section of its chain configuration.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// Name identifies the EEZ stack in chain configurations and selects its rules
// engine.
const Name = "eez"

// ErrInvalid reports an l2 section that does not describe a usable network.
var ErrInvalid = errors.New("invalid eez l2 config")

// L2Config is the l2 section of an EEZ chain configuration. Chain id, fork
// schedule and fee collector come from the standard genesis fields; protocol
// constants live in eez/protocol.
type L2Config struct {
	// RollupID is the rollup id the EEZ registry assigned to this network.
	RollupID uint64 `json:"rollupId"`
	// L1ChainID is the chain id of the settlement layer.
	L1ChainID uint64 `json:"l1ChainId"`
	// Registry is the EEZ registry contract on L1.
	Registry common.Address `json:"eezRegistry"`
	// RegistryDeployBlock is the L1 block the registry was deployed in, where
	// settlement scanning starts.
	RegistryDeployBlock uint64 `json:"registryDeployBlock"`
	// BlockInterval is the L2 block time in seconds.
	BlockInterval uint64 `json:"blockInterval"`
	// L1BlockTime is the L1 block time in seconds; it is a multiple of
	// BlockInterval, so every L1 block spans the same number of L2 blocks.
	L1BlockTime uint64 `json:"l1BlockTime"`
	// BlockGasLimit is the gas limit of every L2 block.
	BlockGasLimit uint64 `json:"blockGasLimit"`
	// RollupX selects the eez-rollupX rule set; it is the config.rollupX
	// section of an eez-rollupX genesis.
	RollupX *RollupXConfig `json:"rollupX,omitempty"`
}

// RollupXConfig is the derivation schedule of an eez-rollupX network. A
// fee collector and a history update offset select the v0.1 rule set.
type RollupXConfig struct {
	// BlockIntervalSeconds is the L2 block time.
	BlockIntervalSeconds uint64 `json:"blockIntervalSeconds"`
	// SyncIntervalBlocks is the number of L2 blocks from one Sync block to
	// the next.
	SyncIntervalBlocks uint64 `json:"syncIntervalBlocks"`
	// L1Reference is the finalized L1 block the genesis is anchored to.
	L1Reference L1Reference `json:"l1Reference"`
	// HistoryUpdateOffset places the L1 history updates: in the blocks
	// HistoryUpdateOffset+1 after each Sync position.
	HistoryUpdateOffset *uint64 `json:"historyUpdateOffset,omitempty"`
	// FeeCollector receives the base fee of ordinary transactions.
	FeeCollector *common.Address `json:"feeCollector,omitempty"`
}

// L1Reference is the L1 block an eez-rollupX genesis is anchored to. Its
// timestamp fixes the phase of the settlement slots; its prevRandao is the
// genesis mixHash and the seed of every block before the first live anchor.
type L1Reference struct {
	ChainID    uint64      `json:"chainId"`
	Number     uint64      `json:"number"`
	Hash       common.Hash `json:"hash"`
	Timestamp  uint64      `json:"timestamp"`
	PrevRandao common.Hash `json:"prevRandao"`
}

// Rule sets, as chain.Rules.L2Version carries them; L2Version is zero
// outside EEZ chains. Transaction types EEZ registers process-wide are valid
// only where it is set.
const (
	// Rollup0 is the rule set of eez-rollup0, the reference client.
	Rollup0 uint64 = 1
	// RollupX is the rule set of eez-rollupX v0.0.1, which the retired
	// Chiado networks and the Ethereum network run.
	RollupX uint64 = 2
	// RollupXV01 is the rule set of eez-rollupX v0.1, which the Chiado
	// networks run since 2026-10-28: types 0x45 and 0x46, base fee to a
	// fee collector, L1 history updates.
	RollupXV01 uint64 = 3
)

// IsRollupX reports whether ruleSet is a rule set of eez-rollupX.
func IsRollupX(ruleSet uint64) bool { return ruleSet == RollupX || ruleSet == RollupXV01 }

// RollupX constants.
const (
	RollupXBlockGasLimit            uint64 = 30_000_000
	RollupXBaseFeeChangeDenominator uint64 = 50
)

var _ chain.L2Config = (*L2Config)(nil)

// Name returns the EEZ stack name.
func (c *L2Config) Name() string { return Name }

// ResolveRules sets the rule set. Forks activate by the genesis timestamps;
// EEZ has no version ladder of its own.
func (c *L2Config) ResolveRules(l2Version, blockNum, blockTime uint64, rules *chain.Rules) {
	rules.L2Version = c.RuleSet()
}

// RuleSet returns RollupXV01 or RollupX when the section has a rollupX
// schedule, Rollup0 otherwise.
func (c *L2Config) RuleSet() uint64 {
	switch {
	case c.RollupX == nil:
		return Rollup0
	case c.RollupX.FeeCollector != nil:
		return RollupXV01
	}
	return RollupX
}

// EEZL2 returns the address of the rule set's EEZL2.
func (c *L2Config) EEZL2() common.Address { return EEZL2(c.RuleSet()) }

// EEZL2 returns the address of the EEZL2 of ruleSet.
func EEZL2(ruleSet uint64) common.Address {
	if IsRollupX(ruleSet) {
		return protocol.RollupXEEZL2Address
	}
	return protocol.EEZL2Address
}

// BlocksPerL1Block is the number of L2 blocks one L1 block spans.
func (c *L2Config) BlocksPerL1Block() uint64 { return c.L1BlockTime / c.BlockInterval }

// Validate rejects a configuration no EEZ network can run with.
func (c *L2Config) Validate() error {
	switch {
	case c.RollupID == 0:
		return fmt.Errorf("%w: rollupId is zero", ErrInvalid)
	case c.L1ChainID == 0:
		return fmt.Errorf("%w: l1ChainId is zero", ErrInvalid)
	case c.Registry == (common.Address{}):
		return fmt.Errorf("%w: eezRegistry is the zero address", ErrInvalid)
	case c.BlockInterval == 0 || c.L1BlockTime == 0:
		return fmt.Errorf("%w: blockInterval and l1BlockTime must be positive", ErrInvalid)
	case c.L1BlockTime%c.BlockInterval != 0:
		return fmt.Errorf("%w: l1BlockTime %d is not a multiple of blockInterval %d", ErrInvalid, c.L1BlockTime, c.BlockInterval)
	case c.BlocksPerL1Block() < 2:
		// The Sync block must be distinct from the blocks around it.
		return fmt.Errorf("%w: %d L2 blocks per L1 block, need at least 2", ErrInvalid, c.BlocksPerL1Block())
	case c.BlockGasLimit == 0:
		return fmt.Errorf("%w: blockGasLimit is zero", ErrInvalid)
	}
	if c.RollupX != nil {
		return c.RollupX.validate(c)
	}
	return nil
}

// validate checks the schedule on its own and against the l2 section, which
// restates its interval and slot time.
func (x *RollupXConfig) validate(l2 *L2Config) error {
	step, positions, ref := x.BlockIntervalSeconds, x.SyncIntervalBlocks, x.L1Reference
	switch {
	case step == 0 || positions < 2:
		return fmt.Errorf("%w: rollupX schedule of %d s and %d blocks", ErrInvalid, step, positions)
	case ref.ChainID == 1 && (step != 2 || positions != 6):
		return fmt.Errorf("%w: rollupX on Ethereum needs 2 s blocks and Sync every 6", ErrInvalid)
	case ref.ChainID == 10200 && (step != 1 || positions != 5):
		return fmt.Errorf("%w: rollupX on Chiado needs 1 s blocks and Sync every 5", ErrInvalid)
	case ref.ChainID != l2.L1ChainID:
		return fmt.Errorf("%w: rollupX L1 reference on chain %d, l1ChainId %d", ErrInvalid, ref.ChainID, l2.L1ChainID)
	case ref.Hash == (common.Hash{}):
		return fmt.Errorf("%w: rollupX L1 reference hash is zero", ErrInvalid)
	case l2.BlockInterval != step || l2.L1BlockTime/positions != step || l2.L1BlockTime%positions != 0:
		return fmt.Errorf("%w: blockInterval %d and l1BlockTime %d differ from the rollupX schedule", ErrInvalid, l2.BlockInterval, l2.L1BlockTime)
	case l2.BlockGasLimit != RollupXBlockGasLimit:
		return fmt.Errorf("%w: rollupX blockGasLimit %d, want %d", ErrInvalid, l2.BlockGasLimit, RollupXBlockGasLimit)
	case (x.FeeCollector == nil) != (x.HistoryUpdateOffset == nil):
		return fmt.Errorf("%w: rollupX feeCollector and historyUpdateOffset come together", ErrInvalid)
	case x.HistoryUpdateOffset != nil && *x.HistoryUpdateOffset >= positions:
		return fmt.Errorf("%w: rollupX historyUpdateOffset %d of %d positions", ErrInvalid, *x.HistoryUpdateOffset, positions)
	case x.FeeCollector != nil && (*x.FeeCollector == common.Address{} || *x.FeeCollector == protocol.NativeSystemAddress || *x.FeeCollector == protocol.RollupXEEZL2Address):
		return fmt.Errorf("%w: rollupX feeCollector %v", ErrInvalid, *x.FeeCollector)
	}
	return nil
}

// HistoryUpdate reports whether block number may carry an L1 history update
// under the v0.1 rule set: whether it is HistoryUpdateOffset+1 blocks after
// a Sync position.
func (x *RollupXConfig) HistoryUpdate(number uint64) bool {
	if x.HistoryUpdateOffset == nil {
		return false
	}
	delay := *x.HistoryUpdateOffset + 1
	return number >= delay && (number-delay)%x.SyncIntervalBlocks == 0
}

// ValidateChain rejects chain configuration fields the rule set fixes
// otherwise. RollupX runs Osaka from genesis and has the 2/50 EIP-1559 rule;
// v0.0.1 burns the base fee, v0.1 sends it to the rollupX fee collector.
func (c *L2Config) ValidateChain(cc *chain.Config) error {
	if c.RollupX == nil {
		if cc.BaseFeeChangeDenominator != nil {
			return fmt.Errorf("%w: baseFeeChangeDenominator outside rollupX", ErrInvalid)
		}
		return nil
	}
	switch {
	case cc.BaseFeeChangeDenominator == nil || *cc.BaseFeeChangeDenominator != RollupXBaseFeeChangeDenominator:
		return fmt.Errorf("%w: rollupX needs baseFeeChangeDenominator %d", ErrInvalid, RollupXBaseFeeChangeDenominator)
	case len(cc.BurntContract) > 0:
		return fmt.Errorf("%w: rollupX has no burnt contract", ErrInvalid)
	case c.RollupX.FeeCollector == nil && cc.FeeCollector != nil:
		return fmt.Errorf("%w: rollupX v0.0.1 burns the base fee", ErrInvalid)
	case c.RollupX.FeeCollector != nil && (cc.FeeCollector == nil || *cc.FeeCollector != *c.RollupX.FeeCollector):
		return fmt.Errorf("%w: feeCollector differs from the rollupX fee collector %v", ErrInvalid, *c.RollupX.FeeCollector)
	case cc.OsakaTime == nil || *cc.OsakaTime != 0 || cc.AmsterdamTime != nil:
		return fmt.Errorf("%w: rollupX runs Osaka from genesis and no later fork", ErrInvalid)
	}
	return nil
}

// CheckGenesis checks the genesis against the schedule: block 0, a whole
// number of slots after the L1 reference, the reference prevRandao as
// mixHash. Under v0.1 the reference precedes genesis by a slot at least,
// where the first history interval starts.
func (x *RollupXConfig) CheckGenesis(genesis *types.Header) error {
	slot := x.BlockIntervalSeconds * x.SyncIntervalBlocks
	ref := x.L1Reference
	switch {
	case !genesis.Number.IsZero():
		return fmt.Errorf("%w: rollupX genesis number %d", ErrInvalid, &genesis.Number)
	case genesis.Time < ref.Timestamp || (genesis.Time-ref.Timestamp)%slot != 0:
		return fmt.Errorf("%w: rollupX genesis time %d off the slots of L1 reference time %d", ErrInvalid, genesis.Time, ref.Timestamp)
	case genesis.MixDigest != ref.PrevRandao:
		return fmt.Errorf("%w: rollupX genesis mixHash %x, want L1 reference prevRandao %x", ErrInvalid, genesis.MixDigest, ref.PrevRandao)
	case x.HistoryUpdateOffset != nil && genesis.Time == ref.Timestamp:
		return fmt.Errorf("%w: rollupX v0.1 genesis time %d at its L1 reference", ErrInvalid, genesis.Time)
	}
	return nil
}

type l2JSON struct {
	Name string `json:"name"`
	L2Config
}

// FromJSON decodes and validates an l2 section. Unknown fields and a name
// other than eez are rejected.
func FromJSON(raw []byte) (*L2Config, error) {
	var v l2JSON
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("%w: trailing data", ErrInvalid)
	}
	if v.Name != Name {
		return nil, fmt.Errorf("%w: name %q, want %q", ErrInvalid, v.Name, Name)
	}
	if err := v.L2Config.Validate(); err != nil {
		return nil, err
	}
	return &v.L2Config, nil
}

var registerOnce sync.Once

// Register makes chain configurations resolve their eez l2 section.
func Register() {
	registerOnce.Do(func() {
		chain.RegisterL2Config(Name, func(raw json.RawMessage) (chain.L2Config, error) {
			c, err := FromJSON(raw)
			if err != nil {
				return nil, err
			}
			return c, nil
		})
	})
}

// MarshalJSON encodes the section with its name, as FromJSON reads it.
func (c *L2Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(l2JSON{Name: Name, L2Config: *c})
}
