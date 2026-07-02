package consensus

import (
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/transition/machine"
)

// EngineType identifies a consensus engine implementation.
type EngineType string

const (
	BeaconChainEngineType EngineType = "beacon"     // L1 beacon chain (full Caplin)
	RollupEngineType      EngineType = "rollup"     // L2 based rollup (multi-node)
	DevEngineType         EngineType = "rollup-dev" // L2 dev mode (single node)
)

// FinalityMode describes how a chain achieves finality.
type FinalityMode int

const (
	FinalityCasperFFG FinalityMode = iota // L1: Casper FFG justification/finalization
	FinalityL1Anchor                      // L2: finality derived from L1 anchor blocks
	FinalityInstant                       // Dev: blocks finalize immediately
)

// Engine defines pluggable consensus behavior for the consensus layer,
// analogous to rules.Engine on the execution layer.
//
// The L1 beacon chain and L2 rollup share the same core algorithms
// (fork choice, committee shuffling, attestation processing) but differ
// in block validation rules, data availability requirements, state
// transition operations, and finality mechanism.
//
// Engine is threaded through the fork choice store and fork graph so
// that these shared components can adapt their behavior per chain type.
type Engine interface {
	// Type returns the engine identifier.
	Type() EngineType

	// Machine returns the state transition machine for this engine.
	// Each call returns a fresh instance configured for the requested
	// validation mode. The beacon engine returns eth2.Impl (full L1
	// state transition). A rollup engine returns a machine that skips
	// L1-specific operations (deposits, eth1 data, sync aggregates).
	Machine(fullValidation bool, blockRewardsCollector *eth2.BlockRewardsCollector) machine.Interface

	// ShouldVerifyDataAvailability reports whether the engine requires
	// blob/column data availability checks during block import. L1
	// returns true (PeerDAS); L2 returns false (no sidecars).
	ShouldVerifyDataAvailability() bool

	// FinalityMode returns how this chain achieves finality.
	FinalityMode() FinalityMode
}
