package consensus_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/consensus"
	"github.com/erigontech/erigon/cl/consensus/beacon"
	"github.com/erigontech/erigon/cl/consensus/rollup"
	"github.com/erigontech/erigon/cl/transition/machine"
)

func TestBeaconEngine(t *testing.T) {
	e := &beacon.Engine{}
	if e.Type() != consensus.BeaconChainEngineType {
		t.Fatalf("expected %s, got %s", consensus.BeaconChainEngineType, e.Type())
	}
	if !e.ShouldVerifyDataAvailability() {
		t.Fatal("beacon engine should verify data availability")
	}
	if e.FinalityMode() != consensus.FinalityCasperFFG {
		t.Fatalf("expected FinalityCasperFFG, got %d", e.FinalityMode())
	}
	m := e.Machine(false, nil)
	if m == nil {
		t.Fatal("Machine() returned nil")
	}
	var _ machine.Interface = m
}

func TestRollupEngine(t *testing.T) {
	e := rollup.NewEngine()
	if e.Type() != consensus.RollupEngineType {
		t.Fatalf("expected %s, got %s", consensus.RollupEngineType, e.Type())
	}
	if e.ShouldVerifyDataAvailability() {
		t.Fatal("rollup engine should not verify data availability")
	}
	if e.FinalityMode() != consensus.FinalityL1Anchor {
		t.Fatalf("expected FinalityL1Anchor, got %d", e.FinalityMode())
	}
	m := e.Machine(true, nil)
	if m == nil {
		t.Fatal("Machine() returned nil")
	}
	var _ machine.Interface = m
}

func TestDevEngine(t *testing.T) {
	e := rollup.NewDevEngine()
	if e.Type() != consensus.DevEngineType {
		t.Fatalf("expected %s, got %s", consensus.DevEngineType, e.Type())
	}
	if e.ShouldVerifyDataAvailability() {
		t.Fatal("dev engine should not verify data availability")
	}
	if e.FinalityMode() != consensus.FinalityInstant {
		t.Fatalf("expected FinalityInstant, got %d", e.FinalityMode())
	}
}
