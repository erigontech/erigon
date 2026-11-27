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

package clparams

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/db/snaptype"
)

type CaplinPruneMode uint8

const (
	CaplinPruneModeSpec CaplinPruneMode = iota
	CaplinPruneModeArchive
	CaplinPruneModePruned
	CaplinPruneModeMinimal
	CaplinPruneModeTip
)

func (m CaplinPruneMode) String() string {
	switch m {
	case CaplinPruneModeArchive:
		return "archive"
	case CaplinPruneModePruned:
		return "pruned"
	case CaplinPruneModeSpec:
		return "spec"
	case CaplinPruneModeMinimal:
		return "minimal"
	case CaplinPruneModeTip:
		return "tip"
	default:
		return "unknown"
	}
}

func ParseCaplinPruneMode(value string) (CaplinPruneMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "archive":
		return CaplinPruneModeArchive, nil
	case "pruned":
		return CaplinPruneModePruned, nil
	case "spec", "":
		return CaplinPruneModeSpec, nil
	case "minimal":
		return CaplinPruneModeMinimal, fmt.Errorf("caplin prune mode minimal is available in a later version")
	case "tip":
		return CaplinPruneModeTip, fmt.Errorf("caplin prune mode tip is available in a later version")
	default:
		return 0, fmt.Errorf("invalid --caplin.prune.mode value %q: expected archive, pruned, spec, minimal or tip", value)
	}
}

type CaplinFloorKind uint8

const (
	CaplinFloorDistance CaplinFloorKind = iota
	CaplinFloorSlot
	CaplinFloorAll
)

type CaplinRetentionFloor struct {
	Kind  CaplinFloorKind
	Value uint64
}

func CaplinDistanceFloor(epochs uint64) CaplinRetentionFloor {
	return CaplinRetentionFloor{Kind: CaplinFloorDistance, Value: epochs}
}

func CaplinSlotFloor(slot uint64) CaplinRetentionFloor {
	return CaplinRetentionFloor{Kind: CaplinFloorSlot, Value: slot}
}

func CaplinAllFloor() CaplinRetentionFloor {
	return CaplinRetentionFloor{Kind: CaplinFloorAll}
}

func (f CaplinRetentionFloor) String() string {
	switch f.Kind {
	case CaplinFloorDistance:
		return strconv.FormatUint(f.Value, 10) + " epochs"
	case CaplinFloorSlot:
		return "slot " + strconv.FormatUint(f.Value, 10)
	case CaplinFloorAll:
		return "all"
	default:
		return "unknown"
	}
}

type CaplinRetention struct {
	Mode     CaplinPruneMode
	Blocks   CaplinRetentionFloor
	Sidecars CaplinRetentionFloor
	States   CaplinRetentionFloor
}

func (r CaplinRetention) String() string {
	return fmt.Sprintf("blocks=%s sidecars=%s states=%s", r.Blocks, r.Sidecars, r.States)
}

type CaplinRetentionFloors struct {
	Blocks   uint64 `json:"blocks"`
	Sidecars uint64 `json:"sidecars"`
	States   uint64 `json:"states"`
}

func (f CaplinRetentionFloors) String() string {
	return fmt.Sprintf("blocks=%d sidecars=%d states=%d", f.Blocks, f.Sidecars, f.States)
}

type CaplinRetentionSource uint8

const (
	CaplinRetentionSourceDefault CaplinRetentionSource = iota
	CaplinRetentionSourceNewFlags
	CaplinRetentionSourceLegacyExact
	CaplinRetentionSourceLegacyInexact
)

func (s CaplinRetentionSource) String() string {
	switch s {
	case CaplinRetentionSourceDefault:
		return "default"
	case CaplinRetentionSourceNewFlags:
		return "new flags"
	case CaplinRetentionSourceLegacyExact:
		return "legacy exact"
	case CaplinRetentionSourceLegacyInexact:
		return "legacy inexact"
	default:
		return "unknown"
	}
}

type StoredSlot struct {
	Slot  uint64
	Holds bool
}

type OldestStoredResult struct {
	Blocks      StoredSlot
	Sidecars    StoredSlot
	States      StoredSlot
	StateTables map[string]StoredSlot
}

func (r OldestStoredResult) HoldsData() bool {
	if r.Blocks.Holds || r.Sidecars.Holds || r.States.Holds {
		return true
	}
	for _, stored := range r.StateTables {
		if stored.Holds {
			return true
		}
	}
	return false
}

type CaplinRetentionAction uint8

const (
	CaplinRetentionActionStore CaplinRetentionAction = iota
	CaplinRetentionActionKeep
	CaplinRetentionActionRefuse
)

func (a CaplinRetentionAction) String() string {
	switch a {
	case CaplinRetentionActionStore:
		return "store"
	case CaplinRetentionActionKeep:
		return "keep"
	case CaplinRetentionActionRefuse:
		return "refuse"
	default:
		return "unknown"
	}
}

func ParseCaplinRetention(mode, distance, sidecars, states string, set ...bool) (CaplinRetention, error) {
	if len(set) != 0 && len(set) != 3 {
		return CaplinRetention{}, fmt.Errorf("invalid Caplin retention flag state")
	}
	pruneMode, err := ParseCaplinPruneMode(mode)
	if err != nil {
		return CaplinRetention{}, err
	}
	values := []struct {
		name  string
		value string
	}{
		{name: "distance", value: distance},
		{name: "sidecars", value: sidecars},
		{name: "states", value: states},
	}
	retention := CaplinRetention{Mode: pruneMode}
	knobSet := func(index int, value string) bool {
		if len(set) == 3 {
			return set[index]
		}
		return strings.TrimSpace(value) != ""
	}
	if pruneMode != CaplinPruneModePruned {
		for index, knob := range values {
			if knobSet(index, knob.value) {
				return CaplinRetention{}, fmt.Errorf("--caplin.prune.%s can only be used with --caplin.prune.mode=pruned", knob.name)
			}
		}
		return retention, nil
	}
	retention.Blocks, err = parseCaplinFloor(distance, "distance")
	if err != nil {
		return CaplinRetention{}, err
	}
	retention.Sidecars, err = parseCaplinFloor(sidecars, "sidecars")
	if err != nil {
		return CaplinRetention{}, err
	}
	retention.States, err = parseCaplinFloor(states, "states")
	if err != nil {
		return CaplinRetention{}, err
	}
	return retention, nil
}

func ResolveRetention(stored CaplinRetention, storedFound bool, requested CaplinRetention, source CaplinRetentionSource, oldest OldestStoredResult, currentSlot uint64, beaconCfg *BeaconChainConfig, nodeBinary, datadir string) (CaplinRetention, CaplinRetentionAction, error) {
	normalizedRequested, err := requested.Normalize(beaconCfg)
	if err != nil {
		return CaplinRetention{}, CaplinRetentionActionRefuse, err
	}
	if source < CaplinRetentionSourceDefault || source > CaplinRetentionSourceLegacyInexact {
		return CaplinRetention{}, CaplinRetentionActionRefuse, fmt.Errorf("invalid Caplin retention source: %d", source)
	}
	if storedFound {
		normalizedStored, err := stored.Normalize(beaconCfg)
		if err != nil {
			return CaplinRetention{}, CaplinRetentionActionRefuse, err
		}
		if source == CaplinRetentionSourceDefault || source == CaplinRetentionSourceLegacyInexact {
			return normalizedStored, CaplinRetentionActionKeep, nil
		}
		if normalizedRequested == normalizedStored {
			return normalizedStored, CaplinRetentionActionKeep, nil
		}
		requestedFloors, err := normalizedRequested.Floors(beaconCfg, currentSlot)
		if err != nil {
			return CaplinRetention{}, CaplinRetentionActionRefuse, err
		}
		storedFloors, err := normalizedStored.Floors(beaconCfg, currentSlot)
		if err != nil {
			return CaplinRetention{}, CaplinRetentionActionRefuse, err
		}
		if retentionFloorBelow(requestedFloors, storedFloors) {
			return CaplinRetention{}, CaplinRetentionActionRefuse, fmt.Errorf("caplin retention request moves away from the stored regime: %s", CaplinRetentionHint(normalizedRequested, oldest, currentSlot, beaconCfg, nodeBinary, datadir))
		}
		return normalizedRequested, CaplinRetentionActionStore, nil
	}
	if source == CaplinRetentionSourceLegacyInexact {
		if !oldest.HoldsData() {
			return CaplinRetention{}, CaplinRetentionActionRefuse, fmt.Errorf("caplin retention cannot infer an inexact legacy regime on an empty datadir: %s", CaplinRetentionHint(normalizedRequested, oldest, currentSlot, beaconCfg, nodeBinary, datadir))
		}
		inferred := CaplinRetention{
			Mode:     CaplinPruneModePruned,
			Blocks:   caplinStoredFloor(oldest.Blocks),
			Sidecars: caplinStoredFloor(oldest.Sidecars),
			States:   caplinStoredFloor(oldest.States),
		}
		inferred, err = inferred.Normalize(beaconCfg)
		if err != nil {
			return CaplinRetention{}, CaplinRetentionActionRefuse, err
		}
		if _, err := inferred.Floors(beaconCfg, currentSlot); err != nil {
			return CaplinRetention{}, CaplinRetentionActionRefuse, err
		}
		return inferred, CaplinRetentionActionStore, nil
	}
	if _, err := normalizedRequested.Floors(beaconCfg, currentSlot); err != nil {
		return CaplinRetention{}, CaplinRetentionActionRefuse, err
	}
	return normalizedRequested, CaplinRetentionActionStore, nil
}

func retentionFloorBelow(requested, stored CaplinRetentionFloors) bool {
	return requested.Blocks < stored.Blocks || requested.Sidecars < stored.Sidecars || requested.States < stored.States
}

func caplinStoredFloor(stored StoredSlot) CaplinRetentionFloor {
	if !stored.Holds {
		return CaplinDistanceFloor(0)
	}
	return CaplinSlotFloor(stored.Slot)
}

func CaplinRetentionHint(retention CaplinRetention, oldest OldestStoredResult, currentSlot uint64, beaconCfg *BeaconChainConfig, nodeBinary, datadir string) string {
	setArgs := []string{"capcli", "seg", "regime", "set", retention.Mode.String()}
	if retention.Mode == CaplinPruneModePruned {
		setArgs = append(
			setArgs,
			"--distance="+retentionFloorArgument(retention.Blocks),
			"--sidecars="+retentionFloorArgument(retention.Sidecars),
			"--states="+retentionFloorArgument(retention.States),
		)
	}
	setArgs = append(setArgs, "--datadir="+datadir)
	commands := []string{strings.Join(setArgs, " ")}
	if caplinRetentionNeedsReset(retention, oldest, currentSlot, beaconCfg) {
		commands = append(commands, "capcli seg reset --datadir="+datadir)
	}
	nodeFlagsExact := caplinRetentionNodeFlagsExact(retention, beaconCfg)
	startArgs := []string{nodeBinary, "--datadir=" + datadir}
	if nodeFlagsExact {
		startArgs = append(startArgs, "--caplin.prune.mode="+retention.Mode.String())
	}
	if nodeFlagsExact && retention.Mode == CaplinPruneModePruned {
		startArgs = append(
			startArgs,
			"--caplin.prune.distance="+retentionFloorArgument(retention.Blocks),
			"--caplin.prune.sidecars="+retentionFloorArgument(retention.Sidecars),
			"--caplin.prune.states="+retentionFloorArgument(retention.States),
		)
	}
	commands = append(commands, strings.Join(startArgs, " "))
	return strings.Join(commands, "; ")
}

func retentionFloorArgument(floor CaplinRetentionFloor) string {
	switch floor.Kind {
	case CaplinFloorDistance:
		return strconv.FormatUint(floor.Value, 10)
	case CaplinFloorSlot:
		return "slot:" + strconv.FormatUint(floor.Value, 10)
	case CaplinFloorAll:
		return "all"
	default:
		return "0"
	}
}

func caplinRetentionNeedsReset(retention CaplinRetention, oldest OldestStoredResult, currentSlot uint64, beaconCfg *BeaconChainConfig) bool {
	normalized, err := retention.Normalize(beaconCfg)
	if err != nil {
		return false
	}
	if beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 {
		return retentionFloorNeedsData(normalized.Blocks) && !oldest.Blocks.Holds ||
			retentionFloorNeedsData(normalized.Sidecars) && !oldest.Sidecars.Holds ||
			retentionFloorNeedsData(normalized.States) && !oldest.States.Holds
	}
	floors, err := normalized.Floors(beaconCfg, currentSlot)
	if err != nil {
		return false
	}
	return retentionFamilyNeedsReset(floors.Blocks, normalized.Blocks, oldest.Blocks, true) ||
		retentionFamilyNeedsReset(floors.Sidecars, normalized.Sidecars, oldest.Sidecars, true) ||
		retentionFamilyNeedsReset(floors.States, normalized.States, oldest.States, normalized.Mode != CaplinPruneModeSpec)
}

func retentionFamilyNeedsReset(floor uint64, knob CaplinRetentionFloor, oldest StoredSlot, keepsState bool) bool {
	if oldest.Holds {
		return floor < oldest.Slot
	}
	return keepsState && retentionFloorNeedsData(knob)
}

func caplinRetentionNodeFlagsExact(retention CaplinRetention, beaconCfg *BeaconChainConfig) bool {
	if beaconCfg == nil {
		return retention.Mode != CaplinPruneModePruned || retention.Blocks.Kind != CaplinFloorSlot && retention.Sidecars.Kind != CaplinFloorSlot && retention.States.Kind != CaplinFloorSlot
	}
	for _, floor := range []CaplinRetentionFloor{retention.Blocks, retention.Sidecars, retention.States} {
		if floor.Kind == CaplinFloorSlot {
			return false
		}
	}
	var cfg CaplinConfig
	if err := ApplyRetention(retention, beaconCfg, &cfg); err != nil {
		return false
	}
	mapped, exact, err := LegacyRetention(cfg, beaconCfg)
	return err == nil && exact && mapped == retention
}

func retentionFloorNeedsData(floor CaplinRetentionFloor) bool {
	return floor.Kind == CaplinFloorAll || floor.Kind == CaplinFloorSlot || floor.Value > 0
}

func (r CaplinRetention) Normalize(beaconCfg *BeaconChainConfig) (CaplinRetention, error) {
	switch r.Mode {
	case CaplinPruneModeArchive:
		return CaplinRetention{Mode: CaplinPruneModeArchive, Blocks: CaplinAllFloor(), Sidecars: CaplinAllFloor(), States: CaplinAllFloor()}, nil
	case CaplinPruneModeMinimal, CaplinPruneModeTip:
		return r, nil
	case CaplinPruneModeSpec, CaplinPruneModePruned:
		blockMin, sidecarMin, err := caplinRetentionMinimums(beaconCfg)
		if err != nil {
			return CaplinRetention{}, err
		}
		if r.Mode == CaplinPruneModeSpec {
			return CaplinRetention{
				Mode:     CaplinPruneModeSpec,
				Blocks:   CaplinDistanceFloor(blockMin),
				Sidecars: CaplinDistanceFloor(sidecarMin),
				States:   CaplinDistanceFloor(0),
			}, nil
		}
		normalized := CaplinRetention{
			Mode:     CaplinPruneModePruned,
			Blocks:   normalizeRetentionFloor(r.Blocks, blockMin),
			Sidecars: normalizeRetentionFloor(r.Sidecars, sidecarMin),
			States:   normalizeRetentionFloor(r.States, 0),
		}
		if normalized.Blocks.Kind == CaplinFloorAll && normalized.Sidecars.Kind == CaplinFloorAll && normalized.States.Kind == CaplinFloorAll {
			return CaplinRetention{Mode: CaplinPruneModeArchive, Blocks: CaplinAllFloor(), Sidecars: CaplinAllFloor(), States: CaplinAllFloor()}, nil
		}
		spec := CaplinRetention{
			Mode:     CaplinPruneModeSpec,
			Blocks:   CaplinDistanceFloor(blockMin),
			Sidecars: CaplinDistanceFloor(sidecarMin),
			States:   CaplinDistanceFloor(0),
		}
		if normalized.Blocks == spec.Blocks && normalized.Sidecars == spec.Sidecars && normalized.States == spec.States {
			return spec, nil
		}
		return normalized, nil
	default:
		return CaplinRetention{}, fmt.Errorf("invalid caplin prune mode %d", r.Mode)
	}
}

func normalizeRetentionFloor(floor CaplinRetentionFloor, minimum uint64) CaplinRetentionFloor {
	if floor.Kind == CaplinFloorDistance && floor.Value == 0 {
		floor.Value = minimum
	}
	if floor.Kind == CaplinFloorSlot && floor.Value == 0 {
		floor.Kind = CaplinFloorAll
	}
	return floor
}

func caplinRetentionMinimums(beaconCfg *BeaconChainConfig) (uint64, uint64, error) {
	if beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 {
		return 0, 0, fmt.Errorf("beacon chain config has no slots per epoch")
	}
	return caplinBlockRetentionMinimum(beaconCfg), caplinSidecarRetentionMinimum(beaconCfg), nil
}

func caplinSidecarRetentionMinimum(beaconCfg *BeaconChainConfig) uint64 {
	return max(beaconCfg.MinEpochsForBlobSidecarsRequests, beaconCfg.MinEpochsForDataColumnSidecarsRequests)
}

func caplinBlockRetentionMinimum(beaconCfg *BeaconChainConfig) uint64 {
	return max(beaconCfg.MinEpochsForBlockRequests(), caplinSidecarRetentionMinimum(beaconCfg))
}

func parseCaplinFloor(value, name string) (CaplinRetentionFloor, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return CaplinDistanceFloor(0), nil
	}
	if value == "all" {
		return CaplinAllFloor(), nil
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return CaplinRetentionFloor{}, fmt.Errorf("invalid --caplin.prune.%s value %q: expected an epoch count or all", name, value)
	}
	if n == 0 && name != "states" {
		return CaplinRetentionFloor{}, fmt.Errorf("--caplin.prune.%s must be at least the chain's spec window", name)
	}
	return CaplinDistanceFloor(n), nil
}

func (r CaplinRetention) Floors(beaconCfg *BeaconChainConfig, headSlot uint64) (CaplinRetentionFloors, error) {
	normalized, err := r.Normalize(beaconCfg)
	if err != nil {
		return CaplinRetentionFloors{}, err
	}
	r = normalized
	if r.Mode == CaplinPruneModeMinimal || r.Mode == CaplinPruneModeTip {
		return CaplinRetentionFloors{}, fmt.Errorf("caplin prune mode %s is available in a later version", r.Mode)
	}
	if r.Mode == CaplinPruneModeArchive {
		return CaplinRetentionFloors{}, nil
	}
	blockMin := caplinBlockRetentionMinimum(beaconCfg)
	sidecarMin := caplinSidecarRetentionMinimum(beaconCfg)
	if err := validateDistance(r.Blocks, blockMin, "--caplin.prune.distance"); err != nil {
		return CaplinRetentionFloors{}, err
	}
	if err := validateDistance(r.Sidecars, sidecarMin, "--caplin.prune.sidecars"); err != nil {
		return CaplinRetentionFloors{}, err
	}
	if err := validateDistance(r.States, 0, "--caplin.prune.states"); err != nil {
		return CaplinRetentionFloors{}, err
	}

	headEpoch := headSlot / beaconCfg.SlotsPerEpoch
	floors := CaplinRetentionFloors{
		Blocks:   floorSlot(r.Blocks, headEpoch, blockMin, beaconCfg.SlotsPerEpoch),
		Sidecars: floorSlot(r.Sidecars, headEpoch, sidecarMin, beaconCfg.SlotsPerEpoch),
		States:   floorSlot(r.States, headEpoch, 0, beaconCfg.SlotsPerEpoch),
	}
	floors.Blocks = roundDown(floors.Blocks, snaptype.CaplinMergeLimit)
	floors.Sidecars = roundDown(floors.Sidecars, snaptype.CaplinMergeLimit)
	floors.States = roundDown(floors.States, SlotsPerDump)
	if floors.Sidecars < floors.Blocks {
		return CaplinRetentionFloors{}, fmt.Errorf("sidecars reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.sidecars")
	}
	if floors.States < floors.Blocks {
		return CaplinRetentionFloors{}, fmt.Errorf("states reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.states")
	}
	return floors, nil
}

func validateDistance(floor CaplinRetentionFloor, minimum uint64, flag string) error {
	if floor.Kind != CaplinFloorDistance || floor.Value == 0 {
		return nil
	}
	if floor.Value < minimum {
		return fmt.Errorf("%s must be at least %d epochs", flag, minimum)
	}
	return nil
}

func floorSlot(floor CaplinRetentionFloor, headEpoch, minimum, slotsPerEpoch uint64) uint64 {
	switch floor.Kind {
	case CaplinFloorAll:
		return 0
	case CaplinFloorSlot:
		return floor.Value
	case CaplinFloorDistance:
		distance := floor.Value
		if distance == 0 && minimum > 0 {
			distance = minimum
		}
		if distance >= headEpoch {
			return 0
		}
		return (headEpoch - distance) * slotsPerEpoch
	default:
		return 0
	}
}

func roundDown(value, unit uint64) uint64 {
	if unit == 0 {
		return value
	}
	return value / unit * unit
}

func LegacyRetention(cfg CaplinConfig, beaconCfg *BeaconChainConfig) (CaplinRetention, bool, error) {
	if beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 {
		return CaplinRetention{}, false, fmt.Errorf("beacon chain config has no slots per epoch")
	}
	blocksAll := cfg.ArchiveBlocks || cfg.ArchiveStates || cfg.ArchiveBlobs || cfg.BlobsArchiveFromSlot > 0
	if cfg.BlobPruningDisabled && !blocksAll {
		return CaplinRetention{}, false, fmt.Errorf("--caplin.blobs-no-pruning requires --caplin.blocks-archive; add --caplin.blocks-archive, or use --caplin.blobs-archive")
	}
	blockMin := caplinBlockRetentionMinimum(beaconCfg)
	sidecarMin := caplinSidecarRetentionMinimum(beaconCfg)
	sidecarsAll := cfg.BlobPruningDisabled || cfg.ArchiveBlobs && cfg.BlobsArchiveFromSlot == 0
	retention := CaplinRetention{Mode: CaplinPruneModePruned, Blocks: CaplinDistanceFloor(blockMin), Sidecars: CaplinDistanceFloor(0)}
	if blocksAll {
		retention.Blocks = CaplinAllFloor()
	}
	switch {
	case cfg.BlobsArchiveFromSlot > 0:
		retention.Sidecars = CaplinSlotFloor(cfg.BlobsArchiveFromSlot)
	case sidecarsAll:
		retention.Sidecars = CaplinAllFloor()
	case cfg.ColumnKeepSlots > 0:
		distance := max(cfg.ColumnKeepSlots/beaconCfg.SlotsPerEpoch, sidecarMin)
		retention.Sidecars = CaplinDistanceFloor(distance)
	}
	if cfg.ArchiveStates {
		retention.States = CaplinAllFloor()
	}
	if blocksAll && sidecarsAll && cfg.ArchiveStates {
		retention.Mode = CaplinPruneModeArchive
		retention.Blocks = CaplinAllFloor()
		retention.Sidecars = CaplinAllFloor()
		retention.States = CaplinAllFloor()
	}
	if !blocksAll && !sidecarsAll && !cfg.ArchiveStates && cfg.ColumnKeepSlots == 0 && cfg.BlobsArchiveFromSlot == 0 {
		retention = CaplinRetention{Mode: CaplinPruneModeSpec}
	}

	mapped := cfg
	normalized, err := retention.Normalize(beaconCfg)
	if err != nil {
		return retention, false, err
	}
	retention = normalized
	if err := ApplyRetention(retention, beaconCfg, &mapped); err != nil {
		return retention, false, nil
	}
	return retention, caplinRetentionReadersEqual(cfg, mapped, beaconCfg), nil
}

func caplinRetentionReadersEqual(a, b CaplinConfig, _ *BeaconChainConfig) bool {
	if a.ArchiveBlocks != b.ArchiveBlocks || a.ArchiveBlobs != b.ArchiveBlobs || a.ArchiveStates != b.ArchiveStates {
		return false
	}
	if a.ColumnKeepSlots != b.ColumnKeepSlots || a.SnapshotBlobsFromSlot() != b.SnapshotBlobsFromSlot() {
		return false
	}
	aRule, aSlot := blobRetentionRule(a)
	bRule, bSlot := blobRetentionRule(b)
	return aRule == bRule && aSlot == bSlot
}

func blobRetentionRule(cfg CaplinConfig) (uint8, uint64) {
	switch {
	case cfg.BlobPruningDisabled || cfg.ArchiveBlobs && cfg.BlobsArchiveFromSlot == 0:
		return 0, 0
	case cfg.ArchiveBlobs:
		return 1, cfg.BlobsArchiveFromSlot
	default:
		return 2, 0
	}
}

func ApplyRetention(r CaplinRetention, beaconCfg *BeaconChainConfig, cfg *CaplinConfig) error {
	if cfg == nil {
		return fmt.Errorf("caplin config is nil")
	}
	normalized, err := r.Normalize(beaconCfg)
	if err != nil {
		return err
	}
	r = normalized
	if r.Mode == CaplinPruneModeMinimal || r.Mode == CaplinPruneModeTip {
		return fmt.Errorf("caplin prune mode %s is available in a later version", r.Mode)
	}
	clearCaplinRetention(cfg)
	switch r.Mode {
	case CaplinPruneModeSpec:
		return nil
	case CaplinPruneModeArchive:
		cfg.ArchiveBlocks = true
		cfg.ArchiveBlobs = true
		cfg.ArchiveStates = true
		return nil
	case CaplinPruneModePruned:
		return applyPrunedRetention(r, beaconCfg, cfg)
	default:
		return fmt.Errorf("invalid caplin prune mode %q", r.Mode)
	}
}

func clearCaplinRetention(cfg *CaplinConfig) {
	cfg.ArchiveBlocks = false
	cfg.ArchiveBlobs = false
	cfg.ArchiveStates = false
	cfg.BlobPruningDisabled = false
	cfg.BlobsArchiveFromSlot = 0
	cfg.ColumnKeepSlots = 0
}

func ApplyCaplinRetentionRestrictions(cfg *CaplinConfig) error {
	if cfg == nil {
		return fmt.Errorf("caplin config is nil")
	}
	if cfg.EnableEngineAPI {
		cfg.ArchiveBlocks = false
		cfg.ArchiveBlobs = false
		cfg.ArchiveStates = false
		cfg.BlobPruningDisabled = false
		cfg.BlobsArchiveFromSlot = 0
		return nil
	}
	if cfg.SnapshotGenerationEnabled && cfg.BlobsArchiveFromSlot > 0 {
		return fmt.Errorf("option caplin.blobs-archive-from-slot cannot be combined with caplin.snapgen: blob snapshots are built contiguously from Deneb")
	}
	return nil
}

func applyPrunedRetention(r CaplinRetention, beaconCfg *BeaconChainConfig, cfg *CaplinConfig) error {
	blockMin := caplinBlockRetentionMinimum(beaconCfg)
	sidecarMin := caplinSidecarRetentionMinimum(beaconCfg)
	blockDistance, blocksAll, err := finiteRetentionDistance(r.Blocks, blockMin, "--caplin.prune.distance")
	if err != nil {
		return err
	}
	if r.Blocks.Kind == CaplinFloorSlot {
		return fmt.Errorf("caplin prune distance is available in a later version")
	}
	sidecarDistance, sidecarsAll, sidecarSlot, err := sidecarRetention(r.Sidecars, sidecarMin, "--caplin.prune.sidecars")
	if err != nil {
		return err
	}
	if err := validateCaplinNesting(blockDistance, blocksAll, r.Sidecars, sidecarDistance, r.States); err != nil {
		return err
	}
	if r.States.Kind == CaplinFloorSlot {
		return fmt.Errorf("caplin prune states are available in a later version")
	}
	if r.States.Kind == CaplinFloorDistance && r.States.Value != 0 {
		return fmt.Errorf("caplin prune states are available in a later version")
	}

	if blocksAll {
		cfg.ArchiveBlocks = true
	}
	switch {
	case sidecarsAll:
		cfg.ArchiveBlobs = true
	case sidecarSlot > 0:
		cfg.ArchiveBlobs = true
		cfg.BlobsArchiveFromSlot = sidecarSlot
	case sidecarDistance > sidecarMin:
		if sidecarDistance > math.MaxUint64/beaconCfg.SlotsPerEpoch {
			return fmt.Errorf("caplin prune sidecars are available in a later version")
		}
		cfg.ColumnKeepSlots = sidecarDistance * beaconCfg.SlotsPerEpoch
	}
	if r.States.Kind == CaplinFloorAll {
		cfg.ArchiveStates = true
	}
	return nil
}

func finiteRetentionDistance(floor CaplinRetentionFloor, minimum uint64, flag string) (uint64, bool, error) {
	if floor.Kind == CaplinFloorAll {
		return 0, true, nil
	}
	if floor.Kind != CaplinFloorDistance {
		return 0, false, nil
	}
	distance := floor.Value
	if distance == 0 {
		distance = minimum
	}
	if distance < minimum {
		return 0, false, fmt.Errorf("%s must be at least %d epochs", flag, minimum)
	}
	if distance > minimum {
		return distance, false, fmt.Errorf("caplin prune %s is available in a later version", strings.TrimPrefix(flag, "--caplin.prune."))
	}
	return distance, false, nil
}

func sidecarRetention(floor CaplinRetentionFloor, minimum uint64, flag string) (uint64, bool, uint64, error) {
	if floor.Kind == CaplinFloorAll {
		return 0, true, 0, nil
	}
	if floor.Kind == CaplinFloorSlot {
		return 0, false, floor.Value, nil
	}
	if floor.Kind != CaplinFloorDistance {
		return 0, false, 0, fmt.Errorf("invalid caplin prune sidecars floor")
	}
	distance := floor.Value
	if distance == 0 {
		distance = minimum
	}
	if distance < minimum {
		return 0, false, 0, fmt.Errorf("%s must be at least %d epochs", flag, minimum)
	}
	return distance, false, 0, nil
}

func validateCaplinNesting(blockDistance uint64, blocksAll bool, sidecar CaplinRetentionFloor, sidecarDistance uint64, states CaplinRetentionFloor) error {
	if blocksAll {
		return nil
	}
	if sidecar.Kind == CaplinFloorAll || sidecar.Kind == CaplinFloorSlot || sidecarDistance > blockDistance {
		return fmt.Errorf("sidecars reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.sidecars")
	}
	if states.Kind == CaplinFloorAll || states.Kind == CaplinFloorSlot || states.Kind == CaplinFloorDistance && states.Value > blockDistance {
		return fmt.Errorf("states reach below blocks: raise --caplin.prune.distance or lower --caplin.prune.states")
	}
	return nil
}
