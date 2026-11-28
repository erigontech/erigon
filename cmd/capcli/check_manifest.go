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

package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
)

type caplinManifestGroup struct {
	Family      string `json:"family"`
	Table       string `json:"table,omitempty"`
	Files       int    `json:"files"`
	LargestFile string `json:"largest_file"`
	LargestSpan uint64 `json:"largest_span"`
	ranges      []snapshotsync.CaplinRange
}

type caplinManifestGroupKey struct {
	family snapshotsync.CaplinFileFamily
	table  string
}

type caplinManifestRange struct {
	Family string `json:"family"`
	Table  string `json:"table,omitempty"`
	From   uint64 `json:"from"`
	To     uint64 `json:"to"`
}

type caplinManifestStateDifference struct {
	Table          string   `json:"table"`
	End            uint64   `json:"end"`
	CommonEnd      uint64   `json:"common_end"`
	Boundaries     []string `json:"boundaries"`
	CommonBoundary []string `json:"common_boundaries"`
}

type caplinManifestReport struct {
	Groups                []caplinManifestGroup           `json:"groups"`
	Gaps                  []caplinManifestRange           `json:"gaps"`
	Overlaps              []caplinManifestRange           `json:"overlaps"`
	Malformed             []caplinManifestRange           `json:"malformed"`
	StateTableDifferences []caplinManifestStateDifference `json:"state_table_differences"`
	Unregistered          []string                        `json:"unregistered"`
}

func caplinCheckManifestCommand() *cli.Command {
	return &cli.Command{
		Name:        "check-manifest",
		Usage:       "check a Caplin snapshot manifest; reads the TOML only without the datadir lock",
		Description: "Reads and checks the supplied Caplin manifest for gaps, overlaps, state-table differences, and unregistered files. It does not open or change a datadir and does not take a lock.",
		ArgsUsage:   "<toml>",
		Flags:       []cli.Flag{&cli.BoolFlag{Name: "json", Usage: "write JSON"}},
		Action:      runCaplinCheckManifest,
	}
}

func runCaplinCheckManifest(_ context.Context, cmd *cli.Command) error {
	if cmd.Args().Len() != 1 {
		return errors.New("caplin seg check-manifest requires one TOML file")
	}
	path := cmd.Args().First()
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	items, err := snapcfg.ParseToml(data)
	if err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}
	report := checkCaplinManifest(items)
	writer := cmd.Writer
	if cmd.Bool("json") {
		if err := json.NewEncoder(writer).Encode(report); err != nil {
			return err
		}
	} else {
		formatCaplinManifestReport(writer, path, report)
	}
	if report.invalid() {
		return errors.New("manifest has gaps or inconsistent state table ends")
	}
	return nil
}

func checkCaplinManifest(items snapcfg.PreverifiedItems) caplinManifestReport {
	report := caplinManifestReport{
		Groups:                make([]caplinManifestGroup, 0),
		Gaps:                  make([]caplinManifestRange, 0),
		Overlaps:              make([]caplinManifestRange, 0),
		Malformed:             make([]caplinManifestRange, 0),
		StateTableDifferences: make([]caplinManifestStateDifference, 0),
		Unregistered:          make([]string, 0),
	}
	groups := make(map[caplinManifestGroupKey]*caplinManifestGroup)
	for _, item := range items {
		if strings.HasSuffix(item.Name, ".torrent") {
			continue
		}
		name := item.Name
		info, _, ok := snaptype.ParseFileName("", name)
		if !ok {
			report.Unregistered = append(report.Unregistered, item.Name)
			continue
		}
		if info.Ext != ".seg" && info.Ext != ".idx" {
			continue
		}
		file, ok := snapshotsync.CaplinFileInfo(name, info)
		if !ok {
			if info.Type == nil {
				report.Unregistered = append(report.Unregistered, item.Name)
			}
			continue
		}
		family := caplinManifestFamily(file.Family)
		table := ""
		if file.Family == snapshotsync.CaplinStates {
			table = info.Type.Name()
		}
		if file.From >= file.To {
			report.Malformed = append(report.Malformed, caplinManifestRange{Family: family, Table: table, From: file.From, To: file.To})
			continue
		}
		key := caplinManifestGroupKey{family: file.Family, table: table}
		group := groups[key]
		if group == nil {
			group = &caplinManifestGroup{Family: family, Table: table}
			groups[key] = group
		}
		group.Files++
		if info.Ext == ".seg" {
			group.ranges = append(group.ranges, snapshotsync.CaplinRange{From: file.From, To: file.To})
			span := file.To - file.From
			if group.LargestFile == "" || span > group.LargestSpan || span == group.LargestSpan && item.Name < group.LargestFile {
				group.LargestFile = item.Name
				group.LargestSpan = span
			}
		} else if group.LargestFile == "" {
			group.LargestFile = item.Name
		}
	}
	slices.SortFunc(report.Unregistered, cmp.Compare)

	groupKeys := slices.SortedFunc(maps.Keys(groups), func(a, b caplinManifestGroupKey) int {
		return cmp.Or(cmp.Compare(a.family, b.family), cmp.Compare(a.table, b.table))
	})
	stateGroups := make([]caplinManifestGroup, 0)
	for _, key := range groupKeys {
		group := *groups[key]
		scan := snapshotsync.ScanCaplinRanges(group.ranges)
		for _, gap := range scan.Gaps {
			report.Gaps = append(report.Gaps, caplinManifestRange{Family: group.Family, Table: group.Table, From: gap.From, To: gap.To})
		}
		for _, overlap := range scan.Overlaps {
			report.Overlaps = append(report.Overlaps, caplinManifestRange{Family: group.Family, Table: group.Table, From: overlap.From, To: overlap.To})
		}
		if group.Family == "beacon_state" {
			stateGroups = append(stateGroups, group)
		}
		report.Groups = append(report.Groups, group)
	}
	stateDifferences(&report, stateGroups)
	return report
}

func caplinManifestFamily(family snapshotsync.CaplinFileFamily) string {
	switch family {
	case snapshotsync.CaplinBlocks:
		return "beacon_blocks"
	case snapshotsync.CaplinSidecars:
		return "blob_sidecars"
	default:
		return "beacon_state"
	}
}

func stateDifferences(report *caplinManifestReport, groups []caplinManifestGroup) {
	if len(groups) < 2 {
		return
	}
	ends := make(map[uint64]int)
	groupEnds := make(map[string]uint64, len(groups))
	groupBoundaries := make(map[string][]string, len(groups))
	for _, group := range groups {
		scan := snapshotsync.ScanCaplinRanges(group.ranges)
		parts := make([]string, len(scan.Covered))
		for i, item := range scan.Covered {
			parts[i] = fmt.Sprintf("%d..%d", item.From, item.To)
		}
		end := uint64(0)
		for _, item := range scan.Covered {
			end = max(end, item.To)
		}
		ends[end]++
		groupEnds[group.Table] = end
		groupBoundaries[group.Table] = parts
	}
	commonEnd := uint64(0)
	commonEndCount := 0
	for end, count := range ends {
		if count > commonEndCount || count == commonEndCount && end > commonEnd {
			commonEnd = end
			commonEndCount = count
		}
	}
	signatures := make(map[string]int)
	var commonBoundary string
	commonParts := []string{}
	commonCount := 0
	commonSet := false
	for _, group := range groups {
		if groupEnds[group.Table] != commonEnd {
			continue
		}
		value := strings.Join(groupBoundaries[group.Table], ",")
		signatures[value]++
		count := signatures[value]
		if !commonSet || count > commonCount || count == commonCount && value < commonBoundary {
			commonBoundary, commonCount, commonParts, commonSet = value, count, groupBoundaries[group.Table], true
		}
	}
	for _, group := range groups {
		end := groupEnds[group.Table]
		boundaries := groupBoundaries[group.Table]
		if end == commonEnd && strings.Join(boundaries, ",") == commonBoundary {
			continue
		}
		parts := boundaries
		report.StateTableDifferences = append(report.StateTableDifferences, caplinManifestStateDifference{
			Table:          group.Table,
			End:            end,
			CommonEnd:      commonEnd,
			Boundaries:     parts,
			CommonBoundary: commonParts,
		})
	}
	slices.SortFunc(report.StateTableDifferences, func(a, b caplinManifestStateDifference) int { return cmp.Compare(a.Table, b.Table) })
}

func (r caplinManifestReport) invalid() bool {
	if len(r.Gaps) != 0 || len(r.Malformed) != 0 {
		return true
	}
	for _, difference := range r.StateTableDifferences {
		if difference.End != difference.CommonEnd {
			return true
		}
	}
	return false
}

func formatCaplinManifestReport(writer io.Writer, path string, report caplinManifestReport) {
	fmt.Fprintf(writer, "manifest %s\n", path)
	fmt.Fprintln(writer, "family        table      files largest span")
	for _, group := range report.Groups {
		fmt.Fprintf(writer, "%-13s %-10s %d     %s %d\n", group.Family, cmp.Or(group.Table, "-"), group.Files, group.LargestFile, group.LargestSpan)
	}
	formatCaplinManifestRanges(writer, "gaps", report.Gaps)
	formatCaplinManifestRanges(writer, "overlaps", report.Overlaps)
	if len(report.Malformed) != 0 {
		formatCaplinManifestRanges(writer, "malformed", report.Malformed)
	}
	fmt.Fprintln(writer, "state table differences")
	if len(report.StateTableDifferences) == 0 {
		fmt.Fprintln(writer, "  none")
	} else {
		for _, difference := range report.StateTableDifferences {
			fmt.Fprintf(writer, "  %s end %d, common end %d; boundaries %s, common %s\n", difference.Table, difference.End, difference.CommonEnd, strings.Join(difference.Boundaries, ","), strings.Join(difference.CommonBoundary, ","))
		}
	}
	fmt.Fprintln(writer, "unregistered")
	if len(report.Unregistered) == 0 {
		fmt.Fprintln(writer, "  none")
	} else {
		for _, name := range report.Unregistered {
			fmt.Fprintf(writer, "  %s\n", name)
		}
	}
	if report.invalid() {
		fmt.Fprintln(writer, "invalid")
	} else {
		fmt.Fprintln(writer, "valid")
	}
}

func formatCaplinManifestRanges(writer io.Writer, label string, ranges []caplinManifestRange) {
	fmt.Fprintln(writer, label)
	if len(ranges) == 0 {
		fmt.Fprintln(writer, "  none")
		return
	}
	for _, item := range ranges {
		if item.Table == "" {
			fmt.Fprintf(writer, "  %s %d..%d\n", item.Family, item.From, item.To)
		} else {
			fmt.Fprintf(writer, "  %s %s %d..%d\n", item.Family, item.Table, item.From, item.To)
		}
	}
}
