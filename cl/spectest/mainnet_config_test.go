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

package spectest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/erigontech/erigon/cl/clparams"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

type configValueCount struct {
	node  *yaml.Node
	count int
}

func testMainnetConfig(t *testing.T) {
	reference, err := readMainnetConfigReference(os.DirFS(mainnetDir))
	if err != nil {
		t.Fatalf("read mainnet config fixtures: %v", err)
	}
	referenceYAML, err := yaml.Marshal(reference)
	if err != nil {
		t.Fatalf("marshal mainnet config reference: %v", err)
	}

	builtInBeacon := clparams.MainnetBeaconConfig
	builtInNetwork := clparams.NetworkConfigs[chainspec.MainnetChainID]
	specBeacon := builtInBeacon
	specNetwork := builtInNetwork
	if err := yaml.Unmarshal(referenceYAML, &specBeacon); err != nil {
		t.Fatalf("decode mainnet beacon config reference: %v", err)
	}
	if err := yaml.Unmarshal(referenceYAML, &specNetwork); err != nil {
		t.Fatalf("decode mainnet network config reference: %v", err)
	}

	matched := make(map[string]struct{})
	problems := compareConfigFields(reference, builtInBeacon, specBeacon, matched)
	problems = append(problems, compareConfigFields(reference, builtInNetwork, specNetwork, matched)...)

	hardcoded := map[string]any{
		"ATTESTATION_DUE_BPS_GLOAS":      clparams.AttestationDueBpsGloas,
		"AGGREGATE_DUE_BPS_GLOAS":        clparams.AggregateDueBpsGloas,
		"PAYLOAD_ATTESTATION_DUE_BPS":    clparams.PayloadAttestationDueBps,
		"REORG_HEAD_WEIGHT_THRESHOLD":    clparams.ReorgHeadWeightThreshold,
		"REORG_PARENT_WEIGHT_THRESHOLD":  clparams.ReorgParentWeightThreshold,
		"SLOT_DURATION_MS":               builtInBeacon.SecondsPerSlot * 1000,
		"ATTESTATION_DUE_BPS":            clparams.BpsFactor / builtInBeacon.IntervalsPerSlot,
		"EPOCHS_PER_SUBNET_SUBSCRIPTION": builtInBeacon.EpochsPerRandomSubnetSubscription,
		"MAXIMUM_GOSSIP_CLOCK_DISPARITY": time.Duration(builtInNetwork.MaximumGossipClockDisparity).Milliseconds(),
		"MESSAGE_DOMAIN_INVALID_SNAPPY":  binary.BigEndian.Uint32(builtInNetwork.MessageDomainInvalidSnappy[:]),
		"MESSAGE_DOMAIN_VALID_SNAPPY":    binary.BigEndian.Uint32(builtInNetwork.MessageDomainValidSnappy[:]),
	}
	notCompared := map[string]struct{}{
		"AGGREGATE_DUE_BPS":                   {},
		"SYNC_MESSAGE_DUE_BPS":                {},
		"CONTRIBUTION_DUE_BPS":                {},
		"SYNC_MESSAGE_DUE_BPS_GLOAS":          {},
		"CONTRIBUTION_DUE_BPS_GLOAS":          {},
		"PROPOSER_REORG_CUTOFF_BPS":           {},
		"REORG_MAX_EPOCHS_SINCE_FINALIZATION": {},
		"CONFIRMATION_BYZANTINE_THRESHOLD":    {},
		"MAX_PAYLOAD_SIZE":                    {}, // Caplin uses 15 MiB: https://github.com/erigontech/erigon/issues/24417
		"ATTESTATION_SUBNET_EXTRA_BITS":       {},
		"GAS_LIMIT_SCHEDULE":                  {},
	}

	for key, specNode := range reference {
		if _, ok := matched[key]; ok {
			continue
		}
		erigonValue, hardcodedOK := hardcoded[key]
		_, notComparedOK := notCompared[key]
		if hardcodedOK && notComparedOK {
			problems = append(problems, fmt.Sprintf("%s: mapped to both hardcoded and notCompared", key))
			continue
		}
		if !hardcodedOK && !notComparedOK {
			var specValue any
			if err := specNode.Decode(&specValue); err != nil {
				problems = append(problems, fmt.Sprintf("%s: decode spec value: %v", key, err))
				continue
			}
			problems = append(problems, fmt.Sprintf("%s: spec %v, not mapped to a Caplin config field, hardcoded or notCompared", key, specValue))
			continue
		}
		if !hardcodedOK {
			continue
		}
		typedSpecValue := reflect.New(reflect.TypeOf(erigonValue))
		if err := specNode.Decode(typedSpecValue.Interface()); err != nil {
			problems = append(problems, fmt.Sprintf("%s: Erigon %v, spec decode: %v", key, erigonValue, err))
			continue
		}
		if specValue := typedSpecValue.Elem().Interface(); !reflect.DeepEqual(erigonValue, specValue) {
			problems = append(problems, fmt.Sprintf("%s: Erigon %v, spec %v", key, erigonValue, specValue))
		}
	}

	if len(problems) > 0 {
		slices.Sort(problems)
		t.Errorf("mainnet config differs from consensus-spec fixtures:\n%s", strings.Join(problems, "\n"))
	}
}

func TestReadMainnetConfigReference(t *testing.T) {
	t.Run("rejects tied values", func(t *testing.T) {
		root := fstest.MapFS{
			"mainnet/gloas/x/y/case_a/config.yaml": {Data: []byte("PAYLOAD_DUE_BPS: 5000\nATTESTATION_DUE_BPS: 3333\n")},
			"mainnet/gloas/x/y/case_b/config.yaml": {Data: []byte("PAYLOAD_DUE_BPS: 7500\nATTESTATION_DUE_BPS: 2500\n")},
		}

		_, err := readMainnetConfigReference(root)
		if err == nil {
			t.Fatal("expected tied config values to fail")
		}
		want := "ambiguous mainnet config values: ATTESTATION_DUE_BPS, PAYLOAD_DUE_BPS"
		if err.Error() != want {
			t.Fatalf("unexpected error: got %q, want %q", err, want)
		}
	})

	t.Run("chooses majority value", func(t *testing.T) {
		root := fstest.MapFS{
			"mainnet/gloas/x/y/case_a/config.yaml": {Data: []byte("PAYLOAD_DUE_BPS: 5000\n")},
			"mainnet/gloas/x/y/case_b/config.yaml": {Data: []byte("PAYLOAD_DUE_BPS: 5000\n")},
			"mainnet/gloas/x/y/case_c/config.yaml": {Data: []byte("PAYLOAD_DUE_BPS: 7500\n")},
		}

		reference, err := readMainnetConfigReference(root)
		if err != nil {
			t.Fatalf("read config reference: %v", err)
		}
		var got uint64
		if err := reference["PAYLOAD_DUE_BPS"].Decode(&got); err != nil {
			t.Fatalf("decode payload due BPS: %v", err)
		}
		if want := uint64(5000); got != want {
			t.Fatalf("unexpected payload due BPS: got %d, want %d", got, want)
		}
	})
}

func readMainnetConfigReference(root fs.FS) (map[string]*yaml.Node, error) {
	values := make(map[string]map[string]*configValueCount)
	configFiles := 0
	err := fs.WalkDir(root, "mainnet", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != "config.yaml" {
			return nil
		}
		parts := strings.Split(name, "/")
		if len(parts) < 3 {
			return nil
		}
		version, err := clparams.StringToClVersion(parts[1])
		if err != nil || version > clparams.GloasVersion {
			return nil
		}

		contents, err := fs.ReadFile(root, name)
		if err != nil {
			return err
		}
		var document yaml.Node
		if err := yaml.Unmarshal(contents, &document); err != nil {
			return fmt.Errorf("decode %s: %w", name, err)
		}
		if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
			return fmt.Errorf("decode %s: expected config mapping", name)
		}
		configFiles++
		config := document.Content[0]
		for i := 0; i < len(config.Content); i += 2 {
			key := config.Content[i].Value
			node := config.Content[i+1]
			if strings.HasSuffix(key, "_FORK_EPOCH") {
				continue
			}
			canonical, err := yaml.Marshal(node)
			if err != nil {
				return fmt.Errorf("encode %s key %s: %w", name, key, err)
			}
			counts := values[key]
			if counts == nil {
				counts = make(map[string]*configValueCount)
			}
			count := counts[string(canonical)]
			if count == nil {
				count = &configValueCount{node: node}
			}
			count.count++
			counts[string(canonical)] = count
			values[key] = counts
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if configFiles == 0 {
		return nil, errors.New("no mainnet config fixtures found")
	}

	reference := make(map[string]*yaml.Node, len(values))
	var tiedKeys []string
	for key, counts := range values {
		var mostCommon *configValueCount
		topCount := 0
		topValues := 0
		// Some fixture cases override config values, so one file does not reliably hold the spec default.
		for _, candidate := range counts {
			if candidate.count > topCount {
				mostCommon = candidate
				topCount = candidate.count
				topValues = 1
			} else if candidate.count == topCount {
				topValues++
			}
		}
		if topValues > 1 {
			tiedKeys = append(tiedKeys, key)
			continue
		}
		reference[key] = mostCommon.node
	}
	if len(tiedKeys) > 0 {
		slices.Sort(tiedKeys)
		return nil, fmt.Errorf("ambiguous mainnet config values: %s", strings.Join(tiedKeys, ", "))
	}
	return reference, nil
}

func compareConfigFields(reference map[string]*yaml.Node, builtIn, spec any, matched map[string]struct{}) []string {
	builtInValue := reflect.ValueOf(builtIn)
	specValue := reflect.ValueOf(spec)
	builtInType := builtInValue.Type()
	var problems []string
	for i := 0; i < builtInType.NumField(); i++ {
		key, _, _ := strings.Cut(builtInType.Field(i).Tag.Get("yaml"), ",")
		if key == "" || key == "-" {
			continue
		}
		if _, ok := reference[key]; !ok {
			continue
		}
		matched[key] = struct{}{}
		erigonField := builtInValue.Field(i).Interface()
		specField := specValue.Field(i).Interface()
		equal := reflect.DeepEqual(erigonField, specField)
		if erigonString, ok := erigonField.(string); ok {
			specString, ok := specField.(string)
			equal = ok && strings.EqualFold(erigonString, specString)
		}
		if !equal {
			problems = append(problems, fmt.Sprintf("%s: Erigon %v, spec %v", key, erigonField, specField))
		}
	}
	return problems
}
