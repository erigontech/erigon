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
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/erigontech/erigon/cl/clparams"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

type configValueCount struct {
	value any
	node  yaml.Node
	count int
}

func testMainnetConfig(t *testing.T) {
	t.Helper()

	reference, referenceNodes := readMainnetConfigReference(t, os.DirFS(mainnetDir))
	referenceYAML, err := yaml.Marshal(referenceNodes)
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
		"MAX_PAYLOAD_SIZE":                    {},
		"ATTESTATION_SUBNET_EXTRA_BITS":       {},
		"GAS_LIMIT_SCHEDULE":                  {},
	}

	for key, specValue := range reference {
		if _, ok := matched[key]; ok {
			continue
		}
		erigonValue, hardcodedOK := hardcoded[key]
		_, notComparedOK := notCompared[key]
		if hardcodedOK == notComparedOK {
			problems = append(problems, fmt.Sprintf("%s: Erigon %v, spec %v", key, "<unknown>", specValue))
			continue
		}
		if !hardcodedOK {
			continue
		}
		typedSpecValue, err := decodeConfigValue(specValue, reflect.TypeOf(erigonValue))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: Erigon %v, spec %v (decode: %v)", key, erigonValue, specValue, err))
			continue
		}
		if !reflect.DeepEqual(erigonValue, typedSpecValue) {
			problems = append(problems, fmt.Sprintf("%s: Erigon %v, spec %v", key, erigonValue, typedSpecValue))
		}
	}

	if len(problems) > 0 {
		slices.Sort(problems)
		t.Errorf("mainnet config differs from consensus-spec fixtures:\n%s", strings.Join(problems, "\n"))
	}
}

func readMainnetConfigReference(t *testing.T, root fs.FS) (map[string]any, map[string]yaml.Node) {
	t.Helper()

	values := make(map[string][]configValueCount)
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
		var config map[string]yaml.Node
		if err := yaml.Unmarshal(contents, &config); err != nil {
			return fmt.Errorf("decode %s: %w", name, err)
		}
		configFiles++
		for key, node := range config {
			if strings.HasSuffix(key, "_FORK_EPOCH") {
				continue
			}
			var value any
			if err := node.Decode(&value); err != nil {
				return fmt.Errorf("decode %s key %s: %w", name, key, err)
			}
			counts := values[key]
			found := false
			for i := range counts {
				if reflect.DeepEqual(counts[i].value, value) {
					counts[i].count++
					found = true
					break
				}
			}
			if !found {
				counts = append(counts, configValueCount{value: value, node: node, count: 1})
			}
			values[key] = counts
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read mainnet config fixtures: %v", err)
	}
	if configFiles == 0 {
		t.Fatal("no mainnet config fixtures found")
	}

	reference := make(map[string]any, len(values))
	referenceNodes := make(map[string]yaml.Node, len(values))
	for key, counts := range values {
		mostCommon := counts[0]
		for _, candidate := range counts[1:] {
			if candidate.count > mostCommon.count {
				mostCommon = candidate
			}
		}
		reference[key] = mostCommon.value
		referenceNodes[key] = mostCommon.node
	}
	return reference, referenceNodes
}

func compareConfigFields(reference map[string]any, builtIn, spec any, matched map[string]struct{}) []string {
	builtInValue := reflect.ValueOf(builtIn)
	specValue := reflect.ValueOf(spec)
	builtInType := builtInValue.Type()
	var problems []string
	for i := 0; i < builtInType.NumField(); i++ {
		key := strings.Split(builtInType.Field(i).Tag.Get("yaml"), ",")[0]
		if key == "" || key == "-" {
			continue
		}
		if _, ok := reference[key]; !ok {
			continue
		}
		matched[key] = struct{}{}
		erigonField := builtInValue.Field(i).Interface()
		specField := specValue.Field(i).Interface()
		if !reflect.DeepEqual(erigonField, specField) {
			problems = append(problems, fmt.Sprintf("%s: Erigon %v, spec %v", key, erigonField, specField))
		}
	}
	return problems
}

func decodeConfigValue(value any, valueType reflect.Type) (any, error) {
	encoded, err := yaml.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoded := reflect.New(valueType)
	if err := yaml.Unmarshal(encoded, decoded.Interface()); err != nil {
		return nil, err
	}
	return decoded.Elem().Interface(), nil
}
