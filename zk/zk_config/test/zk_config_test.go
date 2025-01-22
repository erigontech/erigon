package test

import (
	"github.com/ledgerwatch/erigon/zk/zk_config"
	"github.com/ledgerwatch/erigon/zk/zk_config/cfg_allocs"
	"github.com/ledgerwatch/erigon/zk/zk_config/cfg_chain"
	"github.com/ledgerwatch/erigon/zk/zk_config/cfg_dynamic_genesis"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const configsDir = "./test_configs"

func Test_ZKConfig(t *testing.T) {
	scenarios := map[string]struct {
		includedTestConfigs []string
		chainName           string
		unionFileName       string
		expectedPanic       string
	}{
		"Unmarshal valid union config": {
			includedTestConfigs: []string{
				filepath.Join(configsDir, "union.json"),
			},
			chainName:     "test",
			unionFileName: "union.json",
			expectedPanic: "",
		},
		"Unmarshal valid dynamic config": {
			includedTestConfigs: []string{
				filepath.Join(configsDir, "test-allocs.json"),
				filepath.Join(configsDir, "test-chainspec.json"),
				filepath.Join(configsDir, "test-conf.json"),
			},
			chainName:     "test",
			unionFileName: "",
			expectedPanic: "",
		},
		"Unmarshal dynamic config with invalid allocs": {
			includedTestConfigs: []string{
				filepath.Join(configsDir, "union-invalid.json"),
			},
			chainName:     "test",
			unionFileName: "union-invalid.json",
			expectedPanic: "could not parse allocs",
		},
		"Unmarshal dynamic config, chainspec missing": {
			includedTestConfigs: []string{
				filepath.Join(configsDir, "test-allocs.json"),
				filepath.Join(configsDir, "test-conf.json"),
			},
			chainName:     "test",
			unionFileName: "",
			expectedPanic: "could not find dynamic chain config",
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					actualPanic, ok := r.(string)
					if !ok {
						t.Errorf("expected panic message to start with %v, but got non-string panic: %v", scenario.expectedPanic, r)
						return
					}
					if scenario.expectedPanic == "" && actualPanic != "" {
						t.Errorf("expected no panic but got %v", actualPanic)
					}
					if !strings.HasPrefix(actualPanic, scenario.expectedPanic) {
						t.Errorf("expected panic message to start with %v, got %v", scenario.expectedPanic, actualPanic)
					}
				}
			}()

			// create temp dir for test configs
			tempDir, err := os.MkdirTemp("", "*")
			if err != nil {
				t.Fatalf("could not create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			zk_config.ZKDynamicConfigPath = tempDir

			if scenario.unionFileName != "" {
				zk_config.ZkUnionConfigPath = tempDir + "/" + scenario.unionFileName
			} else {
				zk_config.ZkUnionConfigPath = ""
			}

			for _, filePath := range scenario.includedTestConfigs {
				content, err := os.ReadFile(filePath)
				if err != nil {
					t.Fatalf("could not read file %q: %v", filePath, err)
				}

				if err = os.WriteFile(filepath.Join(tempDir, filepath.Base(filePath)), content, 0644); err != nil {
					t.Fatalf("could not write file: %v", err)
				}
			}

			_ = cfg_allocs.NewDynamicAllocsConfig(scenario.chainName)
			_ = cfg_chain.NewDynamicChainConfig(scenario.chainName)
			_ = cfg_dynamic_genesis.NewDynamicGenesisConfig(scenario.chainName)
		})
	}
}
