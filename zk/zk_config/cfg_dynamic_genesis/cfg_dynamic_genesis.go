package cfg_dynamic_genesis

import (
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon/zk/zk_config"
	"io"
	"os"
	"path"
)

type DynamicGenesisConfig struct {
	Root       string `json:"root"`
	Timestamp  uint64 `json:"timestamp"`
	GasLimit   uint64 `json:"gasLimit"`
	Difficulty int64  `json:"difficulty"`
}

func NewDynamicGenesisConfig(chain string) *DynamicGenesisConfig {
	p := path.Join(zk_config.ZKDynamicConfigPath, chain+"-conf.json")
	if zk_config.ZkUnionConfigPath != "" {
		p = zk_config.ZkUnionConfigPath
		if _, err := os.Stat(p); err == nil {
			return unmarshalDynamicGenesisFromUnionConfig(p)
		}
	} else {
		if _, err := os.Stat(p); err == nil {
			return unmarshalDynamicGenesisFromDynamicConfig(p)
		}
	}

	panic(fmt.Sprintf("could not find dynamic genesis config for %s in %s", chain, p))
}

func unmarshalDynamicGenesisFromUnionConfig(path string) *DynamicGenesisConfig {
	file, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("could not open union config for %s: %v", path, err))
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		panic(fmt.Sprintf("could not read union config for %s: %v", path, err))
	}

	var union map[string]json.RawMessage
	if err = json.Unmarshal(data, &union); err != nil {
		panic(fmt.Sprintf("could not parse union config for %s: %v", path, err))
	}

	var genCfg DynamicGenesisConfig
	if err = json.Unmarshal(union["conf"], &genCfg); err != nil {
		panic(fmt.Sprintf("could not parse dynamic genesis conf for %s: %v", path, err))
	}

	return &genCfg
}

func unmarshalDynamicGenesisFromDynamicConfig(path string) *DynamicGenesisConfig {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("could not open dynamic genesis conf for %s: %v", path, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)

	conf := DynamicGenesisConfig{}
	err = decoder.Decode(&conf)
	if err != nil {
		panic(fmt.Sprintf("could not parse dynamic genesis conf for %s: %v", path, err))
	}

	return &conf
}
