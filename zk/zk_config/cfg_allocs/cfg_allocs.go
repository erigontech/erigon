package cfg_allocs

import (
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/zk_config"
	"io"
	"os"
	"path"
)

func NewDynamicAllocsConfig(chain string) types.GenesisAlloc {
	p := path.Join(zk_config.ZKDynamicConfigPath, chain+"-allocs.json")
	if zk_config.ZkUnionConfigPath != "" {
		p = zk_config.ZkUnionConfigPath
		if _, err := os.Stat(p); err == nil {
			return unmarshalAllocsFromUnionConfig(p)
		}
	} else {
		if _, err := os.Stat(p); err == nil {
			return unmarshalAllocsFromDynamicConfig(p)
		}
	}

	panic(fmt.Sprintf("could not find dynamic allocs config for %s in %s", chain, p))
}

func unmarshalAllocsFromUnionConfig(path string) types.GenesisAlloc {
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

	var allocCfg types.GenesisAlloc
	if err = json.Unmarshal(union["allocs"], &allocCfg); err != nil {
		panic(fmt.Sprintf("could not parse allocs for %s: %v", path, err))
	}

	return allocCfg
}

func unmarshalAllocsFromDynamicConfig(path string) types.GenesisAlloc {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("could not open allocs for %s: %v", path, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)

	alloc := make(types.GenesisAlloc)
	err = decoder.Decode(&alloc)
	if err != nil {
		panic(fmt.Sprintf("could not parse alloc for %s: %v", path, err))
	}

	return alloc
}
