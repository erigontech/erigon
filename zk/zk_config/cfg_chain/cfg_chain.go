package cfg_chain

import (
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/zk/zk_config"
	"io"
	"os"
	"path"
)

func NewDynamicChainConfig(chain string) *chain.Config {
	p := path.Join(zk_config.ZKDynamicConfigPath, chain+"-chainspec.json")
	if zk_config.ZkUnionConfigPath != "" {
		p = zk_config.ZkUnionConfigPath
		if _, err := os.Stat(p); err == nil {
			return unmarshalChainFromUnionConfig(p)
		}
	} else {
		if _, err := os.Stat(p); err == nil {
			return unmarshalChainFromDynamicConfig(p)
		}
	}

	panic(fmt.Sprintf("could not find dynamic chain config for %s in %s", chain, p))
}

func unmarshalChainFromUnionConfig(path string) *chain.Config {
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

	var chainCfg chain.Config
	if err = json.Unmarshal(union["chainspec"], &chainCfg); err != nil {
		panic(fmt.Sprintf("could not parse chain for %s: %v", path, err))
	}

	chainId := chainCfg.ChainID.Uint64()
	chain.SetDynamicChainDetails(chainId, chainCfg.ChainName)

	return &chainCfg
}

func unmarshalChainFromDynamicConfig(path string) *chain.Config {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("could not open chainspec for %s: %v", path, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)

	chainCfg := &chain.Config{}
	err = decoder.Decode(&chainCfg)
	if err != nil {
		panic(fmt.Sprintf("could not parse chainspec for %s: %v", path, err))
	}

	chainId := chainCfg.ChainID.Uint64()
	chain.SetDynamicChainDetails(chainId, chainCfg.ChainName)

	return chainCfg
}
