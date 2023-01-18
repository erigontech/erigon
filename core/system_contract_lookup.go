package core

import (
	"encoding/hex"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
)

func init() {
	// Initialise systemContractCodeLookup
	for _, chainName := range []string{networkname.BSCChainName, networkname.ChapelChainName, networkname.RialtoChainName} {
		byChain := map[libcommon.Address][]state.CodeRecord{}
		systemcontracts.SystemContractCodeLookup[chainName] = byChain
		// Apply genesis with the block number 0
		genesisBlock := DefaultGenesisBlockByChainName(chainName)
		for addr, alloc := range genesisBlock.Alloc {
			if len(alloc.Code) > 0 {
				list := byChain[addr]
				codeHash, err := common.HashData(alloc.Code)
				if err != nil {
					panic(fmt.Errorf("failed to hash system contract code: %s", err.Error()))
				}
				list = append(list, state.CodeRecord{BlockNumber: 0, CodeHash: codeHash})
				byChain[addr] = list
			}
		}
		// Process upgrades
		chainConfig := params.ChainConfigByChainName(chainName)
		if chainConfig.RamanujanBlock != nil {
			blockNum := chainConfig.RamanujanBlock.Uint64()
			if blockNum != 0 {
				addCodeRecords(systemcontracts.RamanujanUpgrade[chainName], blockNum, byChain)
			}
		}
		if chainConfig.NielsBlock != nil {
			blockNum := chainConfig.NielsBlock.Uint64()
			if blockNum != 0 {
				addCodeRecords(systemcontracts.NielsUpgrade[chainName], blockNum, byChain)
			}
		}
		if chainConfig.MirrorSyncBlock != nil {
			blockNum := chainConfig.MirrorSyncBlock.Uint64()
			if blockNum != 0 {
				addCodeRecords(systemcontracts.MirrorUpgrade[chainName], blockNum, byChain)
			}
		}
		if chainConfig.BrunoBlock != nil {
			blockNum := chainConfig.BrunoBlock.Uint64()
			if blockNum != 0 {
				addCodeRecords(systemcontracts.BrunoUpgrade[chainName], blockNum, byChain)
			}
		}
		if chainConfig.EulerBlock != nil {
			blockNum := chainConfig.EulerBlock.Uint64()
			if blockNum != 0 {
				addCodeRecords(systemcontracts.EulerUpgrade[chainName], blockNum, byChain)
			}
		}
		if chainConfig.MoranBlock != nil {
			blockNum := chainConfig.MoranBlock.Uint64()
			if blockNum != 0 {
				addCodeRecords(systemcontracts.MoranUpgrade[chainName], blockNum, byChain)
			}
		}
		if chainConfig.GibbsBlock != nil {
			blockNum := chainConfig.GibbsBlock.Uint64()
			if blockNum != 0 {
				addCodeRecords(systemcontracts.GibbsUpgrade[chainName], blockNum, byChain)
			}
		}
	}
}

func addCodeRecords(upgrade *systemcontracts.Upgrade, blockNum uint64, byChain map[libcommon.Address][]state.CodeRecord) {
	for _, config := range upgrade.Configs {
		list := byChain[config.ContractAddr]
		code, err := hex.DecodeString(config.Code)
		if err != nil {
			panic(fmt.Errorf("failed to decode system contract code: %s", err.Error()))
		}
		codeHash, err := common.HashData(code)
		if err != nil {
			panic(fmt.Errorf("failed to hash system contract code: %s", err.Error()))
		}
		list = append(list, state.CodeRecord{BlockNumber: blockNum, CodeHash: codeHash})
		byChain[config.ContractAddr] = list
	}
}
