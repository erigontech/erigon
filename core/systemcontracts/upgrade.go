package systemcontracts

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/state"
)

type UpgradeConfig struct {
	BeforeUpgrade upgradeHook
	AfterUpgrade  upgradeHook
	ContractAddr  libcommon.Address
	CommitUrl     string
	Code          string
}

type Upgrade struct {
	UpgradeName string
	Configs     []*UpgradeConfig
}

type upgradeHook func(blockNumber *big.Int, contractAddr libcommon.Address, statedb *state.IntraBlockState) error

var (
	//upgrade config

	CalcuttaUpgrade = make(map[string]*Upgrade)

	// SystemContractCodeLookup is used to address a flaw in the upgrade logic of the system contracts. Since they are updated directly, without first being self-destructed
	// and then re-created, the usual incarnation logic does not get activated, and all historical records of the code of these contracts are retrieved as the most
	// recent version. This problem will not exist in erigon3, but until then, a workaround will be used to access code of such contracts through this structure
	// Lookup is performed first by chain name, then by contract address. The value in the map is the list of CodeRecords, with increasing block numbers,
	// to be used in binary search to determine correct historical code
	SystemContractCodeLookup = map[string]map[libcommon.Address][]libcommon.CodeRecord{}
)

func UpgradeBuildInSystemContract(config *chain.Config, blockNumber *big.Int, statedb *state.IntraBlockState) {
	if config == nil || blockNumber == nil || statedb == nil {
		return
	}

	logger := log.New("system-contract-upgrade", config.ChainName)

	if config.Bor != nil && config.Bor.IsOnCalcutta(blockNumber) {
		applySystemContractUpgrade(CalcuttaUpgrade[config.ChainName], blockNumber, statedb, logger)
	}

	/*
		apply other upgrades
	*/
}

func applySystemContractUpgrade(upgrade *Upgrade, blockNumber *big.Int, statedb *state.IntraBlockState, logger log.Logger) {
	if upgrade == nil {
		logger.Info("Empty upgrade config", "height", blockNumber.String())
		return
	}

	logger.Info(fmt.Sprintf("Apply upgrade %s at height %d", upgrade.UpgradeName, blockNumber.Int64()))
	for _, cfg := range upgrade.Configs {
		logger.Info(fmt.Sprintf("Upgrade contract %s to commit %s", cfg.ContractAddr.String(), cfg.CommitUrl))

		if cfg.BeforeUpgrade != nil {
			err := cfg.BeforeUpgrade(blockNumber, cfg.ContractAddr, statedb)
			if err != nil {
				panic(fmt.Errorf("contract address: %s, execute beforeUpgrade error: %s", cfg.ContractAddr.String(), err.Error()))
			}
		}

		newContractCode, err := hex.DecodeString(cfg.Code)
		if err != nil {
			panic(fmt.Errorf("failed to decode new contract code: %s", err.Error()))
		}

		prevContractCode := statedb.GetCode(cfg.ContractAddr)
		if len(prevContractCode) == 0 && len(newContractCode) > 0 {
			// system contracts defined after genesis need to be explicitly created
			statedb.CreateAccount(cfg.ContractAddr, true)
		}

		statedb.SetCode(cfg.ContractAddr, newContractCode)

		if cfg.AfterUpgrade != nil {
			err := cfg.AfterUpgrade(blockNumber, cfg.ContractAddr, statedb)
			if err != nil {
				panic(fmt.Errorf("contract address: %s, execute afterUpgrade error: %s", cfg.ContractAddr.String(), err.Error()))
			}
		}
	}
}
