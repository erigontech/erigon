package clparams

import "math"

const (
	// non-configurable constants
	// For Gloas
	BuilderIndexFlag                   = uint64(1 << 40) // 2^40
	BuilderIndexSelfBuild              = math.MaxUint64
	BuilderPaymentThresholdNumerator   = uint64(6)
	BuilderPaymentThresholdDenominator = uint64(10)
)

var (
	globalBeaconConfig *BeaconChainConfig
	globalCaplinConfig *CaplinConfig
)

func InitGlobalStaticConfig(bcfg *BeaconChainConfig, ccfg *CaplinConfig) {
	if bcfg == nil {
		panic("cannot initialize globalBeaconConfig with nil")
	}
	if ccfg == nil {
		panic("cannot initialize globalCaplinConfig with nil")
	}
	if globalCaplinConfig != nil {
		panic("globalConfig already initialized")
	}
	if globalBeaconConfig != nil {
		panic("globalBeaconConfig already initialized")
	}
	globalBeaconConfig = bcfg
	globalCaplinConfig = ccfg
}

func GetBeaconConfig() *BeaconChainConfig {
	return globalBeaconConfig
}

func IsDevnet() bool {
	return globalCaplinConfig.IsDevnet()
}
