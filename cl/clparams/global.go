package clparams

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
	// Idempotent: CaplinService.Restart re-enters RunCaplinService
	// which calls this again. The config is process-wide and the
	// network doesn't change between runs of the same Caplin, so it's
	// safe to skip silently on subsequent calls.
	if globalCaplinConfig != nil {
		return
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
