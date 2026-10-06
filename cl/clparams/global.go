package clparams

var globalBeaconConfig *BeaconChainConfig

func InitGlobalStaticConfig(bcfg *BeaconChainConfig) {
	if bcfg == nil {
		panic("cannot initialize globalBeaconConfig with nil")
	}
	if globalBeaconConfig != nil {
		panic("globalBeaconConfig already initialized")
	}
	if err := bcfg.ValidateExecutionRequestTypeConstants(); err != nil {
		panic(err)
	}
	globalBeaconConfig = bcfg
}

func GetBeaconConfig() *BeaconChainConfig {
	return globalBeaconConfig
}
