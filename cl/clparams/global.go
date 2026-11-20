package clparams

import "sync/atomic"

var globalBeaconConfig atomic.Pointer[BeaconChainConfig]

// Safe to call more than once; later calls replace the previous config.
func InitGlobalStaticConfig(bcfg *BeaconChainConfig) {
	if bcfg == nil {
		panic("cannot initialize globalBeaconConfig with nil")
	}
	if err := bcfg.ValidateExecutionRequestTypeConstants(); err != nil {
		panic(err)
	}
	globalBeaconConfig.Store(bcfg)
}

func GetBeaconConfig() *BeaconChainConfig {
	return globalBeaconConfig.Load()
}
