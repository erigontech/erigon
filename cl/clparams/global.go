package clparams

import "sync/atomic"

var (
	globalBeaconConfig atomic.Pointer[BeaconChainConfig]
	globalCaplinConfig atomic.Pointer[CaplinConfig]
)

// Safe to call more than once; later calls replace the previous config.
func InitGlobalStaticConfig(bcfg *BeaconChainConfig, ccfg *CaplinConfig) {
	if bcfg == nil {
		panic("cannot initialize globalBeaconConfig with nil")
	}
	if ccfg == nil {
		panic("cannot initialize globalCaplinConfig with nil")
	}
	// main's validation, kept. main's "already initialized" panics are NOT: a
	// node hosting one Caplin per chain initialises this more than once by
	// design, which is why the globals became atomic pointers.
	if err := bcfg.ValidateExecutionRequestTypeConstants(); err != nil {
		panic(err)
	}
	globalBeaconConfig.Store(bcfg)
	globalCaplinConfig.Store(ccfg)
}

func GetBeaconConfig() *BeaconChainConfig {
	return globalBeaconConfig.Load()
}

func IsDevnet() bool {
	cfg := globalCaplinConfig.Load()
	if cfg == nil {
		return false
	}
	return cfg.IsDevnet()
}
