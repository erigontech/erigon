package vm

import (
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/log/v3"
)

type ZkConfig struct {
	Config Config

	TracerCollector  bool
	CounterCollector *CounterCollector
}

func NewZkConfig(config Config, counterCollector *CounterCollector) ZkConfig {
	return ZkConfig{
		Config:           config,
		TracerCollector:  false,
		CounterCollector: counterCollector,
	}
}

func NewTracerZkConfig(config Config, counterCollector *CounterCollector) ZkConfig {
	return ZkConfig{
		Config:           config,
		TracerCollector:  true,
		CounterCollector: counterCollector,
	}
}

func getJumpTable(cr *chain.Rules) *JumpTable {
	var jt *JumpTable
	switch {
	case cr.IsForkID8Elderberry:
		jt = &forkID8ElderberryInstructionSet
	case cr.IsForkID5Dragonfruit, cr.IsForkID6IncaBerry, cr.IsForkID7Etrog:
		jt = &forkID5DragonfruitInstructionSet
	case cr.IsBerlin:
		jt = &forkID4InstructionSet
	}

	return jt
}

// NewZKEVMInterpreter returns a new instance of the Interpreter.
func NewZKEVMInterpreter(evm VMInterpreter, cfg ZkConfig) *EVMInterpreter {
	jt := getJumpTable(evm.ChainRules())

	// here we need to copy the jump table every time as we're about to wrap it with the zk counters handling
	// if we don't take a copy of this it will be wrapped over and over again causing a deeper and deeper stack
	// and duplicating the zk counters handling
	jt = copyJumpTable(jt)

	if len(cfg.Config.ExtraEips) > 0 {
		for i, eip := range cfg.Config.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.Config.ExtraEips = append(cfg.Config.ExtraEips[:i], cfg.Config.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "err", err)
			}
		}
	}

	// if we have an active counter collector for the call then we need
	// to wrap the jump table so that we can process counters as op codes are called within
	// the EVM
	if cfg.CounterCollector != nil {
		WrapJumpTableWithZkCounters(jt, SimpleCounterOperations(cfg.CounterCollector))
	}

	return &EVMInterpreter{
		VM: &VM{
			evm: evm,
			cfg: cfg.Config,
		},
		jt: jt,
	}
}
