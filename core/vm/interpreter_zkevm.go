package vm

import (
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/log/v3"
)

type ZkConfig struct {
	Config Config

	CounterCollector *CounterCollector
}

func NewZkConfig(config Config, counterCollector *CounterCollector) ZkConfig {
	return ZkConfig{
		Config:           config,
		CounterCollector: counterCollector,
	}
}

// NewZKEVMInterpreter returns a new instance of the Interpreter.
func NewZKEVMInterpreter(evm VMInterpreter, cfg ZkConfig) *EVMInterpreter {
	var jt *JumpTable
	switch {
	// to add our own IsRohan chain rule, we would need to fork or code or chain.Config
	// that is why we hard code it here for POC
	// our fork extends berlin anyways and starts from block 1
	case evm.ChainRules().IsForkID7Etrog:
		jt = &forkID7EtrogInstructionSet
	case evm.ChainRules().IsForkID5Dragonfruit, evm.ChainRules().IsForkID6IncaBerry:
		jt = &forkID5DragonfruitInstructionSet
	case evm.ChainRules().IsBerlin:
		jt = &forkID4InstructionSet
	}

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

	// TODOSEQ - replace counter manager with a transaction counter collector
	if sequencer.IsSequencer() {
		if cfg.CounterCollector == nil {
			cfg.CounterCollector = NewCounterCollector()
		}

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
