package vm

import "github.com/ledgerwatch/log/v3"

// NewZKEVMInterpreter returns a new instance of the Interpreter.
func NewZKEVMInterpreter(evm VMInterpreter, cfg Config) *EVMInterpreter {
	var jt *JumpTable
	switch {
	// to add our own IsRohan chain rule, we would need to fork or code or chain.Config
	// that is why we hard code it here for POC
	// our fork extends berlin anyways and starts from block 1
	case evm.ChainRules().IsMordor:
		jt = &zkevmForkID5InstructionSet
	case evm.ChainRules().IsBerlin:
		jt = &zkevmForkID4InstructionSet
	}
	if len(cfg.ExtraEips) > 0 {
		jt = copyJumpTable(jt)
		for i, eip := range cfg.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.ExtraEips = append(cfg.ExtraEips[:i], cfg.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "err", err)
			}
		}
	}

	return &EVMInterpreter{
		VM: &VM{
			evm: evm,
			cfg: cfg,
		},
		jt: jt,
	}
}
