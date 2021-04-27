package vm

// TEVMInterpreter represents an TEVM interpreter
type TEVMInterpreter struct {
	*EVMInterpreter
}

type VmType int8

const (
	UndefinedVm VmType = -1
	EVMType     VmType = 0
	TEVMType    VmType = 1
)

// NewTEVMInterpreter returns a new instance of the Interpreter.
func NewTEVMInterpreter(evm *EVM, cfg Config) *TEVMInterpreter {
	return &TEVMInterpreter{NewEVMInterpreter(evm, cfg, TEVMType)}
}
