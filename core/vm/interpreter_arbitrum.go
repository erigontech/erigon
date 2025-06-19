package vm

func (in *EVMInterpreter) Config() *Config {
	c := in.evm.Config()
	return &c
}

// Depth returns the current call stack depth.
func (in *EVMInterpreter) IncrementDepth() {
	in.depth++
}

func (in *EVMInterpreter) DecrementDepth() {
	in.depth--
}

func (in *EVMInterpreter) EVM() *EVM {
	return in.VM.evm
}

func (in *EVMInterpreter) ReadOnly() bool {
	return in.VM.readOnly
}

func (in *EVMInterpreter) GetReturnData() []byte {
	return in.returnData
}

func (in *EVMInterpreter) SetReturnData(data []byte) {
	in.returnData = data
}
