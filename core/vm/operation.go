package vm

type operation struct {
	// execute is the operation function
	execute     executionFunc
	constantGas uint64
	dynamicGas  gasFunc
	// minStack tells how many stack items are required
	minStack int
	// maxStack specifies the max length the stack can have for this operation
	// to not overflow the stack.
	maxStack int

	numPop  int
	numPush int
	isPush  bool
	isSwap  bool
	isDup   bool
	opNum   int // only for push, swap, dup
	// memorySize returns the memory size required for the operation
	memorySize memorySizeFunc

	halts   bool // indicates whether the operation should halt further execution
	jumps   bool // indicates whether the program counter should not increment
	writes  bool // determines whether this a state modifying operation
	reverts bool // determines whether the operation reverts state (implicitly halts)
	returns bool // determines whether the operations sets the return data content
}

func (o operation) SetConstantGas(constantGas uint64) *operation {
	o.constantGas = constantGas
	return &o
}

func (o operation) SetDynamicGas(dynamicGas gasFunc) *operation {
	o.dynamicGas = dynamicGas
	return &o
}
