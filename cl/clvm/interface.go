package clvm

type Encoder interface {
	Encode(xs ...Instruction)
	NextCycle() error
}

type Decoder interface {
	Scan() bool
	Cycle() Cycle
}

type Cycle interface {
	Step() (bool, error)
	Pc() Instruction
}

type Instruction interface {
	Opcode() []byte
	Arguments() [][]byte
}
