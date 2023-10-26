package clvm

type Encoder interface {
	NextCycle() error
	Encode(xs ...Instruction)
	Discard() error
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
