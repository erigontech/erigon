package clvm

type Encoder interface {
	WriteCycle(xs ...Instruction) error
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
