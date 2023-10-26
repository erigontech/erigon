package clvm

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"io"
)

type encoder struct {
	w io.Writer
	b bytes.Buffer
}

func NewEncoder(w io.Writer) Encoder {
	return &encoder{
		w: w,
	}
}

func (e *encoder) NextCycle() error {
	e.b.WriteByte('\n')
	// TODO: consider prefixing this with a length/header
	if _, err := e.b.WriteTo(e.w); err != nil {
		return err
	}
	e.b.Reset()
	return nil
}

func (e *encoder) Discard() error {
	e.b.Reset()
	return nil
}
func (e *encoder) Encode(xs ...Instruction) {
	for _, x := range xs {
		e.b.Write(x.Opcode())
		e.b.WriteByte('(')
		args := x.Arguments()
		for idx, v := range args {
			hex.NewEncoder(&e.b).Write(v)
			if idx != len(args)-1 {
				e.b.WriteByte(',')
			}
		}
		e.b.WriteByte(')')
		e.b.WriteByte(' ')
	}
	return
}

// Decoder
type decoder struct {
	r *bufio.Scanner
}

// NewDecoder create new decoder
func NewDecoder(r io.Reader) Decoder {
	return &decoder{
		r: bufio.NewScanner(r),
	}
}

// Will return true if there is another cycle to be read.
func (e *decoder) Scan() bool {
	return e.r.Scan()
}

// Returns a cycle to read instructions out of
// the cycle is only valid until the next call to next
// must call scan before cycle
func (e *decoder) Cycle() Cycle {
	return &cycle{u: bytes.NewBuffer(e.r.Bytes())}
}

type cycle struct {
	u *bytes.Buffer

	i instruction
}

// Step will return true if there is more in the pc for this cycle
// it will return error if it errors during parsing of the next instrruction
// if ok is true, err will always be nil, but err can be nil when ok is false
func (c *cycle) Step() (ok bool, err error) {
	// read to the next closed parenthesis, which marks the end of a function
	line, err := c.u.ReadBytes(')')
	if err != nil {
		return false, nil
	}
	lb := bytes.NewBuffer(line)
	functionName, err := lb.ReadBytes('(')
	if err != nil {
		return false, nil
	}
	argsToSplit, err := lb.ReadBytes(')')
	if err != nil {
		return false, nil
	}
	c.i.opcode = bytes.TrimSpace(functionName[:len(functionName)-1])
	hexArgs := bytes.Split(argsToSplit[:len(argsToSplit)-1], []byte{','})
	for i, v := range hexArgs {
		hexArgs[i] = bytes.TrimSpace(v)
	}
	if cap(c.i.args) < len(hexArgs) {
		c.i.args = make([][]byte, len(hexArgs))
	}
	c.i.args = c.i.args[:len(hexArgs)]
	for idx, v := range hexArgs {
		c.i.args[idx] = growSlice(c.i.args[idx], 2*len(v))
		_, err := hex.Decode(c.i.args[idx], v)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func growSlice(from []byte, size int) (to []byte) {
	buf := bytes.NewBuffer(from)
	buf.Truncate(0)
	buf.Grow(size)
	return buf.Bytes()[:size]
}

// Pc gets the instruction at the current pc in the cycle
// the Instruction is only valid until the next call to step
func (c *cycle) Pc() Instruction {
	return &c.i
}

type instruction struct {
	opcode []byte
	args   [][]byte
}

func (c *instruction) Opcode() []byte {
	return c.opcode
}

func (c *instruction) Arguments() [][]byte {
	return c.args
}

func NewInstruction(code []byte, args ...[]byte) Instruction {
	return &instruction{
		opcode: code,
		args:   args,
	}
}
