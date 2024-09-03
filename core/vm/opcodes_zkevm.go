package vm

const (
	SENDALL OpCode = 0xfb
)

// adding extra opcodes dynamically to keep separate from the main codebase
// that simplifies rebasing new versions of Erigon
func init() {
	opCodeToString[SENDALL] = "SENDALL"
	stringToOp["SENDALL"] = SENDALL
}
