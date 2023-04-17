package merkletree

// ResultCode represents the result code.
type ResultCode int64

const (
	// Unspecified is the code for unspecified result.
	Unspecified ResultCode = iota
	// Success is the code for success result.
	Success
	// KeyNotFound is the code for key not found result.
	KeyNotFound
	// DBError is the code for DB error result.
	DBError
	// InternalError is the code for internal error result.
	InternalError
)

// Proof is a proof generated on Get operation.
type Proof struct {
	// Root is the proof root.
	Root []uint64
	// Key is the proof key.
	Key []uint64
	// Value is the proof value.
	Value []uint64
}

// UpdateProof is a proof generated on Set operation.
type UpdateProof struct {
	// OldRoot is the update proof old root.
	OldRoot []uint64
	// NewRoot is the update proof new root.
	NewRoot []uint64
	// Key is the update proof key.
	Key []uint64
	// NewValue is the update proof new value.
	NewValue []uint64
}

// ProgramProof is a proof generated on GetProgram operation.
type ProgramProof struct {
	// Data is the program proof data.
	Data []byte
}
