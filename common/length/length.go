package length

// Lengths of hashes and addresses in bytes.
const (
	// Hash is the expected length of the hash (in bytes)
	Hash = 32
	// Addr is the expected length of the address (in bytes)
	Addr = 20
	// BlockNumberLen length of uint64 big endian
	BlockNum = 8
	// Incarnation length of uint64 for contract incarnations
	Incarnation = 8
)
