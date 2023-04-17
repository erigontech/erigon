package merkletree

// leafType specifies type of the leaf
type leafType uint8

const (
	// LeafTypeBalance specifies that leaf stores Balance
	LeafTypeBalance leafType = 0
	// LeafTypeNonce specifies that leaf stores Nonce
	LeafTypeNonce leafType = 1
	// LeafTypeCode specifies that leaf stores Code
	LeafTypeCode leafType = 2
	// LeafTypeStorage specifies that leaf stores Storage Value
	LeafTypeStorage leafType = 3
	// LeafTypeSCLength specifies that leaf stores Storage Value
	LeafTypeSCLength leafType = 4
)
