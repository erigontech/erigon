package trie

func (t *Trie) ExtractWitness(blockNr uint64, trace bool, rs *ResolveSet) (*Witness, error) {
	return extractWitnessFromRootNode(t.root, blockNr, trace, rs)
}

// extractWitnessFromRootNode extracts a witness for a subtrie starting from the specified root
// if hashOnly param is nil it will make a witness for the full subtrie,
// if hashOnly param is set to a ResolveSet instance, it will make a witness for only the accounts/storages that were actually touched; other paths will be hashed.
func extractWitnessFromRootNode(root node, blockNr uint64, trace bool, hashOnly HashOnly) (*Witness, error) {
	builder := NewWitnessBuilder(root, blockNr, trace)
	var limiter *MerklePathLimiter
	if hashOnly != nil {
		hr := newHasher(false)
		defer returnHasherToPool(hr)
		limiter = &MerklePathLimiter{hashOnly, hr.hash}
	}

	return builder.Build(limiter)
}
