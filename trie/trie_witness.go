package trie

import "errors"

func (t *Trie) ExtractWitness(blockNr uint64, trace bool, rs *ResolveSet) (*Witness, error) {
	var h HashOnly
	if rs != nil {
		h = rs
	}
	return extractWitnessFromRootNode(t.root, blockNr, trace, h)
}

func (t *Trie) ExtractWitnessForPrefix(prefix []byte, blockNr uint64, trace bool, rs *ResolveSet) (*Witness, error) {
	foundNode, _, found, _ := t.getNode(prefix, false)
	if !found {
		return nil, errors.New("no data found for given prefix")
	}
	return extractWitnessFromRootNode(foundNode, blockNr, trace, rs)
}

// ExtractWitnesses extracts witnesses for subtries starting from the specified root
// if hashOnly param is nil it will make a witness for the full subtrie,
// if hashOnly param is set to a ResolveSet instance, it will make a witness for only the accounts/storages that were actually touched; other paths will be hashed.
func ExtractWitnesses(subTries SubTries, trace bool, hashOnly HashOnly) ([]*Witness, error) {
	var witnesses []*Witness
	for _, root := range subTries.roots {
		builder := NewWitnessBuilder(root, trace)
		var limiter *MerklePathLimiter = nil
		if hashOnly != nil {
			hr := newHasher(false)
			defer returnHasherToPool(hr)
			limiter = &MerklePathLimiter{hashOnly, hr.hash}
		}
		if witness, err := builder.Build(limiter); err == nil {
			witnesses = append(witnesses, witness)
		} else {
			return witnesses, err
		}
	}
	return witnesses, nil
}

// extractWitnessFromRootNode extracts witness for subtrie starting from the specified root
// if hashOnly param is nil it will make a witness for the full subtrie,
// if hashOnly param is set to a ResolveSet instance, it will make a witness for only the accounts/storages that were actually touched; other paths will be hashed.
func extractWitnessFromRootNode(root node, blockNr uint64, trace bool, hashOnly HashOnly) (*Witness, error) {
	builder := NewWitnessBuilder(root, trace)
	var limiter *MerklePathLimiter = nil
	if hashOnly != nil {
		hr := newHasher(false)
		defer returnHasherToPool(hr)
		limiter = &MerklePathLimiter{hashOnly, hr.hash}
	}
	return builder.Build(limiter)
}

