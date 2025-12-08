package trie

import "errors"

func (t *Trie) ExtractWitness(trace bool, rl RetainDecider) (*Witness, error) {
	var rd RetainDecider
	if rl != nil {
		rd = rl
	}
	return extractWitnessFromRootNode(t.RootNode, trace, rd)
}

func (t *Trie) ExtractWitnessForPrefix(prefix []byte, trace bool, rl RetainDecider) (*Witness, error) {
	foundNode, _, found, _ := t.getNode(prefix, false)
	if !found {
		return nil, errors.New("no data found for given prefix")
	}
	return extractWitnessFromRootNode(foundNode, trace, rl)
}

// ExtractWitnesses extracts witnesses for subtries starting from the specified root
// if retainDec param is nil it will make a witness for the full subtrie,
// if retainDec param is set to a RetainList instance, it will make a witness for only the accounts/storages that were actually touched; other paths will be hashed.
func ExtractWitnesses(subTries SubTries, trace bool, retainDec RetainDecider) ([]*Witness, error) {
	var witnesses []*Witness
	for _, root := range subTries.roots {
		builder := NewWitnessBuilder(root, trace)
		var limiter *MerklePathLimiter = nil
		if retainDec != nil {
			hr := newHasher(false)
			defer returnHasherToPool(hr)
			limiter = &MerklePathLimiter{retainDec, hr.hash}
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
// if retainDec param is nil it will make a witness for the full subtrie,
// if retainDec param is set to a RetainList instance, it will make a witness for only the accounts/storages that were actually touched; other paths will be hashed.
func extractWitnessFromRootNode(root Node, trace bool, retainDec RetainDecider) (*Witness, error) {
	builder := NewWitnessBuilder(root, trace)
	var limiter *MerklePathLimiter = nil
	if retainDec != nil {
		hr := newHasher(false)
		defer returnHasherToPool(hr)
		limiter = &MerklePathLimiter{retainDec, hr.hash}
	}
	return builder.Build(limiter)
}
