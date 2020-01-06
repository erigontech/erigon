package trie

func (t *Trie) ExtractWitness(blockNr uint64, trace bool, rs *ResolveSet, codeMap CodeMap) (*Witness, error) {
	builder := NewWitnessBuilder(t, blockNr, trace, rs, codeMap)
	hr := newHasher(false)
	defer returnHasherToPool(hr)
	return builder.Build(hr.hash)
}
