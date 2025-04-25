package types

func (tx *DynamicFeeTransaction) isAnchor() bool {
	return tx.isAnhcor
}

func (tx *LegacyTx) isAnchor() bool {
	return false
}

func (tx *AccessListTx) isAnchor() bool {
	return false
}

func (tx *BlobTx) isAnchor() bool {
	return false
}

func (tx *DynamicFeeTransaction) MarkAsAnchor() error {
	tx.isAnhcor = true
	return nil
}

func (tx *LegacyTx) MarkAsAnchor() error {
	return ErrInvalidTxType
}

func (tx *AccessListTx) MarkAsAnchor() error {
	return ErrInvalidTxType
}

func (tx *BlobTx) MarkAsAnchor() error {
	return ErrInvalidTxType
}
