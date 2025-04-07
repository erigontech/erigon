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

func (tx *DynamicFeeTransaction) markAsAnchor() error {
	tx.isAnhcor = true
	return nil
}

func (tx *LegacyTx) markAsAnchor() error {
	return ErrInvalidTxType
}

func (tx *AccessListTx) markAsAnchor() error {
	return ErrInvalidTxType
}

func (tx *BlobTx) markAsAnchor() error {
	return ErrInvalidTxType
}
