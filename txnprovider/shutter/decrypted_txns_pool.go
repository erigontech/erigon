package shutter

import (
	"context"

	"github.com/erigontech/erigon/core/types"
)

type DecryptedTxnsPool struct {
}

func (p DecryptedTxnsPool) SeenDecryptionKeys(slot uint64) bool {
	return false
}

func (p DecryptedTxnsPool) WaitForSlot(ctx context.Context, slot uint64) error {
	return nil
}

func (p DecryptedTxnsPool) DecryptedTxns(slot uint64, gasTarget uint64) (DecryptedTxns, error) {
	return DecryptedTxns{}, nil
}

func (p DecryptedTxnsPool) Run(ctx context.Context) error {
	return nil
}

type DecryptedTxns struct {
	TotalGas     uint64
	Transactions []types.Transaction
}
