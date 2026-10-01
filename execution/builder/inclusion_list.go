package builder

import (
	"context"
	"math"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

func (b *Builder) BuildInclusionList(ctx context.Context) (types.Transactions, error) {
	provideOpts := []txnprovider.ProvideOption{
		txnprovider.WithAvailableRlpSpace(int(params.MaxBytesPerInclusionListEIP7805)),
		txnprovider.WithGasTarget(mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, 0)),
	}

	txns, err := b.txnProvider.ProvideTxns(ctx, provideOpts...)
	if err != nil {
		return nil, err
	}
	return types.Transactions(txns), nil
}
