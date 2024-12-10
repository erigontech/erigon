package composite

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider"
)

func TestTxnProvider(t *testing.T) {
	t.Parallel()

	txn1 := mockTransaction(1)
	txn2 := mockTransaction(2)
	txn3 := mockTransaction(3)
	txn4 := mockTransaction(4)
	for _, tc := range []struct {
		name       string
		providers  []*mockProvider
		opts       []txnprovider.YieldOption
		wantResult txnprovider.YieldResult
	}{
		{
			name: "yield all transactions from all providers in correct order",
			providers: []*mockProvider{
				{
					priority: 10,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn3},
						TotalGas:     21_000,
					},
					wantCalled: true,
				},
				{
					priority: 25,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn4},
						TotalBlobGas: 131_072,
					},
					wantCalled: true,
				},
				{
					priority: 50,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn1, txn2},
						TotalGas:     21_000,
						TotalBlobGas: 131_072,
					},
					wantCalled: true,
				},
			},
			wantResult: txnprovider.YieldResult{
				Transactions: []types.Transaction{txn1, txn2, txn4, txn3},
				TotalGas:     42_000,
				TotalBlobGas: 262_144,
			},
		},
		{
			name: "yield transactions up to amount target",
			opts: []txnprovider.YieldOption{
				txnprovider.WithAmount(3),
			},
			providers: []*mockProvider{
				{
					priority: 10,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn3},
						TotalGas:     21_000,
					},
				},
				{
					priority: 25,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn4},
						TotalBlobGas: 131_072,
					},
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithAmount(1)},
					wantCalled:         true,
				},
				{
					priority: 50,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn1},
						TotalGas:     21_000,
					},
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithAmount(2)},
					wantCalled:         true,
				},
				{
					priority: 75,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn2},
						TotalGas:     21_000,
					},
					wantCalled:         true,
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithAmount(3)},
				},
			},
			wantResult: txnprovider.YieldResult{
				Transactions: []types.Transaction{txn2, txn1, txn4},
				TotalGas:     42_000,
				TotalBlobGas: 131_072,
			},
		},
		{
			name: "yield transactions up to gas target",
			opts: []txnprovider.YieldOption{
				txnprovider.WithGasTarget(43_000),
			},
			providers: []*mockProvider{
				{
					priority: 10,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn3},
						TotalGas:     21_000,
					},
				},
				{
					priority: 25,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn4},
						TotalBlobGas: 131_072,
					},
				},
				{
					priority: 50,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn2},
						TotalGas:     21_000,
					},
					wantCalled:         true,
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithGasTarget(22_000)},
				},
				{
					priority: 75,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn1},
						TotalGas:     21_000,
					},
					wantCalled:         true,
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithGasTarget(43_000)},
				},
			},
			wantResult: txnprovider.YieldResult{
				Transactions: []types.Transaction{txn1, txn2},
				TotalGas:     42_000,
			},
		},
		{
			name: "yield transactions up to blob gas target",
			opts: []txnprovider.YieldOption{
				txnprovider.WithBlobGasTarget(131_072),
			},
			providers: []*mockProvider{
				{
					// note it should still be called even after blob gas target has been reached
					// since we can still fill in the main gas target
					priority: 10,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn3},
						TotalGas:     21_000,
					},
					wantCalled:         true,
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithBlobGasTarget(0)},
				},
				{
					priority: 25,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn4},
						TotalBlobGas: 131_072,
					},
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithBlobGasTarget(131_072)},
					wantCalled:         true,
				},
				{
					priority: 50,
					result: txnprovider.YieldResult{
						Transactions: []types.Transaction{txn1},
						TotalGas:     21_000,
					},
					wantCalledWithOpts: []txnprovider.YieldOption{txnprovider.WithBlobGasTarget(131_072)},
					wantCalled:         true,
				},
			},
			wantResult: txnprovider.YieldResult{
				Transactions: []types.Transaction{txn1, txn4, txn3},
				TotalGas:     42_000,
				TotalBlobGas: 131_072,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			providers := make([]txnprovider.TxnProvider, len(tc.providers))
			for i, p := range tc.providers {
				providers[i] = p
			}

			provider := NewTxnProvider(providers)
			have, err := provider.Yield(ctx, tc.opts...)
			require.NoError(t, err)
			require.Equal(t, tc.wantResult.TotalGas, have.TotalGas)
			require.Equal(t, tc.wantResult.TotalBlobGas, have.TotalBlobGas)
			require.Equal(t, len(tc.wantResult.Transactions), len(have.Transactions))
			for i, wantTxn := range tc.wantResult.Transactions {
				haveTxn := have.Transactions[i]
				msg := "transaction mismatch at index %d with nonce %d vs %d"
				require.Equal(t, wantTxn.Hash(), haveTxn.Hash(), msg, i, wantTxn.GetNonce(), haveTxn.GetNonce())
			}
			for i, p := range tc.providers {
				p.AssertCalls(t, i)
			}
		})
	}
}

func mockTransaction(nonce uint64) types.Transaction {
	chainId := *uint256.NewInt(123)
	to := libcommon.HexToAddress("0x1000")
	amount := uint256.NewInt(444)
	gasLimit := uint64(1_000)
	gasPrice := uint256.NewInt(2)
	gasTip := uint256.NewInt(0)
	gasFeeCap := uint256.NewInt(1_000_000)
	return types.NewEIP1559Transaction(chainId, nonce, to, amount, gasLimit, gasPrice, gasTip, gasFeeCap, nil)
}

type mockProvider struct {
	priority           int
	result             txnprovider.YieldResult
	wantCalled         bool
	haveCalled         bool
	wantCalledWithOpts []txnprovider.YieldOption
	haveCalledWithOpts []txnprovider.YieldOption
}

func (p *mockProvider) Priority() int {
	return p.priority
}

func (p *mockProvider) Yield(_ context.Context, opts ...txnprovider.YieldOption) (txnprovider.YieldResult, error) {
	p.haveCalled = true
	p.haveCalledWithOpts = append(p.haveCalledWithOpts, opts...)
	return p.result, nil
}

func (p *mockProvider) AssertCalls(t *testing.T, i int) {
	require.Equal(t, p.wantCalled, p.haveCalled, "provider haveCalled mismatch at index %d", i)
	wantYieldParams := txnprovider.YieldParamsFromOptions(p.wantCalledWithOpts...)
	haveYieldParams := txnprovider.YieldParamsFromOptions(p.haveCalledWithOpts...)
	require.Equal(t, wantYieldParams, haveYieldParams, "provider haveCalledWithOpts mismatch at index %d", i)
}
