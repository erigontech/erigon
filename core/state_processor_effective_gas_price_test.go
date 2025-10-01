package core

import (
	"context"
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
)

// MockEngine is a simple mock consensus engine for testing
type MockEngine struct{}

func (m *MockEngine) Author(header *types.Header) (libcommon.Address, error) {
	return libcommon.Address{}, nil
}

func (m *MockEngine) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	return nil
}

func (m *MockEngine) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	return nil, nil
}

func (m *MockEngine) VerifyUncles(chain consensus.ChainHeaderReader, block *types.Block) error {
	return nil
}

func (m *MockEngine) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
}

func (m *MockEngine) Finalize(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, receipts []*types.Receipt, withdrawals []*types.Withdrawal) error {
	return nil
}

func (m *MockEngine) FinalizeAndAssemble(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, receipts []*types.Receipt, withdrawals []*types.Withdrawal) (*types.Block, error) {
	return nil, nil
}

func (m *MockEngine) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	return nil
}

func (m *MockEngine) SealHash(header *types.Header) libcommon.Hash {
	return libcommon.Hash{}
}

func (m *MockEngine) CalcDifficulty(chain consensus.ChainHeaderReader, time uint64, parent *types.Header) *big.Int {
	return big.NewInt(0)
}

func (m *MockEngine) APIs(chain consensus.ChainHeaderReader) []interface{} {
	return nil
}

func (m *MockEngine) Close() error {
	return nil
}

func (m *MockEngine) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	// Return false for testing - we want to test the effective gas price mechanism
	return false
}

func (m *MockEngine) Type() chain.ConsensusName {
	return "mock"
}

func (m *MockEngine) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall) ([]consensus.Reward, error) {
	return nil, nil
}

func (m *MockEngine) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState, syscall consensus.SysCallCustom, logger interface{}) {
}

func (m *MockEngine) GetTransferFunc() evmtypes.TransferFunc {
	return nil
}

func (m *MockEngine) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

// TestEffectiveGasPriceFreeTransactions tests that transactions with zero effective gas price are marked as FREE
func TestEffectiveGasPriceFreeTransactions(t *testing.T) {
	tests := []struct {
		name                        string
		effectiveGasPricePercentage uint8
		gasPrice                    *uint256.Int
		expectedIsFree              bool
		description                 string
	}{
		{
			name:                        "ZeroEffectiveGasPrice_ZeroGasPrice_ShouldBeFree",
			effectiveGasPricePercentage: 0, // 0% effective gas price
			gasPrice:                    uint256.NewInt(0),
			expectedIsFree:              true,
			description:                 "Transaction with 0% effective gas price and 0 gas price should be marked as FREE",
		},
		{
			name:                        "ZeroEffectiveGasPrice_NonZeroGasPrice_ShouldBeFree",
			effectiveGasPricePercentage: 0,                          // 0% effective gas price
			gasPrice:                    uint256.NewInt(1000000000), // 1 gwei
			expectedIsFree:              true,
			description:                 "Transaction with 0% effective gas price and non-zero gas price should be marked as FREE",
		},
		{
			name:                        "NonZeroEffectiveGasPrice_ZeroGasPrice_ShouldNotBeFree",
			effectiveGasPricePercentage: 50, // 50% effective gas price
			gasPrice:                    uint256.NewInt(0),
			expectedIsFree:              false,
			description:                 "Transaction with non-zero effective gas price and 0 gas price should not be marked as FREE",
		},
		{
			name:                        "NonZeroEffectiveGasPrice_NonZeroGasPrice_ShouldNotBeFree",
			effectiveGasPricePercentage: 50,                         // 50% effective gas price
			gasPrice:                    uint256.NewInt(1000000000), // 1 gwei
			expectedIsFree:              false,
			description:                 "Transaction with non-zero effective gas price and non-zero gas price should not be marked as FREE",
		},
		{
			name:                        "MaxEffectiveGasPrice_ZeroGasPrice_ShouldNotBeFree",
			effectiveGasPricePercentage: 255, // 100% effective gas price (max value)
			gasPrice:                    uint256.NewInt(0),
			expectedIsFree:              false,
			description:                 "Transaction with max effective gas price and 0 gas price should not be marked as FREE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Generate a test key
			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatalf("Failed to generate key: %v", err)
			}

			// Create a test transaction
			txData := &types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce: 0,
					To:    &libcommon.Address{0x1}, // Contract address
					Gas:   21000,
					Value: uint256.NewInt(0),              // Zero value
					Data:  []byte{0x60, 0x60, 0x60, 0x60}, // Some contract call data
				},
				GasPrice: tt.gasPrice,
			}

			// Sign the transaction
			signer := types.LatestSignerForChainID(big.NewInt(1))
			tx, err := types.SignNewTx(key, *signer, txData)
			if err != nil {
				t.Fatalf("Failed to sign transaction: %v", err)
			}

			// Create test configuration
			config := &chain.Config{
				ChainID:                 big.NewInt(1),
				HomesteadBlock:          big.NewInt(0),
				DAOForkBlock:            nil,
				ByzantiumBlock:          big.NewInt(0),
				ConstantinopleBlock:     big.NewInt(0),
				PetersburgBlock:         big.NewInt(0),
				IstanbulBlock:           big.NewInt(0),
				MuirGlacierBlock:        big.NewInt(0),
				BerlinBlock:             big.NewInt(0),
				LondonBlock:             big.NewInt(0),
				ArrowGlacierBlock:       big.NewInt(0),
				GrayGlacierBlock:        big.NewInt(0),
				MergeNetsplitBlock:      big.NewInt(0),
				ShanghaiTime:            big.NewInt(0),
				CancunTime:              big.NewInt(0),
				PragueTime:              big.NewInt(0),
				ForkID5DragonfruitBlock: big.NewInt(0), // Enable ForkID5Dragonfruit for effective gas price
				AllowFreeTransactions:   true,
			}

			// Create test header
			header := &types.Header{
				Number:     big.NewInt(1),
				Time:       1000,
				GasLimit:   30000000,
				BaseFee:    big.NewInt(1000000000), // 1 gwei base fee
				Difficulty: big.NewInt(0),
			}

			// Create mock state
			db := memdb.NewTestDB(t)
			txDB, err := db.BeginRw(context.TODO())
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}
			defer txDB.Rollback()

			ibs := state.New(state.NewPlainStateReader(txDB))

			// Create EVM
			evm := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, config, vm.Config{})

			// Create mock engine
			engine := &MockEngine{}

			// Test the GetTxContext function
			msg, _, err := GetTxContext(config, engine, ibs, header, tx, evm, tt.effectiveGasPricePercentage)
			if err != nil {
				t.Fatalf("GetTxContext failed: %v", err)
			}

			// Check if the transaction is marked as FREE
			if msg.IsFree() != tt.expectedIsFree {
				t.Errorf("%s: expected IsFree() = %v, got %v", tt.description, tt.expectedIsFree, msg.IsFree())
			}

			// Additional verification: check that the effective gas price is applied correctly
			if tt.effectiveGasPricePercentage == 0 {
				// When effective gas price is 0, the gas price should be 0
				if !msg.GasPrice().IsZero() {
					t.Errorf("Expected gas price to be 0 when effective gas price percentage is 0, got %v", msg.GasPrice())
				}
			}
		})
	}
}

// TestEffectiveGasPriceContractInvocation tests the specific case mentioned in the issue
func TestEffectiveGasPriceContractInvocation(t *testing.T) {
	t.Run("ContractInvocationWithZeroEffectiveGasPrice", func(t *testing.T) {
		// This test specifically addresses the customer issue:
		// - zkevm.effective-gas-price-contract-invocation: 0.0
		// - gasPrice: 0x0
		// - Should be marked as FREE and not get stuck in base fee pool

		// Generate a test key
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("Failed to generate key: %v", err)
		}

		// Create a contract invocation transaction
		txData := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce: 0,
				To:    &libcommon.Address{0x1}, // Contract address
				Gas:   100000,
				Value: uint256.NewInt(0),              // Zero value
				Data:  []byte{0xa9, 0x05, 0x9c, 0xbb}, // ERC20 transfer method signature
			},
			GasPrice: uint256.NewInt(0), // Zero gas price
		}

		// Sign the transaction
		signer := types.LatestSignerForChainID(big.NewInt(1))
		tx, err := types.SignNewTx(key, *signer, txData)
		if err != nil {
			t.Fatalf("Failed to sign transaction: %v", err)
		}

		// Create test configuration with FREE transactions allowed
		config := &chain.Config{
			ChainID:                 big.NewInt(1),
			HomesteadBlock:          big.NewInt(0),
			DAOForkBlock:            nil,
			ByzantiumBlock:          big.NewInt(0),
			ConstantinopleBlock:     big.NewInt(0),
			PetersburgBlock:         big.NewInt(0),
			IstanbulBlock:           big.NewInt(0),
			MuirGlacierBlock:        big.NewInt(0),
			BerlinBlock:             big.NewInt(0),
			LondonBlock:             big.NewInt(0),
			ArrowGlacierBlock:       big.NewInt(0),
			GrayGlacierBlock:        big.NewInt(0),
			MergeNetsplitBlock:      big.NewInt(0),
			ShanghaiTime:            big.NewInt(0),
			CancunTime:              big.NewInt(0),
			PragueTime:              big.NewInt(0),
			ForkID5DragonfruitBlock: big.NewInt(0), // Enable ForkID5Dragonfruit for effective gas price
			AllowFreeTransactions:   true,
		}

		// Create test header
		header := &types.Header{
			Number:     big.NewInt(1),
			Time:       1000,
			GasLimit:   30000000,
			BaseFee:    big.NewInt(1000000000), // 1 gwei base fee
			Difficulty: big.NewInt(0),
		}

		// Create mock state
		db := memdb.NewTestDB(t)
		txDB, err := db.BeginRw(context.TODO())
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		defer txDB.Rollback()

		ibs := state.New(state.NewPlainStateReader(txDB))

		// Create EVM
		evm := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, config, vm.Config{})

		// Create mock engine
		engine := &MockEngine{}

		// Test with 0% effective gas price for contract invocation
		effectiveGasPricePercentage := uint8(0) // This simulates zkevm.effective-gas-price-contract-invocation: 0.0

		msg, _, err := GetTxContext(config, engine, ibs, header, tx, evm, effectiveGasPricePercentage)
		if err != nil {
			t.Fatalf("GetTxContext failed: %v", err)
		}

		// The transaction should be marked as FREE
		if !msg.IsFree() {
			t.Error("Contract invocation with 0% effective gas price should be marked as FREE")
		}

		// The gas price should be 0
		if !msg.GasPrice().IsZero() {
			t.Errorf("Expected gas price to be 0, got %v", msg.GasPrice())
		}

		// The fee cap should be 0
		if !msg.FeeCap().IsZero() {
			t.Errorf("Expected fee cap to be 0, got %v", msg.FeeCap())
		}

		t.Logf("✅ Contract invocation with 0%% effective gas price correctly marked as FREE")
		t.Logf("   Gas Price: %v", msg.GasPrice())
		t.Logf("   Fee Cap: %v", msg.FeeCap())
		t.Logf("   Is Free: %v", msg.IsFree())
	})
}

// TestEffectiveGasPriceCalculation tests the CalculateEffectiveGas function
func TestEffectiveGasPriceCalculation(t *testing.T) {
	tests := []struct {
		name                        string
		gasPrice                    *uint256.Int
		effectiveGasPricePercentage uint8
		expectedGasPrice            *uint256.Int
	}{
		{
			name:                        "ZeroEffectiveGasPrice_ZeroGasPrice",
			gasPrice:                    uint256.NewInt(0),
			effectiveGasPricePercentage: 0,
			expectedGasPrice:            uint256.NewInt(0),
		},
		{
			name:                        "ZeroEffectiveGasPrice_NonZeroGasPrice",
			gasPrice:                    uint256.NewInt(1000000000), // 1 gwei
			effectiveGasPricePercentage: 0,
			expectedGasPrice:            uint256.NewInt(0),
		},
		{
			name:                        "FiftyPercentEffectiveGasPrice",
			gasPrice:                    uint256.NewInt(1000000000), // 1 gwei
			effectiveGasPricePercentage: 127,                        // 50% of 255
			expectedGasPrice:            uint256.NewInt(500000000),  // 0.5 gwei
		},
		{
			name:                        "HundredPercentEffectiveGasPrice",
			gasPrice:                    uint256.NewInt(1000000000), // 1 gwei
			effectiveGasPricePercentage: 255,                        // 100% of 255
			expectedGasPrice:            uint256.NewInt(1000000000), // 1 gwei
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateEffectiveGas(tt.gasPrice, tt.effectiveGasPricePercentage)

			if !result.Eq(tt.expectedGasPrice) {
				t.Errorf("CalculateEffectiveGas(%v, %d) = %v, expected %v",
					tt.gasPrice, tt.effectiveGasPricePercentage, result, tt.expectedGasPrice)
			}
		})
	}
}
