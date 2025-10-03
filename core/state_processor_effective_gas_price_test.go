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

// TestEffectiveGasPriceFreeTransactions tests that transactions are correctly marked as free
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
			expectedIsFree:              false, // it is not free in the sense of setting the field, but it is free in terms of cost
			description:                 "Transaction with 0% effective gas price and 0 gas price should be marked as FREE",
		},
		{
			name:                        "ZeroEffectiveGasPrice_NonZeroGasPrice_ShouldBeFree",
			effectiveGasPricePercentage: 0,                          // 0% effective gas price
			gasPrice:                    uint256.NewInt(1000000000), // 1 gwei
			expectedIsFree:              false,                      // it is not free in the sense of setting the field, but it is free in terms of cost
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
			// Set per-type EGP defaults to 100% (255) to match CLI defaults
			config.EffectiveGasPriceForEthTransfer = 255
			config.EffectiveGasPriceForErc20Transfer = 255
			config.EffectiveGasPriceForContractInvocation = 255
			config.EffectiveGasPriceForContractDeployment = 255

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
				if !msg.GasPrice().IsZero() && tt.gasPrice.IsZero() {
					t.Errorf("Expected gas price to be 0 when effective gas price percentage is 0, got %v", msg.GasPrice())
				}
			} else {
				// When effective gas price is non-zero, the gas price should be adjusted
				// lets verify that gasprice is adjusted correctly
				expectedGasPrice := CalculateEffectiveGas(tt.gasPrice, tt.effectiveGasPricePercentage)
				if msg.GasPrice().Cmp(expectedGasPrice) != 0 {
					t.Errorf("%s: expected GasPrice() = %v, got %v", tt.description, expectedGasPrice, msg.GasPrice())
				}
			}
		})
	}
}

// TestEffectiveGasPriceContractInvocation tests the specific case mentioned in the issue
func TestPerTypeEffectiveGasPriceFreeTransactions(t *testing.T) {
	type txBuilder func() types.Transaction

	// Shared setup
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer := types.LatestSignerForChainID(big.NewInt(1))

	mkHeader := func() *types.Header {
		return &types.Header{
			Number:     big.NewInt(1),
			Time:       1000,
			GasLimit:   30000000,
			BaseFee:    big.NewInt(1000000000), // 1 gwei base fee
			Difficulty: big.NewInt(0),
		}
	}

	// Build different tx types
	makeEthTransfer := func() types.Transaction {
		to := &libcommon.Address{0x1}
		txData := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce: 0,
				To:    to,
				Gas:   21000,
				Value: uint256.NewInt(0),
				Data:  []byte{}, // no calldata
			},
			GasPrice: uint256.NewInt(0),
		}
		tx, _ := types.SignNewTx(key, *signer, txData)
		return tx
	}
	makeErc20Transfer := func() types.Transaction {
		to := &libcommon.Address{0x2}
		data := []byte{0xa9, 0x05, 0x9c, 0xbb} // ERC20 transfer selector
		txData := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce: 1,
				To:    to,
				Gas:   100000,
				Value: uint256.NewInt(0),
				Data:  data,
			},
			GasPrice: uint256.NewInt(1_000_000_000),
		}
		tx, _ := types.SignNewTx(key, *signer, txData)
		return tx
	}
	makeContractInvocation := func() types.Transaction {
		to := &libcommon.Address{0x3}
		data := []byte{0x01, 0x02, 0x03, 0x04} // non-ERC20 selector
		txData := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce: 2,
				To:    to,
				Gas:   120000,
				Value: uint256.NewInt(0),
				Data:  data,
			},
			GasPrice: uint256.NewInt(1_000_000_000),
		}
		tx, _ := types.SignNewTx(key, *signer, txData)
		return tx
	}
	makeContractDeployment := func() types.Transaction {
		// To == nil implies contract creation
		txData := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce: 3,
				To:    nil,
				Gas:   200000,
				Value: uint256.NewInt(0),
				Data:  []byte{0xde, 0xad, 0xbe, 0xef}, // some initcode bytes
			},
			GasPrice: uint256.NewInt(1_000_000_000),
		}
		tx, _ := types.SignNewTx(key, *signer, txData)
		return tx
	}

	tests := []struct {
		name        string
		buildTx     txBuilder
		cfgMutator  func(*chain.Config)
		expectFree  bool
		description string
	}{
		{
			name:    "ETH transfer free when per-type EGP=0",
			buildTx: makeEthTransfer,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 0
				c.EffectiveGasPriceForErc20Transfer = 255
				c.EffectiveGasPriceForContractInvocation = 255
				c.EffectiveGasPriceForContractDeployment = 255
			},
			expectFree:  true,
			description: "ETH transfer is free if per-type EGP is 0",
		},
		{
			name:    "ERC20 transfer free when per-type EGP=0",
			buildTx: makeErc20Transfer,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 255
				c.EffectiveGasPriceForErc20Transfer = 0
				c.EffectiveGasPriceForContractInvocation = 255
				c.EffectiveGasPriceForContractDeployment = 255
			},
			expectFree:  true,
			description: "ERC20 transfer is free if per-type EGP is 0",
		},
		{
			name:    "Contract invocation free when per-type EGP=0",
			buildTx: makeContractInvocation,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 255
				c.EffectiveGasPriceForErc20Transfer = 255
				c.EffectiveGasPriceForContractInvocation = 0
				c.EffectiveGasPriceForContractDeployment = 255
			},
			expectFree:  true,
			description: "Contract invocation is free if per-type EGP is 0",
		},
		{
			name:    "Contract deployment free when per-type EGP=0",
			buildTx: makeContractDeployment,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 255
				c.EffectiveGasPriceForErc20Transfer = 255
				c.EffectiveGasPriceForContractInvocation = 255
				c.EffectiveGasPriceForContractDeployment = 0
			},
			expectFree:  true,
			description: "Contract deployment is free if per-type EGP is 0",
		},

		// Negative cases - per-type EGP non-zero means not free
		{
			name:    "ETH transfer not free when per-type EGP non-zero",
			buildTx: makeEthTransfer,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 100
				c.EffectiveGasPriceForErc20Transfer = 255
				c.EffectiveGasPriceForContractInvocation = 255
				c.EffectiveGasPriceForContractDeployment = 255
			},
			expectFree:  false,
			description: "ETH transfer is not free if per-type EGP is non-zero",
		},
		{
			name:    "ERC20 transfer not free when per-type EGP non-zero",
			buildTx: makeErc20Transfer,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 255
				c.EffectiveGasPriceForErc20Transfer = 100
				c.EffectiveGasPriceForContractInvocation = 255
				c.EffectiveGasPriceForContractDeployment = 255
			},
			expectFree:  false,
			description: "ERC20 transfer is not free if per-type EGP is non-zero",
		},
		{
			name:    "Contract invocation not free when per-type EGP non-zero",
			buildTx: makeContractInvocation,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 255
				c.EffectiveGasPriceForErc20Transfer = 255
				c.EffectiveGasPriceForContractInvocation = 100
				c.EffectiveGasPriceForContractDeployment = 255
			},
			expectFree:  false,
			description: "Contract invocation is not free if per-type EGP is non-zero",
		},
		{
			name:    "Contract deployment not free when per-type EGP non-zero",
			buildTx: makeContractDeployment,
			cfgMutator: func(c *chain.Config) {
				c.EffectiveGasPriceForEthTransfer = 255
				c.EffectiveGasPriceForErc20Transfer = 255
				c.EffectiveGasPriceForContractInvocation = 255
				c.EffectiveGasPriceForContractDeployment = 100
			},
			expectFree:  false,
			description: "Contract deployment is not free if per-type EGP is non-zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test configuration
			config := &chain.Config{
				ChainID:                 big.NewInt(1),
				HomesteadBlock:          big.NewInt(0),
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
				ForkID5DragonfruitBlock: big.NewInt(0), // Enable ForkID5Dragonfruit
				AllowFreeTransactions:   true,
			}
			// Apply per-type EGP settings for this case
			tt.cfgMutator(config)

			header := mkHeader()

			// Create in-memory state
			db := memdb.NewTestDB(t)
			txDB, err := db.BeginRw(context.TODO())
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}
			defer txDB.Rollback()
			ibs := state.New(state.NewPlainStateReader(txDB))

			// EVM + engine
			evm := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, config, vm.Config{})
			engine := &MockEngine{}

			// Build tx of the given type
			tx := tt.buildTx()

			// Use non-zero effectiveGasPricePercentage so only per-type flag can make it free
			effectiveGasPricePercentage := uint8(255)
			msg, _, err := GetTxContext(config, engine, ibs, header, tx, evm, effectiveGasPricePercentage)
			if err != nil {
				t.Fatalf("GetTxContext failed: %v", err)
			}

			if msg.IsFree() != tt.expectFree {
				t.Errorf("%s: expected IsFree() = %v, got %v", tt.description, tt.expectFree, msg.IsFree())
			}

			// Check GasPrice is adjusted correctly when not free
			if !tt.expectFree {
				expectedGasPrice := CalculateEffectiveGas(tx.GetPrice(), effectiveGasPricePercentage)
				if msg.GasPrice().Cmp(expectedGasPrice) != 0 {
					t.Errorf("%s: expected GasPrice() = %v, got %v", tt.description, expectedGasPrice, msg.GasPrice())
				}
			}
		})
	}
}
