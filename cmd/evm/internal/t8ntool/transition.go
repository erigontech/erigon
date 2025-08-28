// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package t8ntool

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"path/filepath"

	"github.com/holiman/uint256"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/eth/consensuschain"
	trace_logger "github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus/ethash"
	"github.com/erigontech/erigon/execution/consensus/merge"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/tests"
)

const (
	ErrorEVM              = 2
	ErrorVMConfig         = 3
	ErrorMissingBlockhash = 4

	ErrorJson = 10
	ErrorIO   = 11

	stdinSelector = "stdin"
)

type NumberedError struct {
	errorCode int
	err       error
}

func NewError(errorCode int, err error) *NumberedError {
	return &NumberedError{errorCode, err}
}

func (n *NumberedError) Error() string {
	return fmt.Sprintf("ERROR(%d): %v", n.errorCode, n.err.Error())
}

func (n *NumberedError) ExitCode() int {
	return n.errorCode
}

// compile-time conformance test
var (
	_ cli.ExitCoder = (*NumberedError)(nil)
)

type input struct {
	Alloc types.GenesisAlloc `json:"alloc,omitempty"`
	Env   *stEnv             `json:"env,omitempty"`
	Txs   []*txWithKey       `json:"txs,omitempty"`
}

func Main(ctx *cli.Context) error {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	var (
		err     error
		baseDir = ""
	)
	var getTracer func(txIndex int, txHash common.Hash) (*tracing.Hooks, error)

	// If user specified a basedir, make sure it exists
	if ctx.IsSet(OutputBasedir.Name) {
		if base := ctx.String(OutputBasedir.Name); len(base) > 0 {
			err2 := os.MkdirAll(base, 0755) // //rw-r--r--
			if err2 != nil {
				return NewError(ErrorIO, fmt.Errorf("failed creating output basedir: %v", err2))
			}
			baseDir = base
		}
	}
	if ctx.Bool(TraceFlag.Name) {
		// Configure the EVM logger
		logConfig := &trace_logger.LogConfig{
			DisableStack:      ctx.Bool(TraceDisableStackFlag.Name),
			DisableMemory:     ctx.Bool(TraceDisableMemoryFlag.Name),
			DisableReturnData: ctx.Bool(TraceDisableReturnDataFlag.Name),
			Debug:             true,
		}
		var prevFile *os.File
		// This one closes the last file
		defer func() {
			if prevFile != nil {
				prevFile.Close()
			}
		}()
		getTracer = func(txIndex int, txHash common.Hash) (*tracing.Hooks, error) {
			if prevFile != nil {
				prevFile.Close()
			}
			traceFile, err2 := os.Create(path.Join(baseDir, fmt.Sprintf("trace-%d-%v.jsonl", txIndex, txHash.String())))
			if err2 != nil {
				return nil, NewError(ErrorIO, fmt.Errorf("failed creating trace-file: %v", err2))
			}
			prevFile = traceFile
			return trace_logger.NewJSONLogger(logConfig, traceFile).Tracer().Hooks, nil
		}
	} else {
		getTracer = func(txIndex int, txHash common.Hash) (tracer *tracing.Hooks, err error) {
			return nil, nil
		}
	}
	// We need to load three things: alloc, env and transactions. May be either in
	// stdin input or in files.
	// Check if anything needs to be read from stdin
	var (
		prestate Prestate
		txs      types.Transactions // txs to apply
		allocStr = ctx.String(InputAllocFlag.Name)

		envStr    = ctx.String(InputEnvFlag.Name)
		txStr     = ctx.String(InputTxsFlag.Name)
		inputData = &input{}
	)
	// Figure out the prestate alloc
	if allocStr == stdinSelector || envStr == stdinSelector || txStr == stdinSelector {
		decoder := json.NewDecoder(os.Stdin)
		decoder.Decode(inputData) //nolint:errcheck
	}
	if allocStr != stdinSelector {
		inFile, err1 := os.Open(allocStr)
		if err1 != nil {
			return NewError(ErrorIO, fmt.Errorf("failed reading alloc file: %v", err1))
		}
		defer inFile.Close()
		decoder := json.NewDecoder(inFile)
		if err = decoder.Decode(&inputData.Alloc); err != nil {
			return NewError(ErrorJson, fmt.Errorf("failed unmarshaling alloc-file: %v", err))
		}
	}

	prestate.Pre = inputData.Alloc

	// Set the block environment
	if envStr != stdinSelector {
		inFile, err1 := os.Open(envStr)
		if err1 != nil {
			return NewError(ErrorIO, fmt.Errorf("failed reading env file: %v", err1))
		}
		defer inFile.Close()
		decoder := json.NewDecoder(inFile)
		var env stEnv
		if err = decoder.Decode(&env); err != nil {
			return NewError(ErrorJson, fmt.Errorf("failed unmarshaling env-file: %v", err))
		}
		inputData.Env = &env
	}
	prestate.Env = *inputData.Env

	vmConfig := vm.Config{
		Tracer:        nil,
		StatelessExec: true,
	}
	// Construct the chainconfig
	var chainConfig *chain.Config
	if cConf, extraEips, err1 := tests.GetChainConfig(ctx.String(ForknameFlag.Name)); err1 != nil {
		return NewError(ErrorVMConfig, fmt.Errorf("failed constructing chain configuration: %v", err1))
	} else {
		chainConfig = cConf
		vmConfig.ExtraEips = extraEips
	}
	// Set the chain id
	chainConfig.ChainID = big.NewInt(ctx.Int64(ChainIDFlag.Name))

	var txsWithKeys []*txWithKey
	if txStr != stdinSelector {
		inFile, err1 := os.Open(txStr)
		if err1 != nil {
			return NewError(ErrorIO, fmt.Errorf("failed reading txs file: %v", err1))
		}
		defer inFile.Close()
		decoder := json.NewDecoder(inFile)
		if err = decoder.Decode(&txsWithKeys); err != nil {
			return NewError(ErrorJson, fmt.Errorf("failed unmarshaling txs-file: %v", err))
		}
	} else {
		txsWithKeys = inputData.Txs
	}
	// We may have to sign the transactions.
	signer := types.MakeSigner(chainConfig, prestate.Env.Number, prestate.Env.Timestamp)

	if txs, err = signUnsignedTransactions(txsWithKeys, *signer); err != nil {
		return NewError(ErrorJson, fmt.Errorf("failed signing transactions: %v", err))
	}

	eip1559 := chainConfig.IsLondon(prestate.Env.Number)
	// Sanity check, to not `panic` in state_transition
	if eip1559 {
		if prestate.Env.BaseFee == nil {
			return NewError(ErrorVMConfig, errors.New("EIP-1559 config but missing 'currentBaseFee' in env section"))
		}
	} else {
		prestate.Env.Random = nil
	}

	if chainConfig.IsShanghai(prestate.Env.Timestamp) && prestate.Env.Withdrawals == nil {
		return NewError(ErrorVMConfig, errors.New("shanghai config but missing 'withdrawals' in env section"))
	}

	isMerged := chainConfig.TerminalTotalDifficulty != nil && chainConfig.TerminalTotalDifficulty.BitLen() == 0
	env := prestate.Env
	if isMerged {
		// post-merge:
		// - random must be supplied
		// - difficulty must be zero
		switch {
		case env.Random == nil:
			return NewError(ErrorVMConfig, errors.New("post-merge requires currentRandom to be defined in env"))
		case env.Difficulty != nil && env.Difficulty.BitLen() != 0:
			return NewError(ErrorVMConfig, errors.New("post-merge difficulty must be zero (or omitted) in env"))
		}
		prestate.Env.Difficulty = nil
	} else if env.Difficulty == nil {
		// If difficulty was not provided by caller, we need to calculate it.
		switch {
		case env.ParentDifficulty == nil:
			return NewError(ErrorVMConfig, errors.New("currentDifficulty was not provided, and cannot be calculated due to missing parentDifficulty"))
		case env.Number == 0:
			return NewError(ErrorVMConfig, errors.New("currentDifficulty needs to be provided for block number 0"))
		case env.Timestamp <= env.ParentTimestamp:
			return NewError(ErrorVMConfig, fmt.Errorf("currentDifficulty cannot be calculated -- currentTime (%d) needs to be after parent time (%d)",
				env.Timestamp, env.ParentTimestamp))
		}
		prestate.Env.Difficulty = calcDifficulty(chainConfig, env.Number, env.Timestamp,
			env.ParentTimestamp, env.ParentDifficulty, env.ParentUncleHash)
	}

	// manufacture block from above inputs
	header := NewHeader(prestate.Env)

	var ommerHeaders = make([]*types.Header, len(prestate.Env.Ommers))
	header.Number.Add(header.Number, big.NewInt(int64(len(prestate.Env.Ommers))))
	for i, ommer := range prestate.Env.Ommers {
		var ommerN big.Int
		ommerN.SetUint64(header.Number.Uint64() - ommer.Delta)
		ommerHeaders[i] = &types.Header{Coinbase: ommer.Address, Number: &ommerN}
	}
	block := types.NewBlock(header, txs, ommerHeaders, nil /* receipts */, prestate.Env.Withdrawals)

	getHash := func(num uint64) (common.Hash, error) {
		if prestate.Env.BlockHashes == nil {
			return common.Hash{}, fmt.Errorf("getHash(%d) invoked, no blockhashes provided", num)
		}
		h, ok := prestate.Env.BlockHashes[math.HexOrDecimal64(num)]
		if !ok {
			return common.Hash{}, fmt.Errorf("getHash(%d) invoked, blockhash for that block not provided", num)
		}
		return h, nil
	}

	db := temporaltest.NewTestDB(nil, datadir.New(""))
	defer db.Close()

	tx, err := db.BeginTemporalRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sd, err := dbstate.NewSharedDomains(tx, log.New())
	if err != nil {
		return err
	}
	defer sd.Close()

	blockNum, txNum := uint64(0), uint64(0)
	sd.SetTxNum(txNum)
	sd.SetBlockNum(blockNum)
	reader, writer := MakePreState((&evmtypes.BlockContext{}).Rules(chainConfig), tx, sd, prestate.Pre, blockNum, txNum)
	blockNum, txNum = uint64(1), uint64(2)
	sd.SetTxNum(txNum)
	sd.SetBlockNum(blockNum)

	// Merge engine can be used for pre-merge blocks as well, as it
	// redirects to the ethash engine based on the block number
	engine := merge.New(&ethash.FakeEthash{})

	t8logger := log.New("t8ntool")
	chainReader := consensuschain.NewReader(chainConfig, tx, nil, t8logger)
	result, err := core.ExecuteBlockEphemerally(chainConfig, &vmConfig, getHash, engine, block, reader, writer, chainReader, getTracer, t8logger)

	if err != nil {
		return fmt.Errorf("error on EBE: %w", err)
	}

	// state root calculation
	root, err := CalculateStateRoot(tx, blockNum, txNum)
	if err != nil {
		return err
	}
	result.StateRoot = *root

	// Dump the execution result
	body, _ := rlp.EncodeToBytes(txs)
	collector := make(Alloc)

	dumper := state.NewDumper(tx, rawdbv3.TxNums, prestate.Env.Number)
	dumper.DumpToCollector(collector, false, false, common.Address{}, 0)
	return dispatchOutput(ctx, baseDir, result, collector, body)
}

// txWithKey is a helper-struct, to allow us to use the types.Transaction along with
// a `secretKey`-field, for input
type txWithKey struct {
	key *ecdsa.PrivateKey
	tx  types.Transaction
}

func (t *txWithKey) UnmarshalJSON(input []byte) error {
	// Read the secretKey, if present
	type sKey struct {
		Key *common.Hash `json:"secretKey"`
	}
	var key sKey
	if err := json.Unmarshal(input, &key); err != nil {
		return err
	}
	if key.Key != nil {
		k := key.Key.Hex()[2:]
		if ecdsaKey, err := crypto.HexToECDSA(k); err == nil {
			t.key = ecdsaKey
		} else {
			return err
		}
	}

	// Now, read the transaction itself
	var txJson ethapi.RPCTransaction

	if err := json.Unmarshal(input, &txJson); err != nil {
		return err
	}

	// assemble transaction
	tx, err := getTransaction(txJson)
	if err != nil {
		return err
	}
	t.tx = tx
	return nil
}

func getTransaction(txJson ethapi.RPCTransaction) (types.Transaction, error) {
	gasPrice, value := uint256.NewInt(0), uint256.NewInt(0)
	var overflow bool
	var chainId *uint256.Int

	if txJson.Value != nil {
		value, overflow = uint256.FromBig(txJson.Value.ToInt())
		if overflow {
			return nil, errors.New("value field caused an overflow (uint256)")
		}
	}

	if txJson.GasPrice != nil {
		gasPrice, overflow = uint256.FromBig(txJson.GasPrice.ToInt())
		if overflow {
			return nil, errors.New("gasPrice field caused an overflow (uint256)")
		}
	}

	if txJson.ChainID != nil {
		chainId, overflow = uint256.FromBig(txJson.ChainID.ToInt())
		if overflow {
			return nil, errors.New("chainId field caused an overflow (uint256)")
		}
	}

	commonTx := types.CommonTx{
		Nonce:    uint64(txJson.Nonce),
		To:       txJson.To,
		Value:    value,
		GasLimit: uint64(txJson.Gas),
		Data:     txJson.Input,
	}

	commonTx.V.SetFromBig(txJson.V.ToInt())
	commonTx.R.SetFromBig(txJson.R.ToInt())
	commonTx.S.SetFromBig(txJson.S.ToInt())
	if txJson.Type == types.LegacyTxType || txJson.Type == types.AccessListTxType {
		if txJson.Type == types.LegacyTxType {
			return &types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    uint64(txJson.Nonce),
					To:       txJson.To,
					Value:    value,
					GasLimit: uint64(txJson.Gas),
					Data:     txJson.Input,
				},
				GasPrice: gasPrice,
			}, nil
		}

		return &types.AccessListTx{
			LegacyTx: types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    uint64(txJson.Nonce),
					To:       txJson.To,
					Value:    value,
					GasLimit: uint64(txJson.Gas),
					Data:     txJson.Input,
				},
				GasPrice: gasPrice,
			},
			ChainID:    chainId,
			AccessList: *txJson.Accesses,
		}, nil
	} else if txJson.Type == types.DynamicFeeTxType || txJson.Type == types.SetCodeTxType {
		var tipCap *uint256.Int
		var feeCap *uint256.Int
		if txJson.MaxPriorityFeePerGas != nil {
			tipCap, overflow = uint256.FromBig(txJson.MaxPriorityFeePerGas.ToInt())
			if overflow {
				return nil, errors.New("maxPriorityFeePerGas field caused an overflow (uint256)")
			}
		}

		if txJson.MaxFeePerGas != nil {
			feeCap, overflow = uint256.FromBig(txJson.MaxFeePerGas.ToInt())
			if overflow {
				return nil, errors.New("maxFeePerGas field caused an overflow (uint256)")
			}
		}

		if txJson.Type == types.DynamicFeeTxType {
			return &types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    uint64(txJson.Nonce),
					To:       txJson.To,
					Value:    value,
					GasLimit: uint64(txJson.Gas),
					Data:     txJson.Input,
				},
				ChainID:    chainId,
				TipCap:     tipCap,
				FeeCap:     feeCap,
				AccessList: *txJson.Accesses,
			}, nil
		}

		auths := make([]types.Authorization, 0)
		for _, auth := range *txJson.Authorizations {
			a, err := auth.ToAuthorization()
			if err != nil {
				return nil, err
			}
			auths = append(auths, a)
		}

		return &types.SetCodeTransaction{
			// it's ok to copy here - because it's constructor of object - no parallel access yet
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    uint64(txJson.Nonce),
					To:       txJson.To,
					Value:    value,
					GasLimit: uint64(txJson.Gas),
					Data:     txJson.Input,
				},
				ChainID:    chainId,
				TipCap:     tipCap,
				FeeCap:     feeCap,
				AccessList: *txJson.Accesses,
			},
			Authorizations: auths,
		}, nil
	} else {
		return nil, nil
	}
}

// signUnsignedTransactions converts the input txs to canonical transactions.
//
// The transactions can have two forms, either
//  1. unsigned or
//  2. signed
//
// For (1), r, s, v, need so be zero, and the `secretKey` needs to be set.
// If so, we sign it here and now, with the given `secretKey`
// If the condition above is not met, then it's considered a signed transaction.
//
// To manage this, we read the transactions twice, first trying to read the secretKeys,
// and secondly to read them with the standard txn json format
func signUnsignedTransactions(txs []*txWithKey, signer types.Signer) (types.Transactions, error) {
	var signedTxs []types.Transaction
	for i, txWithKey := range txs {
		tx := txWithKey.tx
		key := txWithKey.key
		v, r, s := tx.RawSignatureValues()
		if key != nil && v.IsZero() && r.IsZero() && s.IsZero() {
			// This transaction needs to be signed
			signed, err := types.SignTx(tx, signer, key)
			if err != nil {
				return nil, NewError(ErrorJson, fmt.Errorf("tx %d: failed to sign tx: %v", i, err))
			}
			signedTxs = append(signedTxs, signed)
		} else {
			// Already signed
			signedTxs = append(signedTxs, tx)
		}
	}
	return signedTxs, nil
}

type Alloc map[common.Address]types.GenesisAccount

func (g Alloc) OnRoot(common.Hash) {}

func (g Alloc) OnAccount(addr common.Address, dumpAccount state.DumpAccount) {
	balance, _ := new(big.Int).SetString(dumpAccount.Balance, 10)
	var storage map[common.Hash]common.Hash
	if dumpAccount.Storage != nil {
		storage = make(map[common.Hash]common.Hash)
		for k, v := range dumpAccount.Storage {
			storage[common.HexToHash(k)] = common.HexToHash(v)
		}
	}
	genesisAccount := types.GenesisAccount{
		Code:    dumpAccount.Code,
		Storage: storage,
		Balance: balance,
		Nonce:   dumpAccount.Nonce,
	}
	g[addr] = genesisAccount
}

// saveFile marshalls the object to the given file
func saveFile(baseDir, filename string, data interface{}) error {
	b, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return NewError(ErrorJson, fmt.Errorf("failed marshalling output: %v", err))
	}
	location := filepath.Join(baseDir, filename)
	if err = os.WriteFile(location, b, 0644); err != nil { //nolint:gosec
		return NewError(ErrorIO, fmt.Errorf("failed writing output: %v", err))
	}
	log.Info("Wrote file", "file", location)
	return nil
}

// dispatchOutput writes the output data to either stderr or stdout, or to the specified
// files
func dispatchOutput(ctx *cli.Context, baseDir string, result *core.EphemeralExecResult, alloc Alloc, body hexutil.Bytes) error {
	stdOutObject := make(map[string]interface{})
	stdErrObject := make(map[string]interface{})
	dispatch := func(baseDir, fName, name string, obj interface{}) error {
		switch fName {
		case "stdout":
			stdOutObject[name] = obj
		case "stderr":
			stdErrObject[name] = obj
		case "":
			// don't save
		default: // save to file
			if err := saveFile(baseDir, fName, obj); err != nil {
				return err
			}
		}
		return nil
	}
	if err := dispatch(baseDir, ctx.String(OutputAllocFlag.Name), "alloc", alloc); err != nil {
		return err
	}
	if err := dispatch(baseDir, ctx.String(OutputResultFlag.Name), "result", result); err != nil {
		return err
	}
	if err := dispatch(baseDir, ctx.String(OutputBodyFlag.Name), "body", body); err != nil {
		return err
	}
	if len(stdOutObject) > 0 {
		b, err := json.MarshalIndent(stdOutObject, "", " ")
		if err != nil {
			return NewError(ErrorJson, fmt.Errorf("failed marshalling output: %v", err))
		}
		os.Stdout.Write(b)
	}
	if len(stdErrObject) > 0 {
		b, err := json.MarshalIndent(stdErrObject, "", " ")
		if err != nil {
			return NewError(ErrorJson, fmt.Errorf("failed marshalling output: %v", err))
		}
		os.Stderr.Write(b)
	}
	return nil
}

func NewHeader(env stEnv) *types.Header {
	var header types.Header
	header.Coinbase = env.Coinbase
	header.Difficulty = env.Difficulty
	header.GasLimit = env.GasLimit
	header.Number = new(big.Int).SetUint64(env.Number)
	header.Time = env.Timestamp
	header.BaseFee = env.BaseFee
	header.MixDigest = env.MixDigest

	header.UncleHash = env.UncleHash
	header.WithdrawalsHash = env.WithdrawalsHash
	header.RequestsHash = env.RequestsHash

	return &header
}

func CalculateStateRoot(tx kv.TemporalRwTx, blockNum uint64, txNum uint64) (*common.Hash, error) {
	// Generate hashed state
	c, err := tx.RwCursor(kv.PlainState)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	domains, err := dbstate.NewSharedDomains(tx, log.New())
	if err != nil {
		return nil, fmt.Errorf("NewSharedDomains: %w", err)
	}
	defer domains.Close()

	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, fmt.Errorf("interate over plain state: %w", err)
		}
		var newK []byte
		if len(k) == length.Addr {
			newK = make([]byte, length.Hash)
		} else {
			newK = make([]byte, length.Hash*2+length.Incarnation)
		}
		h.Sha.Reset()
		//nolint:errcheck
		h.Sha.Write(k[:length.Addr])
		//nolint:errcheck
		h.Sha.Read(newK[:length.Hash])
		if len(k) > length.Addr {
			copy(newK[length.Hash:], k[length.Addr:length.Addr+length.Incarnation])
			h.Sha.Reset()
			//nolint:errcheck
			h.Sha.Write(k[length.Addr+length.Incarnation:])
			//nolint:errcheck
			h.Sha.Read(newK[length.Hash+length.Incarnation:])
			if err = tx.Put(kv.HashedStorageDeprecated, newK, common.CopyBytes(v)); err != nil {
				return nil, fmt.Errorf("insert hashed key: %w", err)
			}
		} else {
			if err = tx.Put(kv.HashedAccountsDeprecated, newK, common.CopyBytes(v)); err != nil {
				return nil, fmt.Errorf("insert hashed key: %w", err)
			}
		}
	}
	c.Close()
	root, err := domains.ComputeCommitment(context.Background(), true, blockNum, txNum, "")
	if err != nil {
		return nil, err
	}
	hashRoot := common.Hash{}
	hashRoot.SetBytes(root)

	return &hashRoot, nil
}
