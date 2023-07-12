// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

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
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	trace_logger "github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/erigon/turbo/trie"
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
	var getTracer func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error)

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
		getTracer = func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error) {
			if prevFile != nil {
				prevFile.Close()
			}
			traceFile, err2 := os.Create(path.Join(baseDir, fmt.Sprintf("trace-%d-%v.jsonl", txIndex, txHash.String())))
			if err2 != nil {
				return nil, NewError(ErrorIO, fmt.Errorf("failed creating trace-file: %v", err2))
			}
			prevFile = traceFile
			return trace_logger.NewJSONLogger(logConfig, traceFile), nil
		}
	} else {
		getTracer = func(txIndex int, txHash libcommon.Hash) (tracer vm.EVMLogger, err error) {
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
		Debug:         ctx.Bool(TraceFlag.Name),
		StatelessExec: true,
	}
	// Construct the chainconfig
	var chainConfig *chain.Config
	if cConf, extraEips, err1 := tests.GetChainConfig(ctx.String(ForknameFlag.Name)); err1 != nil {
		return NewError(ErrorVMConfig, fmt.Errorf("failed constructing chain configuration: %v", err1))
	} else { //nolint:golint
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
		return NewError(ErrorVMConfig, errors.New("Shanghai config but missing 'withdrawals' in env section"))
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

	var hashError error
	getHash := func(num uint64) libcommon.Hash {
		if prestate.Env.BlockHashes == nil {
			hashError = fmt.Errorf("getHash(%d) invoked, no blockhashes provided", num)
			return libcommon.Hash{}
		}
		h, ok := prestate.Env.BlockHashes[math.HexOrDecimal64(num)]
		if !ok {
			hashError = fmt.Errorf("getHash(%d) invoked, blockhash for that block not provided", num)
		}
		return h
	}

	_, db, _ := temporal.NewTestDB(nil, datadir.New(""), nil)
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	reader, writer := MakePreState(chainConfig.Rules(0, 0), tx, prestate.Pre)
	// Merge engine can be used for pre-merge blocks as well, as it
	// redirects to the ethash engine based on the block number
	engine := merge.New(&ethash.FakeEthash{})

	result, err := core.ExecuteBlockEphemerally(chainConfig, &vmConfig, getHash, engine, block, reader, writer, nil, getTracer, log.New("t8ntool"))

	if hashError != nil {
		return NewError(ErrorMissingBlockhash, fmt.Errorf("blockhash error: %v", err))
	}

	if err != nil {
		return fmt.Errorf("error on EBE: %w", err)
	}

	// state root calculation
	root, err := CalculateStateRoot(tx)
	if err != nil {
		return err
	}
	result.StateRoot = *root

	// Dump the execution result
	body, _ := rlp.EncodeToBytes(txs)
	collector := make(Alloc)

	historyV3, err := kvcfg.HistoryV3.Enabled(tx)
	if err != nil {
		return err
	}
	dumper := state.NewDumper(tx, prestate.Env.Number, historyV3)
	dumper.DumpToCollector(collector, false, false, libcommon.Address{}, 0)
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
		Key *libcommon.Hash `json:"secretKey"`
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
	var txJson jsonrpc.RPCTransaction

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

func getTransaction(txJson jsonrpc.RPCTransaction) (types.Transaction, error) {
	gasPrice, value := uint256.NewInt(0), uint256.NewInt(0)
	var overflow bool
	var chainId *uint256.Int

	if txJson.Value != nil {
		value, overflow = uint256.FromBig((*big.Int)(txJson.Value))
		if overflow {
			return nil, fmt.Errorf("value field caused an overflow (uint256)")
		}
	}

	if txJson.GasPrice != nil {
		gasPrice, overflow = uint256.FromBig((*big.Int)(txJson.GasPrice))
		if overflow {
			return nil, fmt.Errorf("gasPrice field caused an overflow (uint256)")
		}
	}

	if txJson.ChainID != nil {
		chainId, overflow = uint256.FromBig((*big.Int)(txJson.ChainID))
		if overflow {
			return nil, fmt.Errorf("chainId field caused an overflow (uint256)")
		}
	}

	switch txJson.Type {
	case types.LegacyTxType, types.AccessListTxType:
		var toAddr = libcommon.Address{}
		if txJson.To != nil {
			toAddr = *txJson.To
		}
		legacyTx := types.NewTransaction(uint64(txJson.Nonce), toAddr, value, uint64(txJson.Gas), gasPrice, txJson.Input)
		legacyTx.V.SetFromBig(txJson.V.ToInt())
		legacyTx.S.SetFromBig(txJson.S.ToInt())
		legacyTx.R.SetFromBig(txJson.R.ToInt())

		if txJson.Type == types.AccessListTxType {
			accessListTx := types.AccessListTx{
				LegacyTx:   *legacyTx,
				ChainID:    chainId,
				AccessList: *txJson.Accesses,
			}

			return &accessListTx, nil
		} else {
			return legacyTx, nil
		}

	case types.DynamicFeeTxType:
		var tip *uint256.Int
		var feeCap *uint256.Int
		if txJson.Tip != nil {
			tip, overflow = uint256.FromBig((*big.Int)(txJson.Tip))
			if overflow {
				return nil, fmt.Errorf("maxPriorityFeePerGas field caused an overflow (uint256)")
			}
		}

		if txJson.FeeCap != nil {
			feeCap, overflow = uint256.FromBig((*big.Int)(txJson.FeeCap))
			if overflow {
				return nil, fmt.Errorf("maxFeePerGas field caused an overflow (uint256)")
			}
		}

		dynamicFeeTx := types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce: uint64(txJson.Nonce),
				To:    txJson.To,
				Value: value,
				Gas:   uint64(txJson.Gas),
				Data:  txJson.Input,
			},
			ChainID:    chainId,
			Tip:        tip,
			FeeCap:     feeCap,
			AccessList: *txJson.Accesses,
		}

		dynamicFeeTx.V.SetFromBig(txJson.V.ToInt())
		dynamicFeeTx.S.SetFromBig(txJson.S.ToInt())
		dynamicFeeTx.R.SetFromBig(txJson.R.ToInt())

		return &dynamicFeeTx, nil

	default:
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
// and secondly to read them with the standard tx json format
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

type Alloc map[libcommon.Address]types.GenesisAccount

func (g Alloc) OnRoot(libcommon.Hash) {}

func (g Alloc) OnAccount(addr libcommon.Address, dumpAccount state.DumpAccount) {
	balance, _ := new(big.Int).SetString(dumpAccount.Balance, 10)
	var storage map[libcommon.Hash]libcommon.Hash
	if dumpAccount.Storage != nil {
		storage = make(map[libcommon.Hash]libcommon.Hash)
		for k, v := range dumpAccount.Storage {
			storage[libcommon.HexToHash(k)] = libcommon.HexToHash(v)
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
func dispatchOutput(ctx *cli.Context, baseDir string, result *core.EphemeralExecResult, alloc Alloc, body hexutility.Bytes) error {
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
	header.Number = big.NewInt(int64(env.Number))
	header.Time = env.Timestamp
	header.BaseFee = env.BaseFee
	header.MixDigest = env.MixDigest

	header.UncleHash = env.UncleHash
	header.WithdrawalsHash = env.WithdrawalsHash

	return &header
}

func CalculateStateRoot(tx kv.RwTx) (*libcommon.Hash, error) {
	// Generate hashed state
	c, err := tx.RwCursor(kv.PlainState)
	if err != nil {
		return nil, err
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
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
			if err = tx.Put(kv.HashedStorage, newK, common.CopyBytes(v)); err != nil {
				return nil, fmt.Errorf("insert hashed key: %w", err)
			}
		} else {
			if err = tx.Put(kv.HashedAccounts, newK, common.CopyBytes(v)); err != nil {
				return nil, fmt.Errorf("insert hashed key: %w", err)
			}
		}
	}
	c.Close()
	root, err := trie.CalcRoot("", tx)
	if err != nil {
		return nil, err
	}

	return &root, nil
}
