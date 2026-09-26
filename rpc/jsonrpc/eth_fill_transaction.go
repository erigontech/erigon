// Copyright 2026 The Erigon Authors
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

package jsonrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/gasprice"
)

// FillTransaction implements eth_fillTransaction.
func (api *APIImpl) FillTransaction(ctx context.Context, args ethapi.CallArgs) (*ethapi.SignTransactionResult, error) {
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return nil, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	}
	// A set-code transaction has no gasPrice field, so honouring both would mean
	// silently reinterpreting gasPrice as maxFeePerGas and maxPriorityFeePerGas.
	if args.GasPrice != nil && args.AuthorizationList != nil {
		return nil, errors.New("both gasPrice and authorizationList specified")
	}
	if args.MaxFeePerBlobGas != nil && (*uint256.Int)(args.MaxFeePerBlobGas).IsZero() {
		return nil, errors.New("maxFeePerBlobGas, if specified, must be non-zero")
	}

	// The pinned view keeps ReadCurrentHeader and the gas-oracle fee defaults
	// on one head (including the in-flight overlay block); the nonce and
	// gas-estimate sub-calls still open their own txs.
	overlayTx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer overlayTx.Rollback()

	cc, err := api.chainConfig(ctx, overlayTx)
	if err != nil {
		return nil, err
	}
	head := rawdb.ReadCurrentHeader(overlayTx)
	if head == nil {
		return nil, errors.New("missing current header")
	}

	if args.Value == nil {
		args.Value = new(hexutil.U256)
	}

	if args.Nonce == nil {
		var nonce uint64
		if args.From != nil {
			pendingBlock := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
			count, err := api.GetTransactionCount(ctx, *args.From, &pendingBlock)
			if err != nil {
				return nil, err
			}
			nonce = uint64(*count)
		}
		args.Nonce = (*hexutil.Uint64)(&nonce)
	}

	if args.Data != nil && args.Input != nil && !bytes.Equal(*args.Data, *args.Input) {
		return nil, errors.New(`both "data" and "input" are set and not equal. Please use "input" to pass transaction call data`)
	}

	if args.Blobs != nil && args.AuthorizationList != nil {
		return nil, errors.New("both blobs and authorizationList specified")
	}
	sidecar, err := buildBlobSidecar(&args, cc.IsOsaka(head.Time))
	if err != nil {
		return nil, err
	}

	if args.BlobVersionedHashes != nil {
		if n := len(args.BlobVersionedHashes); n == 0 {
			return nil, errors.New("need at least 1 blob for a blob transaction")
		} else if n > params.MaxBlobsPerTxn {
			return nil, fmt.Errorf("too many blobs in transaction (have=%d, max=%d)", n, params.MaxBlobsPerTxn)
		}
	}

	if args.AuthorizationList != nil && len(args.AuthorizationList) == 0 {
		return nil, errors.New("need at least one authorization for a set-code transaction")
	}

	if args.To == nil {
		if args.BlobVersionedHashes != nil {
			return nil, errors.New(`missing "to" in blob transaction`)
		}
		hasData := (args.Input != nil && len(*args.Input) > 0) || (args.Data != nil && len(*args.Data) > 0)
		if !hasData {
			return nil, errors.New(`contract creation without any data provided`)
		}
		if args.AuthorizationList != nil {
			return nil, errors.New(`authorizationList provided for contract creation, but "to" field is missing`)
		}
	}

	if err := api.fillFeeDefaults(ctx, &args, head, overlayTx); err != nil {
		return nil, err
	}

	if args.BlobVersionedHashes != nil {
		if head.ExcessBlobGas == nil {
			return nil, errors.New("blob transactions not supported before Cancun")
		}
		if args.MaxFeePerBlobGas == nil {
			blobFee, err := misc.GetBlobGasPrice(cc, *head.ExcessBlobGas, head.Time)
			if err != nil {
				return nil, err
			}
			blobFeeCap, overflow := new(uint256.Int).AddOverflow(&blobFee, &blobFee)
			if overflow {
				return nil, fmt.Errorf("maxFeePerBlobGas overflow: 2*blobGasPrice (%v) exceeds 256 bits", &blobFee)
			}
			args.MaxFeePerBlobGas = (*hexutil.U256)(blobFeeCap)
		}
	}

	if args.ChainID == nil {
		args.ChainID = (*hexutil.U256)(new(uint256.Int).Set(cc.ChainID))
	} else if have := (*uint256.Int)(args.ChainID); !have.Eq(cc.ChainID) {
		return nil, fmt.Errorf("chainId does not match node's (have=%v, want=%v)", have, cc.ChainID)
	}

	if args.Gas == nil {
		estimated, err := api.EstimateGas(ctx, &args, &latestNumOrHash, nil, nil)
		if err != nil {
			return nil, err
		}
		args.Gas = &estimated
	}

	txn, err := args.ToTransaction(0, head.BaseFee)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if sidecar != nil {
		sidecar = txn.(*types.BlobTx).WithSidecar(sidecar)
		err = sidecar.MarshalBinaryWrapped(&buf)
	} else {
		err = txn.MarshalBinary(&buf)
	}
	if err != nil {
		return nil, err
	}
	return &ethapi.SignTransactionResult{
		Raw:     buf.Bytes(),
		Tx:      ethapi.NewRPCTransaction(txn, common.Hash{}, 0, 0, 0, nil),
		Sidecar: sidecar,
	}, nil
}

// buildBlobSidecar computes the missing commitments, proofs and versioned hashes
// from args.Blobs, or verifies the provided ones. It returns nil when no blobs are given.
func buildBlobSidecar(args *ethapi.CallArgs, cellProofs bool) (*types.BlobTxWrapper, error) {
	if args.Blobs == nil {
		return nil, nil
	}
	if args.Commitments == nil && args.Proofs != nil {
		return nil, errors.New("blob proofs provided while commitments were not")
	} else if args.Commitments != nil && args.Proofs == nil {
		return nil, errors.New("blob commitments provided while proofs were not")
	}

	n := len(args.Blobs)
	if n > params.MaxBlobsPerTxn {
		return nil, fmt.Errorf("too many blobs in transaction (have=%d, max=%d)", n, params.MaxBlobsPerTxn)
	}
	if args.BlobVersionedHashes != nil && len(args.BlobVersionedHashes) != n {
		return nil, fmt.Errorf("number of blobs and hashes mismatch (have=%d, want=%d)", len(args.BlobVersionedHashes), n)
	}
	if args.Commitments != nil && len(args.Commitments) != n {
		return nil, fmt.Errorf("number of blobs and commitments mismatch (have=%d, want=%d)", len(args.Commitments), n)
	}
	sidecar := &types.BlobTxWrapper{Blobs: make(types.Blobs, n)}
	proofLen := n
	if cellProofs {
		sidecar.WrapperVersion = 1
		proofLen = n * int(params.CellsPerExtBlob)
	}
	commitments, proofs := args.Commitments, args.Proofs
	if proofs != nil && len(proofs) != proofLen {
		if len(proofs) != n {
			return nil, fmt.Errorf("number of blobs and proofs mismatch (have=%d, want=%d)", len(proofs), proofLen)
		}
		// Blob proofs from pre-Osaka tooling are replaced by freshly computed cell proofs.
		commitments, proofs = nil, nil
	}

	for i, blob := range args.Blobs {
		if len(blob) != params.BlobSize {
			return nil, fmt.Errorf("blobs[%d]: invalid length %d, want %d", i, len(blob), params.BlobSize)
		}
		copy(sidecar.Blobs[i][:], blob)
	}

	var err error
	if commitments == nil {
		if sidecar.Commitments, _, sidecar.Proofs, err = sidecar.Blobs.ComputeCommitmentsAndProofs(); err != nil {
			return nil, err
		}
		if cellProofs {
			if sidecar.Proofs, err = sidecar.Blobs.ComputeCellProofs(); err != nil {
				return nil, err
			}
		}
	} else {
		if sidecar.Commitments, err = parse48[types.KZGCommitment]("commitments", commitments); err != nil {
			return nil, err
		}
		if sidecar.Proofs, err = parse48[types.KZGProof]("proofs", proofs); err != nil {
			return nil, err
		}
		if err := sidecar.VerifyProofs(); err != nil {
			return nil, fmt.Errorf("failed to verify blob proof: %w", err)
		}
	}

	hashes := make([]common.Hash, n)
	for i, c := range sidecar.Commitments {
		hashes[i] = c.ComputeVersionedHash()
		if len(args.BlobVersionedHashes) == n && args.BlobVersionedHashes[i] != hashes[i] {
			return nil, fmt.Errorf("blob hash verification failed (have=%s, want=%s)", args.BlobVersionedHashes[i], hashes[i])
		}
	}
	args.BlobVersionedHashes = hashes
	return sidecar, nil
}

func parse48[T ~[types.LEN_48]byte](field string, in []hexutil.Bytes) ([]T, error) {
	out := make([]T, len(in))
	for i, b := range in {
		if len(b) != types.LEN_48 {
			return nil, fmt.Errorf("%s[%d]: invalid length %d, want %d", field, i, len(b), types.LEN_48)
		}
		copy(out[i][:], b)
	}
	return out, nil
}

func (api *APIImpl) newGasOracle(dbTx kv.TemporalTx) *gasprice.Oracle {
	backend := NewGasPriceOracleBackend(api.db, dbTx, api.BaseAPI)
	return gasprice.NewOracle(backend, ethconfig.Defaults.GPO, api.gasCache, api.feeHistoryCache, api.logger.New("app", "gasPriceOracle"))
}

func (api *APIImpl) fillFeeDefaults(ctx context.Context, args *ethapi.CallArgs, head *types.Header, dbTx kv.TemporalTx) error {
	if head.BaseFee == nil {
		if args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil {
			return errors.New("maxFeePerGas and maxPriorityFeePerGas are not valid before London is active")
		}
		if args.GasPrice == nil {
			price, err := api.newGasOracle(dbTx).SuggestTipCap(ctx)
			if err != nil {
				return err
			}
			args.GasPrice = (*hexutil.U256)(new(uint256.Int).Set(price))
		}
		return nil
	}

	if args.GasPrice == nil && args.MaxFeePerGas != nil && args.MaxPriorityFeePerGas != nil {
		if (*uint256.Int)(args.MaxFeePerGas).IsZero() {
			return errors.New("maxFeePerGas must be non-zero")
		}
		if (*uint256.Int)(args.MaxFeePerGas).Lt((*uint256.Int)(args.MaxPriorityFeePerGas)) {
			return fmt.Errorf("maxFeePerGas (%v) < maxPriorityFeePerGas (%v)", args.MaxFeePerGas, args.MaxPriorityFeePerGas)
		}
		return nil
	}

	if args.GasPrice != nil {
		if (*uint256.Int)(args.GasPrice).IsZero() {
			return errors.New("gasPrice must be non-zero after london fork")
		}
		return nil
	}

	autoFilledPriorityFee := args.MaxPriorityFeePerGas == nil
	if autoFilledPriorityFee {
		tip, err := api.newGasOracle(dbTx).SuggestTipCap(ctx)
		if err != nil {
			return err
		}
		args.MaxPriorityFeePerGas = (*hexutil.U256)(new(uint256.Int).Set(tip))
	}
	if args.MaxFeePerGas == nil {
		doubledBaseFee, overflow := new(uint256.Int).AddOverflow(head.BaseFee, head.BaseFee)
		if overflow {
			return fmt.Errorf("maxFeePerGas overflow: 2*baseFee (%v) exceeds 256 bits", head.BaseFee)
		}
		val, overflow := new(uint256.Int).AddOverflow((*uint256.Int)(args.MaxPriorityFeePerGas), doubledBaseFee)
		if overflow {
			return fmt.Errorf("maxFeePerGas overflow: maxPriorityFeePerGas (%v) + 2*baseFee exceeds 256 bits", args.MaxPriorityFeePerGas)
		}
		args.MaxFeePerGas = (*hexutil.U256)(val)
	}
	if (*uint256.Int)(args.MaxFeePerGas).Lt((*uint256.Int)(args.MaxPriorityFeePerGas)) {
		if autoFilledPriorityFee {
			return fmt.Errorf("suggested maxPriorityFeePerGas (%v) exceeds provided maxFeePerGas (%v)", args.MaxPriorityFeePerGas, args.MaxFeePerGas)
		}
		return fmt.Errorf("maxFeePerGas (%v) < maxPriorityFeePerGas (%v)", args.MaxFeePerGas, args.MaxPriorityFeePerGas)
	}
	return nil
}
