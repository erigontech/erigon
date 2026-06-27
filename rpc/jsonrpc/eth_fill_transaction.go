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
	"math/big"

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
	if args.MaxFeePerBlobGas != nil && args.MaxFeePerBlobGas.ToInt().Sign() == 0 {
		return nil, errors.New("maxFeePerBlobGas, if specified, must be non-zero")
	}

	dbTx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbTx.Rollback()

	cc, err := api.chainConfig(ctx, dbTx)
	if err != nil {
		return nil, err
	}
	head := rawdb.ReadCurrentHeader(dbTx)
	if head == nil {
		return nil, errors.New("missing current header")
	}

	if args.Value == nil {
		args.Value = new(hexutil.Big)
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

	if args.BlobVersionedHashes != nil {
		if n := len(args.BlobVersionedHashes); n == 0 {
			return nil, errors.New("need at least 1 blob for a blob transaction")
		} else if n > params.MaxBlobsPerTxn {
			return nil, fmt.Errorf("too many blobs in transaction (have=%d, max=%d)", n, params.MaxBlobsPerTxn)
		}
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

	if err := api.fillFeeDefaults(ctx, &args, head, dbTx); err != nil {
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
			b := blobFee.ToBig()
			args.MaxFeePerBlobGas = (*hexutil.Big)(b.Lsh(b, 1))
		}
	}

	chainIDBig := cc.ChainID.ToBig()
	if args.ChainID == nil {
		args.ChainID = (*hexutil.Big)(chainIDBig)
	} else if have := args.ChainID.ToInt(); have.Cmp(chainIDBig) != 0 {
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
	if err = txn.MarshalBinary(&buf); err != nil {
		return nil, err
	}

	return &ethapi.SignTransactionResult{
		Raw: buf.Bytes(),
		Tx:  ethapi.NewRPCTransaction(txn, common.Hash{}, 0, 0, 0, nil),
	}, nil
}

func (api *APIImpl) newGasOracle(dbTx kv.TemporalTx) *gasprice.Oracle {
	return gasprice.NewOracle(NewGasPriceOracleBackend(api.db, dbTx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, api.feeHistoryCache, api.logger.New("app", "gasPriceOracle"))
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
			args.GasPrice = (*hexutil.Big)(price.ToBig())
		}
		return nil
	}

	if args.GasPrice == nil && args.MaxFeePerGas != nil && args.MaxPriorityFeePerGas != nil {
		if args.MaxFeePerGas.ToInt().Sign() == 0 {
			return errors.New("maxFeePerGas must be non-zero")
		}
		if args.MaxFeePerGas.ToInt().Cmp(args.MaxPriorityFeePerGas.ToInt()) < 0 {
			return fmt.Errorf("maxFeePerGas (%v) < maxPriorityFeePerGas (%v)", args.MaxFeePerGas, args.MaxPriorityFeePerGas)
		}
		return nil
	}

	if args.GasPrice != nil {
		if args.GasPrice.ToInt().Sign() == 0 {
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
		args.MaxPriorityFeePerGas = (*hexutil.Big)(tip.ToBig())
	}
	if args.MaxFeePerGas == nil {
		val := new(big.Int).Add(
			args.MaxPriorityFeePerGas.ToInt(),
			new(big.Int).Lsh(head.BaseFee.ToBig(), 1),
		)
		args.MaxFeePerGas = (*hexutil.Big)(val)
	}
	if args.MaxFeePerGas.ToInt().Cmp(args.MaxPriorityFeePerGas.ToInt()) < 0 {
		if autoFilledPriorityFee {
			return fmt.Errorf("suggested maxPriorityFeePerGas (%v) exceeds provided maxFeePerGas (%v)", args.MaxPriorityFeePerGas, args.MaxFeePerGas)
		}
		return fmt.Errorf("maxFeePerGas (%v) < maxPriorityFeePerGas (%v)", args.MaxFeePerGas, args.MaxPriorityFeePerGas)
	}
	return nil
}
