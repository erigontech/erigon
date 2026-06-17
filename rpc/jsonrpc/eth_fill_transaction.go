// Copyright 2024 The Erigon Authors
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

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/gasprice"
)

// FillTransaction implements eth_fillTransaction.
func (api *APIImpl) FillTransaction(ctx context.Context, args ethapi.CallArgs) (*ethapi.SignTransactionResult, error) {
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return nil, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
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
			reply, err2 := api.txPool.Nonce(ctx, &txpoolproto.NonceRequest{
				Address: gointerfaces.ConvertAddressToH160(*args.From),
			}, &grpc.EmptyCallOption{})
			if err2 == nil && reply != nil && reply.Found {
				nonce = reply.Nonce + 1
			} else {
				latestBlock := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
				if count, err3 := api.GetTransactionCount(ctx, *args.From, &latestBlock); err3 == nil && count != nil {
					nonce = uint64(*count)
				}
			}
		}
		args.Nonce = (*hexutil.Uint64)(&nonce)
	}

	if args.Data != nil && args.Input != nil && !bytes.Equal(*args.Data, *args.Input) {
		return nil, errors.New(`both "data" and "input" are set and not equal. Please use "input" to pass transaction call data`)
	}

	if args.To == nil {
		hasData := (args.Input != nil && len(*args.Input) > 0) || (args.Data != nil && len(*args.Data) > 0)
		if !hasData {
			return nil, errors.New(`contract creation without any data provided`)
		}
	}

	if err := api.fillFeeDefaults(ctx, &args, head, dbTx); err != nil {
		return nil, err
	}

	if args.BlobVersionedHashes != nil && args.MaxFeePerBlobGas == nil && head.ExcessBlobGas != nil {
		nextBlockTime := head.Time + cc.SecondsPerSlot()
		blobFee, err := misc.GetBlobGasPrice(cc, *head.ExcessBlobGas, nextBlockTime)
		if err != nil {
			return nil, err
		}
		args.MaxFeePerBlobGas = (*hexutil.Big)(new(big.Int).Lsh(blobFee.ToBig(), 1))
	}

	chainIDBig := cc.ChainID.ToBig()
	if args.ChainID == nil {
		args.ChainID = (*hexutil.Big)(chainIDBig)
	} else if have := args.ChainID.ToInt(); have.Cmp(chainIDBig) != 0 {
		return nil, fmt.Errorf("chainId does not match node's (have=%v, want=%v)", have, cc.ChainID)
	}

	if args.Gas == nil {
		estimated, err := api.EstimateGas(ctx, &args, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		args.Gas = &estimated
	}

	baseFee := head.BaseFee

	txn, err := args.ToTransaction(api.GasCap, baseFee)
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

func (api *APIImpl) fillFeeDefaults(ctx context.Context, args *ethapi.CallArgs, head *types.Header, dbTx kv.TemporalTx) error {
	isPostLondon := head.BaseFee != nil
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(api.db, dbTx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, nil, api.logger.New("app", "gasPriceOracle"))

	if !isPostLondon {
		if args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil {
			return errors.New("maxFeePerGas and maxPriorityFeePerGas are not valid before London is active")
		}
		if args.GasPrice == nil {
			price, err := oracle.SuggestTipCap(ctx)
			if err != nil {
				return err
			}
			args.GasPrice = (*hexutil.Big)(price.ToBig())
		}
		return nil
	}

	// Post-London: if both EIP-1559 fields already set, just sanity-check them.
	if args.GasPrice == nil && args.MaxFeePerGas != nil && args.MaxPriorityFeePerGas != nil {
		if args.MaxFeePerGas.ToInt().Sign() == 0 {
			return errors.New("maxFeePerGas must be non-zero")
		}
		if args.MaxFeePerGas.ToInt().Cmp(args.MaxPriorityFeePerGas.ToInt()) < 0 {
			return fmt.Errorf("maxFeePerGas (%v) < maxPriorityFeePerGas (%v)", args.MaxFeePerGas, args.MaxPriorityFeePerGas)
		}
		return nil
	}

	// GasPrice is already set: the caller wants a legacy transaction.
	if args.GasPrice != nil {
		return nil
	}

	tip, err := oracle.SuggestTipCap(ctx)
	if err != nil {
		return err
	}
	if args.MaxPriorityFeePerGas == nil {
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
		return fmt.Errorf("maxFeePerGas (%v) < maxPriorityFeePerGas (%v)", args.MaxFeePerGas, args.MaxPriorityFeePerGas)
	}
	return nil
}
