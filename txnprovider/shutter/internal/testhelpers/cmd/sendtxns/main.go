// Copyright 2025 The Erigon Authors
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

package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/tyler-smith/go-bip32"
	"github.com/tyler-smith/go-bip39"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
)

func main() {
	if len(os.Args) < 8 {
		fmt.Printf("Usage: %s <seedFile> <fromAddress> <toAddress> <amountEth> <rpcURL> <numTxn> <chain>\n", os.Args[0])
		os.Exit(1)
	}
	seedFile := os.Args[1]
	from := os.Args[2]
	to := os.Args[3]
	amount := os.Args[4]
	rpcUrl := os.Args[5]
	numTxnStr := os.Args[6]
	chainStr := os.Args[7]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StderrHandler))
	err := sendTxns(ctx, logger, seedFile, from, to, amount, rpcUrl, numTxnStr, chainStr)
	if err != nil {
		logger.Error("failed to send transactions", "err", err)
		os.Exit(1)
	}
}

func sendTxns(ctx context.Context, logger log.Logger, seedFile, fromStr, toStr, amountStr, url, countStr, chain string) error {
	chainId := params.ChainConfigByChainName(chain).ChainID
	rpcClient := requests.NewRequestGenerator(url, logger)
	transactor := testhelpers.NewTransactor(rpcClient, chainId)
	amount, _ := new(big.Int).SetString(amountStr, 10)
	to := libcommon.HexToAddress(toStr)
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return err
	}
	mnemonicBytes, err := os.ReadFile(seedFile)
	if err != nil {
		if os.IsNotExist(err) {
			entropy, err := bip39.NewEntropy(128)
			if err != nil {
				return err
			}
			mnemonic, err := bip39.NewMnemonic(entropy)
			if err != nil {
				return err
			}
			mnemonicBytes = []byte(mnemonic)
			err = os.WriteFile(seedFile, []byte(mnemonic), 0644)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	mnemonic := string(mnemonicBytes)
	seed := bip39.NewSeed(mnemonic, "")
	masterKey, err := bip32.NewMasterKey(seed)
	if err != nil {
		return err
	}
	purpose := 44 + bip32.FirstHardenedChild
	coinType := 60 + bip32.FirstHardenedChild
	account := 0 + bip32.FirstHardenedChild
	change := uint32(0)
	addressIndex := uint32(0)
	// Derive the purpose key (m/44H)
	purposeKey, err := masterKey.NewChildKey(purpose)
	if err != nil {
		return err
	}
	// Derive the coin type key (m/44'/60H)
	coinTypeKey, err := purposeKey.NewChildKey(coinType)
	if err != nil {
		return err
	}
	// Derive the account key (m/44'/60'/0H)
	accountKey, err := coinTypeKey.NewChildKey(account)
	if err != nil {
		return err
	}
	// Derive the change key (m/44'/60'/0'/0)
	changeKey, err := accountKey.NewChildKey(change)
	if err != nil {
		return err
	}
	// Finally, derive the address index key (m/44'/60'/0'/0/0)
	addressKey, err := changeKey.NewChildKey(addressIndex)
	if err != nil {
		return err
	}
	from, err := crypto.ToECDSA(addressKey.Key)
	if err != nil {
		return err
	}
	if fromStr != crypto.PubkeyToAddress(from.PublicKey).String() {
		panic(fmt.Sprintf("from address mismatch: %s != %s", fromStr, crypto.PubkeyToAddress(from.PublicKey).String()))
	}
	for i := 0; i < count; i++ {
		txn, err := transactor.SubmitSimpleTransfer(from, to, amount)
		if err != nil {
			return err
		}
		logger.Info("transaction sent", "hash", txn.Hash())
		_, err = waitInclusion(ctx, txn, rpcClient)
		if err != nil {
			return err
		}
	}
	logger.Info("all transactions broadcast successfully")
	return nil
}

func waitInclusion(ctx context.Context, txn types.Transaction, rpcClient requests.RequestGenerator) (*types.Receipt, error) {
	queryTicker := time.NewTicker(time.Second)
	defer queryTicker.Stop()

	logger := log.New("hash", txn.Hash())
	for {
		receipt, err := rpcClient.GetTransactionReceipt(ctx, txn.Hash())
		if receipt != nil && receipt.Status == types.ReceiptStatusSuccessful {
			logger.Warn("transaction included in a block", "blockNumber", receipt.BlockNumber)
			return receipt, nil
		}
		if err != nil {
			logger.Warn("receipt retrieval failed", "err", err)
		} else {
			logger.Warn("transaction not yet included in a block")
		}
		// Wait for the next round.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-queryTicker.C:
		}
	}
}
