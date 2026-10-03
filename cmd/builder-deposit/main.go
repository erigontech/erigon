// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"slices"
	"strconv"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
)

type builderDepositJSON struct {
	BuilderPubkey         common.Bytes48 `json:"builder_pubkey"`
	WithdrawalCredentials common.Hash    `json:"withdrawal_credentials"`
	ExecutionAddress      common.Address `json:"execution_address"`
	AmountGwei            uint64         `json:"amount_gwei"`
	RequestFeeWei         uint64         `json:"request_fee_wei"`
	TransactionValueWei   string         `json:"transaction_value_wei"`
	Calldata              hexutil.Bytes  `json:"calldata"`
	GenesisForkVersion    common.Bytes4  `json:"genesis_fork_version"`
	DomainBuilderDeposit  common.Bytes4  `json:"domain_builder_deposit"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("builder-deposit", flag.ContinueOnError)
	keyPath := flags.String("key", "", "existing raw 32-byte BLS private key file")
	executionAddressText := flags.String("execution-address", "", "builder withdrawal execution address")
	genesisForkVersionText := flags.String("genesis-fork-version", "", "four-byte genesis fork version")
	amountGweiText := flags.String("amount-gwei", "0", "builder top-up amount in Gwei")
	requestFeeWeiText := flags.String("request-fee-wei", "1", "builder deposit request fee in Wei")
	outPath := flags.String("out", "", "output JSON file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected positional argument: %s", flags.Arg(0))
	}
	if *keyPath == "" || *executionAddressText == "" || *genesisForkVersionText == "" || *outPath == "" {
		return errors.New("-key, -execution-address, -genesis-fork-version, and -out are required")
	}
	amountGwei, err := strconv.ParseUint(*amountGweiText, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid amount-gwei: %w", err)
	}
	requestFeeWei, err := strconv.ParseUint(*requestFeeWeiText, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid request-fee-wei: %w", err)
	}
	if requestFeeWei == 0 {
		return errors.New("request-fee-wei must be at least 1")
	}
	if !common.IsHexAddress(*executionAddressText) {
		return errors.New("invalid execution address")
	}
	executionAddress := common.HexToAddress(*executionAddressText)
	var genesisForkVersion common.Bytes4
	if err := genesisForkVersion.UnmarshalText([]byte(*genesisForkVersionText)); err != nil {
		return fmt.Errorf("invalid genesis fork version: %w", err)
	}
	result, err := buildBuilderDeposit(
		*keyPath,
		executionAddress,
		genesisForkVersion,
		amountGwei,
		requestFeeWei,
	)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode deposit JSON: %w", err)
	}
	encoded = append(encoded, '\n')
	file, err := os.OpenFile(*outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		file.Close()
		_ = dir.RemoveFile(*outPath)
		return fmt.Errorf("write output file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = dir.RemoveFile(*outPath)
		return fmt.Errorf("close output file: %w", err)
	}
	return nil
}

func buildBuilderDeposit(
	keyPath string,
	executionAddress common.Address,
	genesisForkVersion common.Bytes4,
	amountGwei uint64,
	requestFeeWei uint64,
) (builderDepositJSON, error) {
	if executionAddress == (common.Address{}) {
		return builderDepositJSON{}, errors.New("execution address must not be zero")
	}
	minimumAmount := clparams.MainnetBeaconConfig.MinDepositAmount
	if amountGwei < minimumAmount {
		return builderDepositJSON{}, fmt.Errorf("amount-gwei must be at least %d", minimumAmount)
	}
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return builderDepositJSON{}, fmt.Errorf("read builder key: %w", err)
	}
	privateKey, err := bls.NewPrivateKeyFromBytes(keyBytes)
	if err != nil {
		return builderDepositJSON{}, fmt.Errorf("parse builder key: %w", err)
	}
	pubkey := common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey()))
	var withdrawalCredentials common.Hash
	withdrawalCredentials[0] = byte(clparams.MainnetBeaconConfig.BuilderWithdrawalPrefix)
	copy(withdrawalCredentials[12:], executionAddress[:])
	depositData := &cltypes.DepositData{
		PubKey: pubkey, WithdrawalCredentials: withdrawalCredentials, Amount: amountGwei,
	}
	messageRoot, err := depositData.MessageHash()
	if err != nil {
		return builderDepositJSON{}, fmt.Errorf("hash builder deposit: %w", err)
	}
	domainType := clparams.MainnetBeaconConfig.DomainBuilderDeposit
	domain, err := fork.ComputeDomain(domainType[:], genesisForkVersion, common.Hash{})
	if err != nil {
		return builderDepositJSON{}, fmt.Errorf("compute builder deposit domain: %w", err)
	}
	signingRoot := crypto.Sha256(messageRoot[:], domain)
	signature := common.Bytes96(privateKey.Sign(signingRoot[:]).Bytes())
	calldata := slices.Concat(
		pubkey[:],
		withdrawalCredentials[:],
		binary.BigEndian.AppendUint64(nil, amountGwei),
		signature[:],
	)
	transactionValue := new(big.Int).Mul(new(big.Int).SetUint64(amountGwei), big.NewInt(common.GWei))
	transactionValue.Add(transactionValue, new(big.Int).SetUint64(requestFeeWei))
	return builderDepositJSON{
		BuilderPubkey: pubkey, WithdrawalCredentials: withdrawalCredentials, ExecutionAddress: executionAddress,
		AmountGwei: amountGwei, RequestFeeWei: requestFeeWei, TransactionValueWei: transactionValue.String(),
		Calldata: calldata, GenesisForkVersion: genesisForkVersion, DomainBuilderDeposit: domainType,
	}, nil
}
