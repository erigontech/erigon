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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

func TestBuilderDepositProducesValidTopUp(t *testing.T) {
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	executionAddress := common.HexToAddress("0x1122334455667788990011223344556677889900")
	genesisForkVersion := common.Bytes4{1, 2, 3, 4}

	got, err := buildBuilderDeposit(keyPath, executionAddress, genesisForkVersion, 1_000_000_123, 7)
	require.NoError(t, err)
	require.Equal(t, bls.CompressPublicKey(privateKey.PublicKey()), got.BuilderPubkey[:])
	require.Len(t, got.Calldata, solid.SizeBuilderDepositRequest)
	signature := common.Bytes96(got.Calldata[88:])
	request := &solid.BuilderDepositRequest{
		PubKey: got.BuilderPubkey, WithdrawalCredentials: got.WithdrawalCredentials,
		Amount: got.AmountGwei, Signature: signature,
	}
	cfg := clparams.MainnetBeaconConfig
	cfg.GenesisForkVersion = clparams.ConfigForkVersion(binary.BigEndian.Uint32(genesisForkVersion[:]))
	valid, err := state.IsValidBuilderDepositSignature(&cfg, request)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t, byte(0xb0), got.WithdrawalCredentials[0])
	require.Equal(t, make([]byte, 11), got.WithdrawalCredentials[1:12])
	require.Equal(t, executionAddress[:], got.WithdrawalCredentials[12:])
	require.Equal(t, request.PubKey[:], []byte(got.Calldata[:48]))
	require.Equal(t, request.WithdrawalCredentials[:], []byte(got.Calldata[48:80]))
	require.Equal(t, request.Amount, binary.BigEndian.Uint64(got.Calldata[80:88]))
	require.Equal(t, request.Signature[:], []byte(got.Calldata[88:]))
	require.Equal(t, "1000000123000000007", got.TransactionValueWei)
	require.Equal(t, cfg.DomainBuilderDeposit, got.DomainBuilderDeposit)
}

func TestBuilderDepositRefusesOverwriteAndPreservesKey(t *testing.T) {
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	keyBytes := privateKey.Bytes()
	require.NoError(t, os.WriteFile(keyPath, keyBytes, 0o600))
	outPath := filepath.Join(t.TempDir(), "deposit.json")
	args := []string{
		"-key", keyPath,
		"-execution-address", "0x1122334455667788990011223344556677889900",
		"-genesis-fork-version", "0x01020304",
		"-amount-gwei", "1000000000",
		"-out", outPath,
	}
	require.NoError(t, run(args))
	encoded, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var got builderDepositJSON
	require.NoError(t, json.Unmarshal(encoded, &got))
	require.Equal(t, uint64(1_000_000_000), got.AmountGwei)
	require.Equal(t, uint64(1), got.RequestFeeWei)
	require.ErrorContains(t, run(args), "file exists")
	after, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	require.Equal(t, keyBytes, after)
}

func TestBuilderDepositRejectsAmountBelowMinimum(t *testing.T) {
	_, err := buildBuilderDeposit("unused", common.Address{1}, common.Bytes4{}, 999_999_999, 1)
	require.ErrorContains(t, err, "at least 1000000000")
}

func TestBuilderDepositParsesAmountsAsDecimal(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "deposit.json")
	require.NoError(t, run(testBuilderDepositArgs(t, outPath, "032000000000", "010")))

	encoded, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var got builderDepositJSON
	require.NoError(t, json.Unmarshal(encoded, &got))
	require.Equal(t, uint64(32_000_000_000), got.AmountGwei)
	require.Equal(t, uint64(10), got.RequestFeeWei)
}

func TestBuilderDepositRejectsZeroRequestFee(t *testing.T) {
	err := run(testBuilderDepositArgs(t, filepath.Join(t.TempDir(), "deposit.json"), "1000000000", "0"))
	require.ErrorContains(t, err, "request-fee-wei must be at least 1")
}

func TestBuilderDepositRejectsZeroExecutionAddress(t *testing.T) {
	args := testBuilderDepositArgsWithAddress(t, filepath.Join(t.TempDir(), "deposit.json"), "1000000000", "1", common.Address{})
	err := run(args)
	require.ErrorContains(t, err, "execution address must not be zero")
}

func TestBuilderDepositRejectsPositionalArguments(t *testing.T) {
	args := append(testBuilderDepositArgs(t, filepath.Join(t.TempDir(), "deposit.json"), "1000000000", "1"), "unexpected")
	err := run(args)
	require.ErrorContains(t, err, "unexpected positional argument")
}

func testBuilderDepositArgs(t *testing.T, outPath, amountGwei, requestFeeWei string) []string {
	return testBuilderDepositArgsWithAddress(t, outPath, amountGwei, requestFeeWei, common.HexToAddress("0x1122334455667788990011223344556677889900"))
}

func testBuilderDepositArgsWithAddress(t *testing.T, outPath, amountGwei, requestFeeWei string, executionAddress common.Address) []string {
	t.Helper()
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	return []string{
		"-key", keyPath,
		"-execution-address", executionAddress.Hex(),
		"-genesis-fork-version", "0x01020304",
		"-amount-gwei", amountGwei,
		"-request-fee-wei", requestFeeWei,
		"-out", outPath,
	}
}
