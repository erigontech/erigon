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

package types

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const goldenChainID = 1337

func goldenU256(s string) uint256.Int {
	return *new(uint256.Int).SetBytes(common.FromHex(s))
}

func goldenTo() *common.Address {
	to := common.HexToAddress("0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae")
	return &to
}

func goldenCommonTx(to *common.Address, v uint64) CommonTx {
	return CommonTx{
		Nonce:    42,
		GasLimit: 123457,
		To:       to,
		Value:    *uint256.NewInt(1000000000000000000),
		Data:     common.FromHex("0x600160010160005500"),
		V:        *uint256.NewInt(v),
		R:        goldenU256("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"),
		S:        goldenU256("0x202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
	}
}

func goldenAccessListData() AccessList {
	return AccessList{
		{
			Address: common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			StorageKeys: []common.Hash{
				common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
				common.HexToHash("0xfe00000000000000000000000000000000000000000000000000000000000000"),
			},
		},
		{
			Address: common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			StorageKeys: []common.Hash{
				common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ff"),
			},
		},
	}
}

func goldenAuthorizations() []Authorization {
	return []Authorization{
		{
			ChainID: *uint256.NewInt(goldenChainID),
			Address: common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc"),
			Nonce:   7,
			YParity: 1,
			R:       goldenU256("0x1111111111111111111111111111111111111111111111111111111111111111"),
			S:       goldenU256("0x2222222222222222222222222222222222222222222222222222222222222222"),
		},
		{
			ChainID: *uint256.NewInt(1),
			Address: common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
			Nonce:   8,
			YParity: 0,
			R:       *uint256.NewInt(3),
			S:       *uint256.NewInt(4),
		},
	}
}

func goldenLegacyTx(to *common.Address, v uint64) *LegacyTx {
	return &LegacyTx{
		CommonTx: goldenCommonTx(to, v),
		GasPrice: *uint256.NewInt(20000000000),
	}
}

func goldenAccessListTx(to *common.Address) *AccessListTx {
	return &AccessListTx{
		LegacyTx: LegacyTx{
			CommonTx: goldenCommonTx(to, 1),
			GasPrice: *uint256.NewInt(20000000000),
		},
		ChainID:    *uint256.NewInt(goldenChainID),
		AccessList: goldenAccessListData(),
	}
}

func goldenDynFeeTxData(to *common.Address) DynamicFeeTransaction {
	return DynamicFeeTransaction{
		CommonTx:   goldenCommonTx(to, 1),
		ChainID:    *uint256.NewInt(goldenChainID),
		TipCap:     *uint256.NewInt(2000000000),
		FeeCap:     *uint256.NewInt(100000000000),
		AccessList: goldenAccessListData(),
	}
}

func goldenDynFeeTx(to *common.Address) *DynamicFeeTransaction {
	txn := goldenDynFeeTxData(to)
	return &txn
}

func goldenBlobTx() *BlobTx {
	return &BlobTx{
		DynamicFeeTransaction: goldenDynFeeTxData(goldenTo()),
		MaxFeePerBlobGas:      *uint256.NewInt(123456789),
		BlobVersionedHashes: []common.Hash{
			common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000aaa"),
			common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000bbb"),
		},
	}
}

func goldenSetCodeTx(auths []Authorization) *SetCodeTransaction {
	return &SetCodeTransaction{
		DynamicFeeTransaction: goldenDynFeeTxData(goldenTo()),
		Authorizations:        auths,
	}
}

func goldenAATx() *AccountAbstractionTransaction {
	paymaster := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	deployer := common.HexToAddress("0xffffffffffffffffffffffffffffffffffffff01")
	return &AccountAbstractionTransaction{
		Nonce:                       42,
		ChainID:                     uint256.NewInt(goldenChainID),
		Tip:                         uint256.NewInt(2000000000),
		FeeCap:                      uint256.NewInt(100000000000),
		GasLimit:                    123457,
		AccessList:                  goldenAccessListData(),
		SenderAddress:               accounts.InternAddress(common.HexToAddress("0x9999999999999999999999999999999999999999")),
		SenderValidationData:        common.FromHex("0x0badc0de"),
		ExecutionData:               common.FromHex("0x600160010160005500"),
		Paymaster:                   &paymaster,
		PaymasterData:               common.FromHex("0x1badc0de"),
		Deployer:                    &deployer,
		DeployerData:                common.FromHex("0x2badc0de"),
		BuilderFee:                  uint256.NewInt(777),
		ValidationGasLimit:          50000,
		PaymasterValidationGasLimit: 60000,
		PostOpGasLimit:              70000,
		Authorizations:              goldenAuthorizations(),
		NonceKey:                    uint256.NewInt(11),
	}
}

func goldenBlobTxWrapper(version byte) *BlobTxWrapper {
	var blob Blob
	for i := range blob {
		blob[i] = byte(i % 251)
	}
	var commitment KZGCommitment
	for i := range commitment {
		commitment[i] = byte(i + 1)
	}
	var proof0, proof1 KZGProof
	for i := range proof0 {
		proof0[i] = byte(2*i + 1)
		proof1[i] = byte(3*i + 2)
	}
	return &BlobTxWrapper{
		Tx:             goldenBlobTx().copyData(),
		WrapperVersion: version,
		Commitments:    BlobKzgs{commitment},
		Blobs:          Blobs{blob},
		Proofs:         KZGProofs{proof0, proof1},
	}
}

type namedGoldenTx struct {
	name string
	txn  Transaction
}

func goldenCodecTxs() []namedGoldenTx {
	return []namedGoldenTx{
		{"legacy-eip155", goldenLegacyTx(goldenTo(), 2*goldenChainID+36)},
		{"legacy-pre-eip155-create", goldenLegacyTx(nil, 28)},
		{"access-list", goldenAccessListTx(goldenTo())},
		{"access-list-create", goldenAccessListTx(nil)},
		{"dynamic-fee", goldenDynFeeTx(goldenTo())},
		{"dynamic-fee-create", goldenDynFeeTx(nil)},
		{"blob", goldenBlobTx()},
		{"set-code", goldenSetCodeTx(goldenAuthorizations())},
		{"set-code-no-auths", goldenSetCodeTx([]Authorization{})},
		{"account-abstraction", goldenAATx()},
	}
}

type txCodecGolden struct {
	marshalBinary string
	encodeRLP     string
	hash          string
	signingHash   string
	encodingSize  int
}

type blobWrapperGolden struct {
	wrappedSize   int
	wrappedKeccak string
	innerHash     string
}

var txCodecGoldens = map[string]txCodecGolden{
	"legacy-eip155": {
		marshalBinary: "f8782a8504a817c8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500820a96a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "f8782a8504a817c8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500820a96a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "e0c6ce68c7bf152006f9390c766eaacb0aafca988806e3887007c1e9cfaab656",
		signingHash:   "b7bf2cb062f921c4895cf438d9e1ce52c9b91e2b4694e897d489d4d3c772aed7",
		encodingSize:  120,
	},
	"legacy-pre-eip155-create": {
		marshalBinary: "f8622a8504a817c8008301e24180880de0b6b3a7640000896001600101600055001ca00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "f8622a8504a817c8008301e24180880de0b6b3a7640000896001600101600055001ca00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "24836d05c6b9143f7ace9c01150b6ec965b4f04a1870ec8be8edbad96f56b47c",
		signingHash:   "2ca0630fdc395d51e7e8e3295dace494937a2e99efc216088c82218773f3ef94",
		encodingSize:  98,
	},
	"access-list": {
		marshalBinary: "01f9010e8205392a8504a817c8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "b9011201f9010e8205392a8504a817c8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "3e59507ec16ad8a44a2280023c1618b54d821ce0bcef0f6ac714169d29b60f0f",
		signingHash:   "79db98c4ac795460b87ec740bf5589d44ded215a7303c929001565fa0f3b85c7",
		encodingSize:  274,
	},
	"access-list-create": {
		marshalBinary: "01f8fa8205392a8504a817c8008301e24180880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "b8fd01f8fa8205392a8504a817c8008301e24180880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "69b1920fca67943a9da148803e5074feaa9e3038e4f4b900a4af80c4da7463d5",
		signingHash:   "4a5400c797498925a7483e5b1a16e5213f91cfd68082b5babf6207a354d3c22f",
		encodingSize:  253,
	},
	"dynamic-fee": {
		marshalBinary: "02f901138205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "b9011702f901138205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "bbd74dd9e1950e2c6826dcf48915bfb6c750133056e9b94f266497e38c3d9392",
		signingHash:   "ac8477fb5d649719ddf0500f8275c1eda0fdc78abdda24923b3035b395db27d1",
		encodingSize:  279,
	},
	"dynamic-fee-create": {
		marshalBinary: "02f8ff8205392a847735940085174876e8008301e24180880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "b9010202f8ff8205392a847735940085174876e8008301e24180880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "a9b8114832a64cc29ca7f71e627f21128d14fff6789a2adfe0cf8c6f84be78dd",
		signingHash:   "46fb7ec210d43665f813682022e052c8a01093d2355bbaf108fdbfae6724aa9c",
		encodingSize:  258,
	},
	"blob": {
		marshalBinary: "03f9015c8205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff84075bcd15f842a00100000000000000000000000000000000000000000000000000000000000aaaa00100000000000000000000000000000000000000000000000000000000000bbb01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "b9016003f9015c8205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ff84075bcd15f842a00100000000000000000000000000000000000000000000000000000000000aaaa00100000000000000000000000000000000000000000000000000000000000bbb01a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "8adf24f0a9514fabbf9545219753b9079409dd86966b60fe473eb552520e6b23",
		signingHash:   "651ba3c9d41f67a58215d0143fa6d4773680bf69a832549a5e9bca3c76eec325",
		encodingSize:  352,
	},
	"set-code": {
		marshalBinary: "04f9018e8205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000fff879f85c82053994cccccccccccccccccccccccccccccccccccccccc0701a01111111111111111111111111111111111111111111111111111111111111111a02222222222222222222222222222222222222222222222222222222222222222da0194dddddddddddddddddddddddddddddddddddddddd0880030401a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "b9019204f9018e8205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000fff879f85c82053994cccccccccccccccccccccccccccccccccccccccc0701a01111111111111111111111111111111111111111111111111111111111111111a02222222222222222222222222222222222222222222222222222222222222222da0194dddddddddddddddddddddddddddddddddddddddd0880030401a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "bcd118c9d0fa59d15557121ba0a7e0cb5775ce5733c269ea7cc989866820caa7",
		signingHash:   "aca6c69f60ac1c785b18d059f7f40bb2b8ace6fe17d673e3f0b6801719b463a1",
		encodingSize:  402,
	},
	"set-code-no-auths": {
		marshalBinary: "04f901148205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ffc001a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		encodeRLP:     "b9011804f901148205392a847735940085174876e8008301e24194de0b295669a9fd93d5f28d9ec85e40f4cb697bae880de0b6b3a764000089600160010160005500f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000ffc001a00102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20a0202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
		hash:          "dbd52213d5d3ccbae3686bd4782a1052686cd5c145562d24e269186fc29a3180",
		signingHash:   "07d6e6d13c100d0dadc13eadb6f57f63845751d51b8f451867afcf9f4b464d90",
		encodingSize:  280,
	},
	"account-abstraction": {
		marshalBinary: "05f901898205390b2a949999999999999999999999999999999999999999840badc0de94ffffffffffffffffffffffffffffffffffffff01842badc0de94eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee841badc0de89600160010160005500820309847735940085174876e80082c35082ea60830111708301e241f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000fff879f85c82053994cccccccccccccccccccccccccccccccccccccccc0701a01111111111111111111111111111111111111111111111111111111111111111a02222222222222222222222222222222222222222222222222222222222222222da0194dddddddddddddddddddddddddddddddddddddddd08800304",
		encodeRLP:     "b9018d05f901898205390b2a949999999999999999999999999999999999999999840badc0de94ffffffffffffffffffffffffffffffffffffff01842badc0de94eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee841badc0de89600160010160005500820309847735940085174876e80082c35082ea60830111708301e241f893f85994aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf842a00000000000000000000000000000000000000000000000000000000000000001a0fe00000000000000000000000000000000000000000000000000000000000000f794bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbe1a000000000000000000000000000000000000000000000000000000000000000fff879f85c82053994cccccccccccccccccccccccccccccccccccccccc0701a01111111111111111111111111111111111111111111111111111111111111111a02222222222222222222222222222222222222222222222222222222222222222da0194dddddddddddddddddddddddddddddddddddddddd08800304",
		hash:          "b595c7deb680a67a1c8e3b9c75cde26b84f2e1f89bb4113b5ec2fd383d61c13b",
		signingHash:   "2dd6cd2886279f18b5df0b3936fbe92f4e6936e0130bcd61684539c3036512bc",
		encodingSize:  397,
	},
}

var blobWrapperGoldens = map[byte]blobWrapperGolden{
	0: {
		wrappedSize:   131586,
		wrappedKeccak: "d90b9d4ba9a17f897aea81781180947857705cda159049528733b76b7b831d1a",
		innerHash:     "8adf24f0a9514fabbf9545219753b9079409dd86966b60fe473eb552520e6b23",
	},
	1: {
		wrappedSize:   131587,
		wrappedKeccak: "f2a9826163ad2bc26c2861cf68493a4d99452ebc664320de2f0bb0bb7899f0c6",
		innerHash:     "8adf24f0a9514fabbf9545219753b9079409dd86966b60fe473eb552520e6b23",
	},
}

func TestTxCodecGoldens(t *testing.T) {
	chainID := uint256.NewInt(goldenChainID)
	for _, tc := range goldenCodecTxs() {
		t.Run(tc.name, func(t *testing.T) {
			want, ok := txCodecGoldens[tc.name]
			if !ok {
				t.Fatalf("no golden pinned for %q", tc.name)
			}

			var mb bytes.Buffer
			if err := tc.txn.MarshalBinary(&mb); err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			if got := hex.EncodeToString(mb.Bytes()); got != want.marshalBinary {
				t.Errorf("MarshalBinary drift:\n got %s\nwant %s", got, want.marshalBinary)
			}

			var eb bytes.Buffer
			if err := tc.txn.EncodeRLP(&eb); err != nil {
				t.Fatalf("EncodeRLP: %v", err)
			}
			if got := hex.EncodeToString(eb.Bytes()); got != want.encodeRLP {
				t.Errorf("EncodeRLP drift:\n got %s\nwant %s", got, want.encodeRLP)
			}

			hash := tc.txn.Hash()
			if got := hex.EncodeToString(hash[:]); got != want.hash {
				t.Errorf("Hash drift: got %s want %s", got, want.hash)
			}
			signingHash := tc.txn.SigningHash(chainID)
			if got := hex.EncodeToString(signingHash[:]); got != want.signingHash {
				t.Errorf("SigningHash drift: got %s want %s", got, want.signingHash)
			}
			if got := tc.txn.EncodingSize(); got != want.encodingSize {
				t.Errorf("EncodingSize drift: got %d want %d", got, want.encodingSize)
			}

			decodedBin, err := UnmarshalTransactionFromBinary(mb.Bytes(), false)
			if err != nil {
				t.Fatalf("UnmarshalTransactionFromBinary: %v", err)
			}
			var mb2 bytes.Buffer
			if err := decodedBin.MarshalBinary(&mb2); err != nil {
				t.Fatalf("re-MarshalBinary: %v", err)
			}
			if !bytes.Equal(mb.Bytes(), mb2.Bytes()) {
				t.Errorf("MarshalBinary round-trip not byte-identical:\n got %x\nwant %x", mb2.Bytes(), mb.Bytes())
			}
			if got := decodedBin.Hash(); got != hash {
				t.Errorf("decoded Hash mismatch: got %x want %x", got, hash)
			}

			s := rlp.NewStream(bytes.NewReader(eb.Bytes()), 0)
			decodedRLP, err := DecodeRLPTransaction(s, false)
			if err != nil {
				t.Fatalf("DecodeRLPTransaction: %v", err)
			}
			var eb2 bytes.Buffer
			if err := decodedRLP.EncodeRLP(&eb2); err != nil {
				t.Fatalf("re-EncodeRLP: %v", err)
			}
			if !bytes.Equal(eb.Bytes(), eb2.Bytes()) {
				t.Errorf("EncodeRLP round-trip not byte-identical:\n got %x\nwant %x", eb2.Bytes(), eb.Bytes())
			}
			if got := decodedRLP.Hash(); got != hash {
				t.Errorf("RLP-decoded Hash mismatch: got %x want %x", got, hash)
			}
		})
	}
}

func TestBlobTxWrapperCodecGoldens(t *testing.T) {
	for _, version := range []byte{0, 1} {
		t.Run(hex.EncodeToString([]byte{version}), func(t *testing.T) {
			want, ok := blobWrapperGoldens[version]
			if !ok {
				t.Fatalf("no golden pinned for wrapper version %d", version)
			}
			txw := goldenBlobTxWrapper(version)

			var wrapped bytes.Buffer
			if err := txw.MarshalBinaryWrapped(&wrapped); err != nil {
				t.Fatalf("MarshalBinaryWrapped: %v", err)
			}
			if got := wrapped.Len(); got != want.wrappedSize {
				t.Errorf("wrapped size drift: got %d want %d", got, want.wrappedSize)
			}
			keccak := crypto.HashData(wrapped.Bytes())
			if got := hex.EncodeToString(keccak[:]); got != want.wrappedKeccak {
				t.Errorf("wrapped bytes drift (keccak): got %s want %s", got, want.wrappedKeccak)
			}
			innerHash := txw.Hash()
			if got := hex.EncodeToString(innerHash[:]); got != want.innerHash {
				t.Errorf("inner tx hash drift: got %s want %s", got, want.innerHash)
			}

			var mb bytes.Buffer
			if err := txw.MarshalBinary(&mb); err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			if got := hex.EncodeToString(mb.Bytes()); got != txCodecGoldens["blob"].marshalBinary {
				t.Errorf("wrapper MarshalBinary must match unwrapped blob golden:\n got %s\nwant %s", got, txCodecGoldens["blob"].marshalBinary)
			}
			var eb bytes.Buffer
			if err := txw.EncodeRLP(&eb); err != nil {
				t.Fatalf("EncodeRLP: %v", err)
			}
			if got := hex.EncodeToString(eb.Bytes()); got != txCodecGoldens["blob"].encodeRLP {
				t.Errorf("wrapper EncodeRLP must match unwrapped blob golden:\n got %s\nwant %s", got, txCodecGoldens["blob"].encodeRLP)
			}

			decoded, err := DecodeWrappedTransaction(wrapped.Bytes())
			if err != nil {
				t.Fatalf("DecodeWrappedTransaction: %v", err)
			}
			decodedWrapper, ok := decoded.(*BlobTxWrapper)
			if !ok {
				t.Fatalf("expected *BlobTxWrapper, got %T", decoded)
			}
			if decodedWrapper.WrapperVersion != version {
				t.Errorf("WrapperVersion mismatch: got %d want %d", decodedWrapper.WrapperVersion, version)
			}
			var wrapped2 bytes.Buffer
			if err := decodedWrapper.MarshalBinaryWrapped(&wrapped2); err != nil {
				t.Fatalf("re-MarshalBinaryWrapped: %v", err)
			}
			if !bytes.Equal(wrapped.Bytes(), wrapped2.Bytes()) {
				t.Errorf("wrapped round-trip not byte-identical (%d vs %d bytes)", wrapped2.Len(), wrapped.Len())
			}
			if got := decodedWrapper.Hash(); got != innerHash {
				t.Errorf("decoded inner hash mismatch: got %x want %x", got, innerHash)
			}
		})
	}
}
