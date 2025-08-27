// Copyright 2016 The go-ethereum Authors
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

package types

import (
	"errors"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
)

func TestEIP1559Signing(t *testing.T) {
	t.Parallel()
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)

	chainId := uint256.NewInt(18)
	signer := LatestSignerForChainID(chainId.ToBig())
	txn, err := SignTx(NewEIP1559Transaction(*chainId, 0, addr, new(uint256.Int), 0, new(uint256.Int), new(uint256.Int), new(uint256.Int), nil), *signer, key)
	if err != nil {
		t.Fatal(err)
	}

	from, err := txn.Sender(*signer)
	if err != nil {
		t.Fatal(err)
	}
	if from != addr {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, addr)
	}
}

func TestEIP155Signing(t *testing.T) {
	t.Parallel()
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)

	signer := LatestSignerForChainID(big.NewInt(18))
	txn, err := SignTx(NewTransaction(0, addr, new(uint256.Int), 0, new(uint256.Int), nil), *signer, key)
	if err != nil {
		t.Fatal(err)
	}

	from, err := txn.Sender(*signer)
	if err != nil {
		t.Fatal(err)
	}
	if from != addr {
		t.Errorf("exected from and address to be equal. Got %x want %x", from, addr)
	}
}

func TestEIP155ChainId(t *testing.T) {
	t.Parallel()
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)

	signer := LatestSignerForChainID(big.NewInt(18))
	txn, err := SignTx(NewTransaction(0, addr, new(uint256.Int), 0, new(uint256.Int), nil), *signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if !txn.Protected() {
		t.Fatal("expected txn to be protected")
	}

	if !txn.GetChainID().Eq(&signer.chainID) {
		t.Errorf("expected chainId to be %s, got %s", &signer.chainID, txn.GetChainID())
	}

	txn = NewTransaction(0, addr, new(uint256.Int), 0, new(uint256.Int), nil)
	txn, err = SignTx(txn, *LatestSignerForChainID(nil), key)
	if err != nil {
		t.Fatal(err)
	}

	if txn.Protected() {
		t.Error("didn't expect txn to be protected")
	}

	if !txn.GetChainID().IsZero() {
		t.Error("expected chain id to be 0 got", txn.GetChainID())
	}
}

func TestEIP155SigningVitalik(t *testing.T) {
	t.Parallel()
	// Test vectors come from (now broken link) - vitalik.ca/files/eip155_testvec.txt
	for i, test := range []struct {
		txRlp, addr string
	}{
		{"f864808504a817c800825208943535353535353535353535353535353535353535808025a0044852b2a670ade5407e78fb2863c51de9fcb96542a07186fe3aeda6bb8a116da0044852b2a670ade5407e78fb2863c51de9fcb96542a07186fe3aeda6bb8a116d", "0xf0f6f18bca1b28cd68e4357452947e021241e9ce"},
		{"f864018504a817c80182a410943535353535353535353535353535353535353535018025a0489efdaa54c0f20c7adf612882df0950f5a951637e0307cdcb4c672f298b8bcaa0489efdaa54c0f20c7adf612882df0950f5a951637e0307cdcb4c672f298b8bc6", "0x23ef145a395ea3fa3deb533b8a9e1b4c6c25d112"},
		{"f864028504a817c80282f618943535353535353535353535353535353535353535088025a02d7c5bef027816a800da1736444fb58a807ef4c9603b7848673f7e3a68eb14a5a02d7c5bef027816a800da1736444fb58a807ef4c9603b7848673f7e3a68eb14a5", "0x2e485e0c23b4c3c542628a5f672eeab0ad4888be"},
		{"f865038504a817c803830148209435353535353535353535353535353535353535351b8025a02a80e1ef1d7842f27f2e6be0972bb708b9a135c38860dbe73c27c3486c34f4e0a02a80e1ef1d7842f27f2e6be0972bb708b9a135c38860dbe73c27c3486c34f4de", "0x82a88539669a3fd524d669e858935de5e5410cf0"},
		{"f865048504a817c80483019a28943535353535353535353535353535353535353535408025a013600b294191fc92924bb3ce4b969c1e7e2bab8f4c93c3fc6d0a51733df3c063a013600b294191fc92924bb3ce4b969c1e7e2bab8f4c93c3fc6d0a51733df3c060", "0xf9358f2538fd5ccfeb848b64a96b743fcc930554"},
		{"f865058504a817c8058301ec309435353535353535353535353535353535353535357d8025a04eebf77a833b30520287ddd9478ff51abbdffa30aa90a8d655dba0e8a79ce0c1a04eebf77a833b30520287ddd9478ff51abbdffa30aa90a8d655dba0e8a79ce0c1", "0xa8f7aba377317440bc5b26198a363ad22af1f3a4"},
		{"f866068504a817c80683023e3894353535353535353535353535353535353535353581d88025a06455bf8ea6e7463a1046a0b52804526e119b4bf5136279614e0b1e8e296a4e2fa06455bf8ea6e7463a1046a0b52804526e119b4bf5136279614e0b1e8e296a4e2d", "0xf1f571dc362a0e5b2696b8e775f8491d3e50de35"},
		{"f867078504a817c807830290409435353535353535353535353535353535353535358201578025a052f1a9b320cab38e5da8a8f97989383aab0a49165fc91c737310e4f7e9821021a052f1a9b320cab38e5da8a8f97989383aab0a49165fc91c737310e4f7e9821021", "0xd37922162ab7cea97c97a87551ed02c9a38b7332"},
		{"f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10", "0x9bddad43f934d313c2b79ca28a432dd2b7281029"},
		{"f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb", "0x3c24d7329e92f84f08556ceb6df1cdb0104ca49f"},
	} {
		signer := LatestSignerForChainID(big.NewInt(1))

		txn, err := DecodeTransaction(common.Hex2Bytes(test.txRlp))
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		from, err := txn.Sender(*signer)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		addr := common.HexToAddress(test.addr)
		if from != addr {
			t.Errorf("%d: expected %x got %x", i, addr, from)
		}

	}
}

func TestChainId(t *testing.T) {
	t.Parallel()
	key, _ := defaultTestKey()

	var txn Transaction = NewTransaction(0, common.Address{}, new(uint256.Int), 0, new(uint256.Int), nil)

	var err error
	txn, err = SignTx(txn, *LatestSignerForChainID(big.NewInt(1)), key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = txn.Sender(*LatestSignerForChainID(big.NewInt(2)))
	if !errors.Is(err, ErrInvalidChainId) {
		t.Error("expected error:", ErrInvalidChainId)
	}

	_, err = txn.Sender(*LatestSignerForChainID(big.NewInt(1)))
	if err != nil {
		t.Error("expected no error")
	}
}
