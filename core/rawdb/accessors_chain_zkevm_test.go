package rawdb

import (
	"testing"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/common/dbutils"
)

func TestBodyStorageZkevm(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	require := require.New(t)

	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	// prepare db so it works with our test
	signer1 := types.MakeSigner(params.HermezMainnetChainConfig, 1)
	body := &types.Body{
		Transactions: []types.Transaction{
			mustSign(types.NewTransaction(1, testAddr, u256.Num1, 1, u256.Num1, nil), *signer1),
			mustSign(types.NewTransaction(2, testAddr, u256.Num1, 2, u256.Num1, nil), *signer1),
		},
		Uncles: []*types.Header{{Extra: []byte("test header")}},
	}

	// Create a test body to move around the database and make sure it's really new
	hasher := sha3.NewLegacyKeccak256()
	_ = rlp.Encode(hasher, body)
	hash := libcommon.BytesToHash(hasher.Sum(nil))

	if entry := ReadCanonicalBodyWithTransactions(tx, hash, 0); entry != nil {
		t.Fatalf("Non existent body returned: %v", entry)
	}
	require.NoError(WriteBody(tx, hash, 0, body))
	if entry := ReadCanonicalBodyWithTransactions(tx, hash, 0); entry == nil {
		t.Fatalf("Stored body not found")
	} else if types.DeriveSha(types.Transactions(entry.Transactions)) != types.DeriveSha(types.Transactions(body.Transactions)) || types.CalcUncleHash(entry.Uncles) != types.CalcUncleHash(body.Uncles) {
		t.Fatalf("Retrieved body mismatch: have %v, want %v", entry, body)
	}
	if entry := ReadBodyRLP(tx, hash, 0); entry == nil {
		t.Fatalf("Stored body RLP not found")
	} else {
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(entry)

		if calc := libcommon.BytesToHash(hasher.Sum(nil)); calc != hash {
			t.Fatalf("Retrieved RLP body mismatch: have %v, want %v", entry, body)
		}
	}

	// zkevm check with overwriting transactions
	bodyForStorage, err := ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(0, hash))
	if err != nil {
		t.Fatalf("ReadBodyForStorageByKey failed: %s", err)
	}
	// overwrite the transactions using the new code from zkevm
	require.NoError(WriteBodyAndTransactions(tx, hash, 0, body.Transactions, bodyForStorage))

	// now re-run the checks from above after reading the body again
	if entry := ReadCanonicalBodyWithTransactions(tx, hash, 0); entry == nil {
		t.Fatalf("Stored body not found")
	} else if types.DeriveSha(types.Transactions(entry.Transactions)) != types.DeriveSha(types.Transactions(body.Transactions)) || types.CalcUncleHash(entry.Uncles) != types.CalcUncleHash(body.Uncles) {
		t.Fatalf("Retrieved body mismatch: have %v, want %v", entry, body)
	}
	if entry := ReadBodyRLP(tx, hash, 0); entry == nil {
		t.Fatalf("Stored body RLP not found")
	} else {
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(entry)

		if calc := libcommon.BytesToHash(hasher.Sum(nil)); calc != hash {
			t.Fatalf("Retrieved RLP body mismatch: have %v, want %v", entry, body)
		}
	}

	// Delete the body and verify the execution
	deleteBody(tx, hash, 0)
	if entry := ReadCanonicalBodyWithTransactions(tx, hash, 0); entry != nil {
		t.Fatalf("Deleted body returned: %v", entry)
	}
}
