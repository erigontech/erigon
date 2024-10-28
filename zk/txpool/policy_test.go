package txpool

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	mdbx2 "github.com/erigontech/mdbx-go/mdbx"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

// newTestState creates new instance of state used by tests.
func newTestACLDB(tb testing.TB, dir string) kv.RwDB {
	tb.Helper()

	if dir == "" {
		dir = tb.TempDir()
	}

	state, err := OpenACLDB(context.Background(), dir)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatal(err)
		}
	})

	return state
}

func newTestTxPoolDB(tb testing.TB, dir string) kv.RwDB {
	tb.Helper()

	if dir == "" {
		dir = fmt.Sprintf("/tmp/txpool-db-temp_%v", time.Now().UTC().Format(time.RFC3339Nano))
	}

	err := os.Mkdir(dir, 0775)
	if err != nil {
		tb.Fatal(err)
	}

	txPoolDB, err := mdbx.NewMDBX(log.New()).Label(kv.TxPoolDB).Path(dir).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).
		Flags(func(f uint) uint { return f ^ mdbx2.Durable | mdbx2.SafeNoSync }).
		GrowthStep(16 * datasize.MB).
		SyncPeriod(30 * time.Second).
		Open(context.Background())
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatal(err)
		}
	})

	return txPoolDB
}

func policyTransactionSliceEqual(a, b []PolicyTransaction) bool {
	// Check if lengths are different
	if len(a) != len(b) {
		return false
	}

	// Check each element, excluding timeTx
	for i := range a {
		if a[i].aclType != b[i].aclType {
			return false
		}
		if ACLTypeBinary(a[i].operation) != ACLTypeBinary(b[i].operation) {
			return false
		}
		if a[i].addr != b[i].addr {
			return false
		}
		if a[i].policy != b[i].policy {
			return false
		}
	}

	return true
}

func TestCheckDBsCreation(t *testing.T) {
	t.Parallel()

	path := fmt.Sprintf("/tmp/db-test-%v", time.Now().UTC().Format(time.RFC3339Nano))

	txPoolDB := newTestTxPoolDB(t, path)
	aclsDB := newTestACLDB(t, path)

	// Check if the dbs are created
	require.NotNil(t, txPoolDB)
	require.NotNil(t, aclsDB)
}

func TestSetMode(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	t.Run("SetMode - Valid Mode", func(t *testing.T) {
		t.Parallel()

		mode := AllowlistMode

		err := SetMode(ctx, db, mode)
		require.NoError(t, err)

		// Check if the mode is set correctly
		modeInDB, err := GetMode(ctx, db)
		require.NoError(t, err)
		require.Equal(t, string(mode), string(modeInDB))
	})

	t.Run("SetMode - Invalid Mode", func(t *testing.T) {
		t.Parallel()

		mode := "invalid_mode"

		err := SetMode(ctx, db, mode)
		require.ErrorIs(t, err, errInvalidMode)
	})
}

func TestRemovePolicy(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	SetMode(ctx, db, BlocklistMode)

	t.Run("RemovePolicy - Policy Exists", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add the policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "blocklist", addr, policy))

		// Remove the policy from the ACL
		err := RemovePolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy is removed from the ACL
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("RemovePolicy - Policy Does Not Exist", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add some different policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "blocklist", addr, Deploy))

		// Remove the policy from the ACL
		err := RemovePolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy is still not present in the ACL
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("RemovePolicy - Address Not Found", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Remove the policy from the ACL
		err := RemovePolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy is still not present in the ACL
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("RemovePolicy - Unsupported acl type", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		err := RemovePolicy(ctx, db, "unknown_acl_type", addr, policy)
		require.ErrorIs(t, err, errUnsupportedACLType)
	})
}

func TestAddPolicy(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	SetMode(ctx, db, BlocklistMode)

	t.Run("AddPolicy - Policy Does Not Exist", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		err := AddPolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy exists in the ACL
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("AddPolicy - Policy Already Exists", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add the policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "blocklist", addr, policy))

		// Add the policy again
		err := AddPolicy(ctx, db, "blocklist", addr, policy)
		require.NoError(t, err)

		// Check if the policy still exists in the ACL
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr, policy)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("AddPolicy - Unsupported Policy", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := Policy(33) // Assume Policy(33) is not supported

		err := AddPolicy(ctx, db, "blocklist", addr, policy)
		require.ErrorIs(t, err, errUnknownPolicy)
	})

	t.Run("AddPolicy - Unsupported acl type", func(t *testing.T) {
		t.Parallel()

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		err := AddPolicy(ctx, db, "unknown_acl_type", addr, policy)
		require.ErrorIs(t, err, errUnsupportedACLType)
	})
}

func TestUpdatePolicies(t *testing.T) {
	t.Parallel()

	db := newTestACLDB(t, "")
	ctx := context.Background()

	SetMode(ctx, db, BlocklistMode)

	t.Run("UpdatePolicies - Add Policies", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcdef")
		addr2 := common.HexToAddress("0xabcdef1234567890")
		policies := [][]Policy{
			{SendTx, Deploy},
			{SendTx},
		}

		err := UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policies)
		require.NoError(t, err)

		// Check if the policies are added correctly
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("UpdatePolicies - Remove Policies", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcdea")
		addr2 := common.HexToAddress("0xabcdef1234567891")
		policiesOld := [][]Policy{
			{SendTx, Deploy},
			{SendTx},
		}

		err := UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policiesOld)
		require.NoError(t, err)

		// Check if the policies are added correctly
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		policies := [][]Policy{
			{},
			{SendTx},
		}

		err = UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policies)
		require.NoError(t, err)

		// Check if the policies are removed correctly
		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)
	})

	t.Run("UpdatePolicies - Empty Policies", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcded")
		addr2 := common.HexToAddress("0xabcdef1234567893")

		// first add these policies
		policiesOld := [][]Policy{
			{SendTx, Deploy},
			{SendTx},
		}

		err := UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policiesOld)
		require.NoError(t, err)

		// Check if the policies are added correctly
		hasPolicy, err := DoesAccountHavePolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.True(t, hasPolicy)

		// then remove policies
		policies := [][]Policy{
			{},
			{},
		}

		err = UpdatePolicies(ctx, db, "blocklist", []common.Address{addr1, addr2}, policies)
		require.NoError(t, err)

		// Check if the policies are removed correctly
		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr1, SendTx)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr1, Deploy)
		require.NoError(t, err)
		require.False(t, hasPolicy)

		hasPolicy, err = DoesAccountHavePolicy(ctx, db, addr2, SendTx)
		require.NoError(t, err)
		require.False(t, hasPolicy)
	})

	t.Run("UpdatePolicies - Unsupported acl type", func(t *testing.T) {
		t.Parallel()

		// Create test addresses and policies
		addr1 := common.HexToAddress("0x1234567890abcdef")
		addr2 := common.HexToAddress("0xabcdef1234567890")
		policies := [][]Policy{
			{SendTx, Deploy},
			{SendTx},
		}

		err := UpdatePolicies(ctx, db, "unknown_acl_type", []common.Address{addr1, addr2}, policies)
		require.ErrorIs(t, err, errUnsupportedACLType)
	})
}

func TestLastPolicyTransactions(t *testing.T) {
	db := newTestACLDB(t, "")
	ctx := context.Background()

	SetMode(ctx, db, BlocklistMode)

	// Create a test address and policy
	addrOne := common.HexToAddress("0x1234567890abcdef")
	policyOne := SendTx

	addrTwo := common.HexToAddress("0xabcdef1234567890")
	policyTwo := SendTx

	// Add the policy to the ACL
	require.NoError(t, AddPolicy(ctx, db, "blocklist", addrOne, policyOne))
	require.NoError(t, AddPolicy(ctx, db, "blocklist", addrTwo, policyTwo))

	// Create expected policyTransaction output and append to []PolicyTransaction
	policyTransactionOne := PolicyTransaction{addr: common.HexToAddress("0x1234567890abcdef"), aclType: ResolveACLTypeToBinary("blocklist"), policy: Policy(SendTx.ToByte()), operation: Operation(Add.ToByte())}
	policyTransactionTwo := PolicyTransaction{addr: common.HexToAddress("0xabcdef1234567890"), aclType: ResolveACLTypeToBinary("blocklist"), policy: Policy(SendTx.ToByte()), operation: Operation(Add.ToByte())}

	// LastPolicyTransactions seems to append in reverse order than this test function. So the order of elements is also reversed
	// Single element in PolicyTransaction slice
	var policyTransactionSliceSingle []PolicyTransaction
	policyTransactionSliceSingle = append(policyTransactionSliceSingle, policyTransactionTwo)

	// Two elements in PolicyTransaction slice
	var policyTransactionSliceDouble []PolicyTransaction
	policyTransactionSliceDouble = append(policyTransactionSliceDouble, policyTransactionTwo)
	policyTransactionSliceDouble = append(policyTransactionSliceDouble, policyTransactionOne)

	// Table driven test
	var tests = []struct {
		count int
		want  []PolicyTransaction
	}{
		{1, policyTransactionSliceSingle},
		{2, policyTransactionSliceDouble},
	}
	for _, tt := range tests {
		t.Run("LastPolicyTransactions", func(t *testing.T) {
			ans, err := LastPolicyTransactions(ctx, db, tt.count)
			if err != nil {
				t.Errorf("LastPolicyTransactions did not execute successfully: %v", err)
			}
			if !policyTransactionSliceEqual(ans, tt.want) {
				t.Errorf("got %v, want %v", ans, tt.want)
			}
		})
	}
}

func TestIsActionAllowed(t *testing.T) {
	db := newTestACLDB(t, "")
	ctx := context.Background()

	txPool := &TxPool{aclDB: db}

	t.Run("isActionAllowed - BlocklistMode - Policy Exists", func(t *testing.T) {
		SetMode(ctx, db, BlocklistMode)

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add the policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "blocklist", addr, policy))

		// Check if the action is allowed
		allowed, err := txPool.isActionAllowed(ctx, addr, policy)
		require.NoError(t, err)
		require.False(t, allowed) // In blocklist mode, having the policy means the action is not allowed
	})

	t.Run("isActionAllowed - BlocklistMode - Policy Does Not Exist", func(t *testing.T) {
		SetMode(ctx, db, BlocklistMode)

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := Deploy

		// Check if the action is allowed
		allowed, err := txPool.isActionAllowed(ctx, addr, policy)
		require.NoError(t, err)
		require.True(t, allowed) // In blocklist mode, not having the policy means the action is allowed
	})

	t.Run("isActionAllowed - AllowlistMode - Policy Exists", func(t *testing.T) {
		SetMode(ctx, db, AllowlistMode)

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Add the policy to the ACL
		require.NoError(t, AddPolicy(ctx, db, "allowlist", addr, policy))

		// Check if the action is allowed
		allowed, err := txPool.isActionAllowed(ctx, addr, policy)
		require.NoError(t, err)
		require.True(t, allowed) // In allowlist mode, having the policy means the action is allowed
	})

	t.Run("isActionAllowed - AllowlistMode - Policy Does Not Exist", func(t *testing.T) {
		SetMode(ctx, db, AllowlistMode)

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := Deploy

		// Check if the action is allowed
		allowed, err := txPool.isActionAllowed(ctx, addr, policy)
		require.NoError(t, err)
		require.False(t, allowed) // In allowlist mode, not having the policy means the action is not allowed
	})

	t.Run("isActionAllowed - DisabledMode", func(t *testing.T) {
		SetMode(ctx, db, DisabledMode)

		// Create a test address and policy
		addr := common.HexToAddress("0x1234567890abcdef")
		policy := SendTx

		// Check if the action is allowed
		allowed, err := txPool.isActionAllowed(ctx, addr, policy)
		require.NoError(t, err)
		require.True(t, allowed) // In disabled mode, all actions are allowed
	})
}
