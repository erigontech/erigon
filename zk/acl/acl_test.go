package acl

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnmarshalAcl(t *testing.T) {
	a, err := UnmarshalAcl("./test_acl_json/test-acl.json")
	assert.NoError(t, err)
	assert.NotNil(t, a)
	assert.NotNil(t, a.Allow)
	assert.NotNil(t, a.Allow.Deploy)
	assert.NotNil(t, a.Allow.Send)
	assert.NotNil(t, a.Deny)
	assert.NotNil(t, a.Deny.Deploy)
	assert.NotNil(t, a.Deny.Send)
}

func TestCheckAllow(t *testing.T) {
	a, err := UnmarshalAcl("./test_acl_json/test-acl-allow-only.json")
	assert.NoError(t, err)
	assert.NotNil(t, a)
	assert.True(t, a.AllowExists())
	assert.False(t, a.DenyExists())
}

func TestCheckDeny(t *testing.T) {
	a, err := UnmarshalAcl("./test_acl_json/test-acl-deny-only.json")
	assert.NoError(t, err)
	assert.NotNil(t, a)
	assert.True(t, a.DenyExists())
	assert.False(t, a.AllowExists())
}

func TestRuleTypeBlockAll(t *testing.T) {
	acl, err := UnmarshalAcl("./test_acl_json/test-acl.json")
	assert.NoError(t, err)
	assert.NotNil(t, acl)

	// Address is present in both so we should deny (deny trumps allow)
	v := NewPolicyValidator(acl)
	allowed, err := v.IsActionAllowed(context.Background(), common.HexToAddress("0x0000000000000000000000000000000000000000"), 0)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestIsActionAllowed(t *testing.T) {
	scenarios := map[string]struct {
		aclPath  string
		addr     common.Address
		policy   Policy
		expected bool
	}{
		"[Both List] Address is in both allow/deny. (Deny)": {
			aclPath:  "./test_acl_json/test-acl.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000000"),
			policy:   SendTx,
			expected: false,
		},
		"[Deny Only List] Address is in deny only. (Deny)": {
			aclPath:  "./test_acl_json/test-acl-deny-only.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000000"),
			policy:   SendTx,
			expected: false,
		},
		"[Allow Only List] Address is in allow only. (Allow)": {
			aclPath:  "./test_acl_json/test-acl-allow-only.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000000"),
			policy:   SendTx,
			expected: true,
		},
		"[Both List] Address is not found in either. (Deny)": {
			aclPath:  "./test_acl_json/test-acl.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000001"),
			policy:   SendTx,
			expected: false,
		},
		"[Deny Only List] Address is not found in deny list. (Allow)": {
			aclPath:  "./test_acl_json/test-acl-deny-only.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000001"),
			policy:   SendTx,
			expected: true,
		},
		"[Allow Only List] Address is not found in allow list. (Deny)": {
			aclPath:  "./test_acl_json/test-acl-allow-only.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000001"),
			policy:   SendTx,
			expected: false,
		},
		"[Both List] Address is on allow but not deny. (Allow)": {
			aclPath:  "./test_acl_json/test-acl-both-multiple-addr.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000001"),
			policy:   SendTx,
			expected: true,
		},
		"[Both List] Address is on allow for deploy allow for send. (Deny)": {
			aclPath:  "./test_acl_json/test-acl-both-multiple-addr.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000000"),
			policy:   SendTx,
			expected: false,
		},
		"[Both List] Address is on deny list but not allow and allow exists. (Deny)": {
			aclPath:  "./test_acl_json/test-acl-both-multiple-addr.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000004"),
			policy:   SendTx,
			expected: false,
		},
		"[Both List] Address is on deny for deploy but not send and allow list exists. (Deny)": {
			aclPath:  "./test_acl_json/test-acl-both-multiple-addr.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000003"),
			policy:   SendTx,
			expected: false,
		},
		"[Both List] Address is in both for deploy. (Deny)": {
			aclPath:  "./test_acl_json/test-acl.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000000"),
			policy:   Deploy,
			expected: false,
		},
		"[Both List] Address is on deny for send but not deploy and allow list exists. (Deny)": {
			aclPath:  "./test_acl_json/test-acl-both-multiple-addr.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000004"),
			policy:   Deploy,
			expected: false,
		},
		"[Both List] Address found in deny for deploy. (Deny)": {
			aclPath:  "./test_acl_json/test-acl-both-multiple-addr.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000003"),
			policy:   Deploy,
			expected: false,
		},
		"[Both List] Address not found in deny for deploy and address is in allow for deploy. (Allow)": {
			aclPath:  "./test_acl_json/test-acl-both-multiple-addr.json",
			addr:     common.HexToAddress("0x0000000000000000000000000000000000000000"),
			policy:   Deploy,
			expected: true,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			acl, err := UnmarshalAcl(scenario.aclPath)
			assert.NoError(t, err)
			assert.NotNil(t, acl)

			v := NewPolicyValidator(acl)
			allowed, err := v.IsActionAllowed(context.Background(), scenario.addr, scenario.policy.ToByte())
			assert.NoError(t, err)
			assert.Equal(t, scenario.expected, allowed)
		})
	}
}
