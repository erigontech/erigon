package txpool

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/types"
)

// PolicyName is a named policy
type PolicyName string

const (
	// SendTx is the name of the policy that governs that an address may send transactions to pool
	SendTx PolicyName = "send_tx"
	// Deploy is the name of the policy that governs that an address may deploy a contract
	Deploy PolicyName = "deploy"
)

// containsPolicy checks if the given policy is present in the policy list
func containsPolicy(policies []byte, policy PolicyName) bool {
	return bytes.Contains(policies, []byte(policy))
}

// create a method checkpolicy to check an address according to passed policy in the method
func (p *TxPool) checkPolicy(addr common.Address, policy PolicyName) (bool, error) {
	var whitelistPolicy []byte
	err := p.aclDB.View(context.TODO(), func(tx kv.Tx) error {
		value, err := tx.GetOne("Whitelist", addr.Bytes())
		if err != nil {
			return err
		}
		whitelistPolicy = value
		return nil
	})
	if err != nil {
		return false, err
	}
	if whitelistPolicy != nil && containsPolicy(whitelistPolicy, policy) {
		// If address is in the whitelist and has the policy, return true
		return true, nil
	}

	var blacklistPolicy []byte
	err = p.aclDB.View(context.TODO(), func(tx kv.Tx) error {
		value, err := tx.GetOne("Blacklist", addr.Bytes())
		if err != nil {
			return err
		}
		blacklistPolicy = value
		return nil
	})
	if err != nil {
		return false, err
	}
	if blacklistPolicy != nil && containsPolicy(blacklistPolicy, policy) {
		// If address is in the blacklist and has the policy, return false
		return false, nil
	}

	// If the address is not in either list, return false
	return false, nil
}

// create a method to resolve policy which will decode a tx to either sendTx or deploy policy
func resolvePolicy(txn *types.TxSlot) PolicyName {
	if txn.Creation {
		return Deploy
	}
	return SendTx
}

// create a method to setpolicy which will set a policy for an address in the db
func (p *TxPool) setpolicy(addr common.Address, policy PolicyName, bucket string) error {
	return p.aclDB.Update(context.TODO(), func(tx kv.RwTx) error {
		value, err := tx.GetOne(bucket, addr.Bytes())
		if err != nil {
			return err
		}
		var policies []byte
		if value != nil {
			policies = value
			// Check if the policy already exists
			if bytes.Contains(policies, []byte(policy)) {
				return nil
			}
			// Append the new policy
			policies = append(policies, byte(','))
			policies = append(policies, []byte(policy)...)
		} else {
			// New entry
			policies = []byte(policy)
		}
		return tx.Put(bucket, addr.Bytes(), policies)
	})
}

// method to remove a address from policy
func (p *TxPool) removepolicy(addr common.Address, policy PolicyName, bucket string) error {
	return p.aclDB.Update(context.TODO(), func(tx kv.RwTx) error {
		value, err := tx.GetOne(bucket, addr.Bytes())
		if err != nil {
			return err
		}
		if value == nil {
			// No policies exist for this address
			return nil
		}

		policies := bytes.Split(value, []byte(","))
		var updatedPolicies [][]byte

		for _, p := range policies {
			if string(p) != string(policy) {
				updatedPolicies = append(updatedPolicies, p)
			}
		}

		if len(updatedPolicies) == 0 {
			return tx.Delete(bucket, addr.Bytes())
		}

		// Join the updated policies back into a single byte slice
		updatedValue := bytes.Join(updatedPolicies, []byte(","))
		return tx.Put(bucket, addr.Bytes(), updatedValue)
	})
}
