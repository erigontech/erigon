package txpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/types"
)

const (
	logCountPolicyTransactions = "log_count_output" // config variable name
)

// Operation
type Operation byte

const (
	Add Operation = iota
	Remove
	Update
	ModeChange
)

func (p Operation) ToByte() byte {
	return byte(p)
}

func (p Operation) ToByteArray() []byte {
	return []byte{byte(p)}
}

// Convert byte back to Operation
func OperationFromByte(b byte) Operation {
	switch b {
	case byte(Add):
		return Add
	case byte(Remove):
		return Remove
	case byte(Update):
		return Update
	default:
		return Add // Default or error handling can be added here
	}
}

// Convert Operation to string
func (p Operation) String() string {
	switch p {
	case Add:
		return "add"
	case Remove:
		return "remove"
	case Update:
		return "update"
	case ModeChange:
		return "mode change"
	default:
		return "unknown operation"
	}
}

// ACLType Binary
type ACLTypeBinary byte

const (
	AllowListTypeB ACLTypeBinary = iota
	BlockListTypeB
	DisabledModeB
)

func (p ACLTypeBinary) ToByte() byte {
	return byte(p)
}

func (p ACLTypeBinary) ToByteArray() []byte {
	return []byte{byte(p)}
}

// To be used in encoding, avoid in other cases
func ResolveACLTypeToBinary(aclType string) ACLTypeBinary {
	switch aclType {
	case string(AllowListType):
		return AllowListTypeB
	case string(BlockListType):
		return BlockListTypeB
	case string(DisabledMode):
		return DisabledModeB
	}
	return BlockListTypeB
}

// Convert byte back to ACLTypeBinary
func ACLTypeBinaryFromByte(b byte) ACLTypeBinary {
	switch b {
	case byte(AllowListTypeB):
		return AllowListTypeB
	case byte(BlockListTypeB):
		return BlockListTypeB
	case byte(DisabledModeB):
		return DisabledModeB
	default:
		return BlockListTypeB // Default or error handling can be added here
	}
}

// Convert ACLTypeBinary to string
func (p ACLTypeBinary) String() string {
	switch p {
	case AllowListTypeB:
		return "allowlist"
	case BlockListTypeB:
		return "blocklist"
	case DisabledModeB:
		return "disabled"
	default:
		return "Unknown ACLTypeBinary"
	}
}

// Policy is a named policy
type Policy byte

// when a new Policy is added, it should be added to policiesList also.
const (
	// SendTx is the name of the policy that governs that an address may send transactions to pool
	SendTx Policy = iota
	// Deploy is the name of the policy that governs that an address may deploy a contract
	Deploy
)

var policiesList = []Policy{SendTx, Deploy}

func (p Policy) ToByte() byte {
	return byte(p)
}

func (p Policy) ToByteArray() []byte {
	return []byte{byte(p)}
}

// IsSupportedPolicy checks if the given policy is supported
func IsSupportedPolicy(policy Policy) bool {
	switch policy {
	case SendTx, Deploy:
		return true
	default:
		return false
	}
}

func ResolvePolicy(policy string) (Policy, error) {
	switch policy {
	case "sendTx":
		return SendTx, nil
	case "deploy":
		return Deploy, nil
	default:
		return SendTx, errUnknownPolicy
	}
}

// containsPolicy checks if the given policy is present in the policy list
func containsPolicy(policies []byte, policy Policy) bool {
	return bytes.Contains(policies, policy.ToByteArray())
}

// address policyMapping returns a string of user policies.
func policyMapping(policies []byte, pList []Policy) string {
	policyPresence := make(map[string]bool)

	for _, policy := range pList {
		// Check if the policy exists in the provided byte slice
		exists := bytes.Contains(policies, policy.ToByteArray())
		if policyName(policy) == "unknown" {
			continue
		}
		// Store the result in the map with the policy name
		policyPresence[policyName(policy)] = exists
	}

	// could be used to return a map here

	// Create a slice to hold the formatted policy strings
	formattedPolicies := make([]string, 0, len(policyPresence))

	// Populate the slice with formatted strings
	for policy, exists := range policyPresence {
		formattedPolicies = append(formattedPolicies, fmt.Sprintf("\t%s: %v", policy, exists))
	}

	// Join the formatted strings with ", "
	return strings.Join(formattedPolicies, "\n")
}

// policyName returns the string name of a policy
func policyName(policy Policy) string {
	switch policy {
	case SendTx:
		return "sendTx"
	case Deploy:
		return "deploy"
	default:
		return "unknown"
	}
}

// DoesAccountHavePolicy checks if the given account has the given policy for the online ACL mode
func DoesAccountHavePolicy(ctx context.Context, aclDB kv.RwDB, addr common.Address, policy Policy) (bool, error) {
	hasPolicy, _, err := checkIfAccountHasPolicy(ctx, aclDB, addr, policy)
	return hasPolicy, err
}

func checkIfAccountHasPolicy(ctx context.Context, aclDB kv.RwDB, addr common.Address, policy Policy) (bool, ACLMode, error) {
	if !IsSupportedPolicy(policy) {
		return false, DisabledMode, errUnknownPolicy
	}

	// Retrieve the mode configuration
	var (
		hasPolicy bool
		mode      ACLMode = DisabledMode
	)

	err := aclDB.View(ctx, func(tx kv.Tx) error {
		value, err := tx.GetOne(Config, []byte("mode"))
		if err != nil {
			return err
		}

		if value == nil || string(value) == DisabledMode {
			hasPolicy = true
			return nil
		}

		mode = ACLMode(value)

		table := BlockList
		if mode == AllowlistMode {
			table = Allowlist
		}

		var policyBytes []byte
		value, err = tx.GetOne(table, addr.Bytes())
		if err != nil {
			return err
		}

		policyBytes = value
		if policyBytes != nil && containsPolicy(policyBytes, policy) {
			// If address is in the allowlist and has the policy, return true
			// If address is in the blocklist and has the policy, return false
			hasPolicy = true
		}

		return nil
	})
	if err != nil {
		return false, mode, err
	}

	return hasPolicy, mode, nil
}

// UpdatePolicies sets a policy for an address
func UpdatePolicies(ctx context.Context, aclDB kv.RwDB, aclType string, addrs []common.Address, policies [][]Policy) error {
	table, err := resolveTable(aclType)
	if err != nil {
		return err
	}
	// Create an array to hold policy transactions
	var policyTransactions []PolicyTransaction
	timeNow := time.Now()
	err = aclDB.Update(ctx, func(tx kv.RwTx) error {
		for i, addr := range addrs {
			// Add the policy transaction to the array
			policyTransactions = append(policyTransactions, PolicyTransaction{
				aclType:   ResolveACLTypeToBinary(aclType),
				addr:      addr,
				operation: Update,
				timeTx:    timeNow,
			})

			if len(policies[i]) > 0 {
				// just update the policies for the address to match the one provided
				policyBytes := make([]byte, 0, len(policies[i]))
				for _, p := range policies[i] {
					policyBytes = append(policyBytes, p.ToByte())
				}
				// Update the policies in the table
				if err := tx.Put(table, addr.Bytes(), policyBytes); err != nil {
					return err
				}
				continue
			}

			// remove the address from the table
			if err := tx.Delete(table, addr.Bytes()); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Insert policy transaction for adding or updating the policy
	// I'm omiting the policies in this case.
	return InsertPolicyTransactions(ctx, aclDB, policyTransactions)
}

type PolicyTransaction struct {
	addr      common.Address //  20 bytes in size
	aclType   ACLTypeBinary
	policy    Policy
	operation Operation
	timeTx    time.Time
}

// Convert time.Time to bytes (Unix timestamp)
func timestampToBytes(t time.Time) []byte {
	unixTime := t.Unix()                              // Get Unix timestamp (seconds since epoch)
	buf := make([]byte, 8)                            // Allocate 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(unixTime)) // Store as big-endian bytes
	return buf
}

func InsertPolicyTransactions(ctx context.Context, aclDB kv.RwDB, pts []PolicyTransaction) error {
	return aclDB.Update(ctx, func(tx kv.RwTx) error {
		for _, pt := range pts {
			t := pt.timeTx
			// Convert time.Time to bytes
			unixBytes := timestampToBytes(t)
			// composite key.
			addressTimestamp := append(pt.addr.Bytes(), unixBytes...)
			value := append([]byte{pt.aclType.ToByte(), pt.operation.ToByte(), pt.policy.ToByte()}, addressTimestamp...)

			if err := tx.Put(PolicyTransactions, addressTimestamp, value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Convert bytes back to time.Time (Unix timestamp)
func bytesToTimestamp(b []byte) time.Time {
	if len(b) != 8 {
		// Handle error, invalid byte slice length
		return time.Time{}
	}
	unixTime := int64(binary.BigEndian.Uint64(b)) // Convert bytes to int64 (Unix timestamp)
	return time.Unix(unixTime, 0)                 // Convert Unix time to time.Time
}

// LastPolicyTransactions returns the last n policy transactions, defined by logCountPolicyTransactions config variable
func LastPolicyTransactions(ctx context.Context, aclDB kv.RwDB, count int) ([]PolicyTransaction, error) {
	var pts []PolicyTransaction
	err := aclDB.View(ctx, func(tx kv.Tx) error {
		if count == 0 {
			return nil
		}
		c, err := tx.Cursor(PolicyTransactions)
		if err != nil {
			return err
		}
		defer c.Close()
		_, value, err := c.Last()
		if err != nil {
			return err
		}

		pt, err := byteToPolicyTransaction(value)
		if err != nil {
			return err
		}
		pts = append(pts, pt)

		for i := 1; i < count; i++ {
			_, value, err = c.Prev()
			if err != nil {
				return err
			}

			pt, err := byteToPolicyTransaction(value)
			if err != nil {
				return err
			}
			pts = append(pts, pt)
		}
		return nil
	})

	return pts, err
}

func byteToPolicyTransaction(value []byte) (PolicyTransaction, error) {
	// if the length is the size of mode change = 9, then it is a mode change transaction
	if len(value) == 9 {
		return PolicyTransaction{
			aclType:   ACLTypeBinary(value[0]),
			timeTx:    bytesToTimestamp(value[1:9]),
			operation: ModeChange,
		}, nil
	}

	// Check for expected length:
	// 1 byte for aclType,
	// 1 byte for operation,
	// 1 byte for policy,
	// 20 bytes for address,
	// 8 bytes for timestamp = 31 bytes in total
	if len(value) != 31 {
		return PolicyTransaction{}, fmt.Errorf("invalid value length %d", len(value))
	}

	// Extract aclType from the first byte
	aclType := ACLTypeBinary(value[0])

	// Extract operation from the second byte
	operation := Operation(value[1])

	// Extract policy from the third byte
	policy := Policy(value[2])

	// Extract address from the next 20 bytes (3 to 22 inclusive)
	var addr common.Address
	copy(addr[:], value[3:23])

	// Extract timestamp from the last 8 bytes (23 to 30 inclusive)
	timestampBytes := value[23:31]
	timeTx := bytesToTimestamp(timestampBytes)

	// Return the reconstructed PolicyTransaction struct
	return PolicyTransaction{
		aclType:   aclType,
		addr:      addr,
		policy:    policy,
		operation: operation,
		timeTx:    timeTx,
	}, nil
}

func (pt PolicyTransaction) ToString() string {
	// on mode change we only have aclType and timeTx
	// so we need to check if the operation is ModeChange
	// to print the correct information to display
	if pt.operation == ModeChange {
		return fmt.Sprintf("ACLType: %s, Operation: %s, Time: %s",
			pt.aclType.String(),
			pt.operation.String(),
			pt.timeTx.Format(time.RFC3339)) // Use RFC3339 format for the
	}
	return fmt.Sprintf("ACLType: %s, Address: %s, Policy: %s, Operation: %s, Time: %s",
		pt.aclType.String(),
		hex.EncodeToString(pt.addr[:]), // Convert address to hexadecimal string representation
		policyName(pt.policy),          // Use policyName function to get the policy name
		pt.operation.String(),
		pt.timeTx.Format(time.RFC3339)) // Use RFC3339 format for the time
}

// AddPolicy adds a policy to the ACL of given address
func AddPolicy(ctx context.Context, aclDB kv.RwDB, aclType string, addr common.Address, policy Policy) error {
	if !IsSupportedPolicy(policy) {
		return errUnknownPolicy
	}

	table, err := resolveTable(aclType)
	if err != nil {
		return err
	}

	err = aclDB.Update(ctx, func(tx kv.RwTx) error {
		value, err := tx.GetOne(table, addr.Bytes())
		if err != nil {
			return err
		}

		policyBytes := policy.ToByteArray()
		if value == nil {
			return tx.Put(table, addr.Bytes(), policyBytes)
		}

		// Check if the policy already exists
		if containsPolicy(value, policy) {
			return nil
		}

		value = append(value, policyBytes...)

		return tx.Put(table, addr.Bytes(), value)
	})
	if err != nil {
		return err
	}

	err = InsertPolicyTransactions(ctx, aclDB, []PolicyTransaction{{
		aclType:   ResolveACLTypeToBinary(aclType),
		addr:      addr,
		policy:    policy,
		operation: Add,
		timeTx:    time.Now(),
	}})

	return err
}

// RemovePolicy removes a policy from the ACL of given address
func RemovePolicy(ctx context.Context, aclDB kv.RwDB, aclType string, addr common.Address, policy Policy) error {
	table, err := resolveTable(aclType)
	if err != nil {
		return err
	}

	err = aclDB.Update(ctx, func(tx kv.RwTx) error {
		policies, err := tx.GetOne(table, addr.Bytes())
		if err != nil {
			return err
		}
		if policies == nil {
			// No policies exist for this address
			return nil
		}

		updatedPolicies := []byte{}

		for _, p := range policies {
			if p != policy.ToByte() {
				updatedPolicies = append(updatedPolicies, p)
			}
		}

		if len(updatedPolicies) == 0 {
			return tx.Delete(table, addr.Bytes())
		}

		return tx.Put(table, addr.Bytes(), updatedPolicies)
	})
	if err != nil {
		return err
	}

	err = InsertPolicyTransactions(ctx, aclDB, []PolicyTransaction{{
		aclType:   ResolveACLTypeToBinary(aclType),
		addr:      addr,
		policy:    policy,
		operation: Remove,
		timeTx:    time.Now(),
	}})

	return err
}

func ListContentAtACL(ctx context.Context, db kv.RwDB) (string, error) {

	var buffer bytes.Buffer

	tables := db.AllTables()
	buffer.WriteString("ListContentAtACL\n")
	buffer.WriteString("Tables\nTable - { Flags, AutoDupSortKeysConversion, IsDeprecated, DBI, DupFromLen, DupToLen }\n")
	for key, config := range tables {
		buffer.WriteString(fmt.Sprint(key, config, "\n"))
	}

	err := db.View(ctx, func(tx kv.Tx) error {
		// Config table
		buffer.WriteString("\nConfig\n")
		err := tx.ForEach(Config, nil, func(k, v []byte) error {
			buffer.WriteString(fmt.Sprintf("Key: %s, Value: %s\n", string(k), string(v)))
			return nil
		})

		// BlockList table
		var BlockListContent strings.Builder
		err = tx.ForEach(BlockList, nil, func(k, v []byte) error {
			BlockListContent.WriteString(fmt.Sprintf(
				"Key: %s, Value: {\n%s\n}\n",
				hex.EncodeToString(k),
				policyMapping(v, policiesList),
			))
			return nil
		})
		if err != nil {
			return err
		}
		if BlockListContent.String() != "" {
			buffer.WriteString(fmt.Sprintf(
				"\nBlocklist\n%s",
				BlockListContent.String(),
			))
		} else {
			buffer.WriteString("\nBlocklist is empty")
		}

		// Allowlist table
		var AllowlistContent strings.Builder
		err = tx.ForEach(Allowlist, nil, func(k, v []byte) error {
			AllowlistContent.WriteString(fmt.Sprintf(
				"Key: %s, Value: {\n%s\n}\n",
				hex.EncodeToString(k),
				policyMapping(v, policiesList),
			))
			return nil
		})
		if err != nil {
			return err
		}
		if AllowlistContent.String() != "" {
			buffer.WriteString(fmt.Sprintf(
				"\nAllowlist\n%s",
				AllowlistContent.String(),
			))
		} else {
			buffer.WriteString("\nAllowlist is empty")
		}

		return err
	})

	return buffer.String(), err
}

// SetMode sets the mode of the ACL
func SetMode(ctx context.Context, aclDB kv.RwDB, mode string) error {
	m, err := ResolveACLMode(mode)
	if err != nil {
		return err
	}

	return aclDB.Update(ctx, func(tx kv.RwTx) error {
		err := tx.Put(Config, []byte(modeKey), []byte(m))

		// Timestamp bytes + single byte.
		mb := ResolveACLTypeToBinary(mode)
		unixBytes := timestampToBytes(time.Now())
		addressMode := append(mb.ToByteArray(), unixBytes...)
		value := append([]byte{mb.ToByte()}, unixBytes...)
		if err = tx.Put(PolicyTransactions, addressMode, value); err != nil {
			return err
		}

		return err
	})
}

// GetMode gets the mode of the ACL
func GetMode(ctx context.Context, aclDB kv.RwDB) (ACLMode, error) {
	var mode ACLMode
	err := aclDB.View(ctx, func(tx kv.Tx) error {
		value, err := tx.GetOne(Config, []byte(modeKey))
		if err != nil {
			return err
		}

		mode = ACLMode(value)
		return nil
	})

	return mode, err
}

// resolveTable resolves the ACL table based on aclType
func resolveTable(aclType string) (string, error) {
	at, err := ResolveACLType(aclType)
	if err != nil {
		return "", err
	}

	table := BlockList
	if at == AllowListType {
		table = Allowlist
	}

	return table, nil
}

// create a method to resolve policy which will decode a tx to either sendTx or deploy policy
func resolvePolicy(txn *types.TxSlot) Policy {
	if txn.Creation {
		return Deploy
	}
	return SendTx
}

// isActionAllowed checks if the given action is allowed for the given address
func (p *TxPool) isActionAllowed(ctx context.Context, addr common.Address, policy Policy) (bool, error) {
	hasPolicy, mode, err := checkIfAccountHasPolicy(ctx, p.aclDB, addr, policy)
	if err != nil {
		return false, err
	}

	switch mode {
	case BlocklistMode:
		// If the mode is blocklist, and address has a certain policy, then invert the result
		// because, for example, if it has sendTx policy, it means it is not allowed to sendTx
		return !hasPolicy, nil
	default:
		return hasPolicy, nil
	}
}
