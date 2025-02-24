package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common"
	"io"
	"os"
)

type Acl struct {
	Allow *Rules `json:"allow"`
	Deny  *Rules `json:"deny"`
}

type Rules struct {
	Deploy []common.Address `json:"deploy"`
	Send   []common.Address `json:"send"`
}

func UnmarshalAcl(path string) (*Acl, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var acl Acl
	if err = json.Unmarshal(data, &acl); err != nil {
		return nil, err
	}

	return &acl, nil
}

func (a *Acl) AllowExists() bool {
	if a.Allow == nil {
		return false
	}
	return len(a.Allow.Deploy) > 0 || len(a.Allow.Send) > 0
}

func (a *Acl) DenyExists() bool {
	if a.Deny == nil {
		return false
	}
	return len(a.Deny.Deploy) > 0 || len(a.Deny.Send) > 0
}

type Policy byte

const (
	SendTx Policy = iota
	Deploy
)

func (p Policy) ToByte() byte {
	return byte(p)
}

var errUnknownPolicy = errors.New("unknown policy")

func resolvePolicyByte(policy byte) (Policy, error) {
	switch policy {
	case 0:
		return SendTx, nil
	case 1:
		return Deploy, nil
	default:
		return SendTx, errUnknownPolicy
	}
}

type Validator struct {
	acl *Acl
}

func NewPolicyValidator(acl *Acl) *Validator {
	return &Validator{acl: acl}
}

func (v *Validator) IsActionAllowed(ctx context.Context, addr common.Address, policy byte) (bool, error) {
	p, err := resolvePolicyByte(policy)
	if err != nil {
		return false, err
	}

	hasDenyPolicy, err := v.acl.AddressHasDenyPolicy(p, addr)
	if err != nil {
		return false, err
	}

	// if we have Deny policy, we should return false
	// and not even check for Allow policy
	if hasDenyPolicy {
		return false, nil
	}

	if !v.acl.AllowExists() {
		return true, nil
	}

	hasAllowPolicy, err := v.acl.AddressHasAllowPolicy(p, addr)
	if err != nil {
		return false, err
	}

	if hasAllowPolicy {
		return true, nil
	}

	return false, nil
}

func (a *Acl) AddressHasDenyPolicy(policy Policy, addr common.Address) (bool, error) {
	switch policy {
	case SendTx:
		if a.DenyExists() {
			for _, allowed := range a.Deny.Send {
				if allowed == addr {
					return true, nil
				}
			}
		}
		return false, nil
	case Deploy:
		if a.DenyExists() {
			for _, allowed := range a.Deny.Deploy {
				if allowed == addr {
					return true, nil
				}
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("invalid policy: %v", policy)
	}
}

func (a *Acl) AddressHasAllowPolicy(policy Policy, addr common.Address) (bool, error) {
	switch policy {
	case SendTx:
		if a.AllowExists() {
			for _, allowed := range a.Allow.Send {
				if allowed == addr {
					return true, nil
				}
			}
		}
		return false, nil
	case Deploy:
		if a.AllowExists() {
			for _, allowed := range a.Allow.Deploy {
				if allowed == addr {
					return true, nil
				}
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("invalid policy: %v", policy)
	}
}
