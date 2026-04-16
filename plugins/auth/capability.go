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

package auth

import (
	"fmt"
	"strings"
)

// Capability represents a UCAN capability — a command path with optional
// policy constraints. Capabilities follow a hierarchical namespace:
//
//	/storage/*       — all storage operations
//	/storage/read    — read only
//	/id/decrypt      — decrypt identity claims
//	/auth/delegate   — create delegations
type Capability struct {
	Command string   `json:"cmd"`
	Policy  []Policy `json:"pol,omitempty"`
}

// Policy is a constraint on a capability: [field, operator, value].
// Operators: "==", "!=", "in", "subset", "glob".
type Policy struct {
	Field    string `json:"field"`
	Operator string `json:"op"`
	Value    any    `json:"value"`
}

// Covers returns true if this capability authorizes the required capability.
// A capability covers another if:
//  1. The command is equal or a parent wildcard (e.g. /storage/* covers /storage/read)
//  2. All policies on this capability are satisfied by the required capability
//
// Policy evaluation: each policy on this (granting) capability must be matched
// by a corresponding policy on the required capability with the same key and
// a value that satisfies the operator. This is a simple equality check for v1;
// range/comparison operators are a future extension.
func (c Capability) Covers(required Capability) bool {
	if !commandCovers(c.Command, required.Command) {
		return false
	}
	// Every policy on the granting capability must be satisfied.
	for _, grantPol := range c.Policy {
		matched := false
		for _, reqPol := range required.Policy {
			if grantPol.Field == reqPol.Field && policyValueSatisfies(grantPol, reqPol.Value) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// policyValueSatisfies checks if the required value satisfies the granting policy.
// For v1, only equality ("eq") is supported.
func policyValueSatisfies(grant Policy, requiredValue any) bool {
	switch grant.Operator {
	case "eq", "":
		return fmt.Sprintf("%v", grant.Value) == fmt.Sprintf("%v", requiredValue)
	default:
		return false
	}
}

// commandCovers checks if the granting command covers the required command.
//
// Rules:
//   - Exact match: "/storage/read" covers "/storage/read"
//   - Wildcard: "/storage/*" covers "/storage/read", "/storage/write"
//   - Deep wildcard: "/*" covers everything
//   - Parent does NOT cover child without wildcard: "/storage" does NOT cover "/storage/read"
func commandCovers(granting, required string) bool {
	if granting == required {
		return true
	}

	// Wildcard matching
	if strings.HasSuffix(granting, "/*") {
		prefix := strings.TrimSuffix(granting, "*")
		return strings.HasPrefix(required, prefix)
	}

	return false
}

// Attenuates returns true if the child capability is a valid attenuation
// (narrowing) of the parent capability. Attenuation can only narrow scope:
//   - Command must be equal or more specific
//   - Policies can be added or tightened, never removed or loosened
func (c Capability) Attenuates(parent Capability) bool {
	// Child command must be covered by parent command
	if !commandCovers(parent.Command, c.Command) {
		return false
	}
	// Additional policies on child are fine (they narrow scope)
	// We don't validate policy tightening here — that requires
	// knowing the policy semantics, which is application-specific.
	return true
}

// String returns a human-readable representation.
func (c Capability) String() string {
	if len(c.Policy) == 0 {
		return c.Command
	}
	return fmt.Sprintf("%s (with %d policies)", c.Command, len(c.Policy))
}

// Well-known capability namespaces.
const (
	NamespaceStorage = "/storage"
	NamespaceID      = "/id"
	NamespaceWebF    = "/webf"
	NamespaceAuth    = "/auth"
)
