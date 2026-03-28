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

package validate

// Rule is a single static-analysis check applied to one instruction in context.
//
// insns is the full decoded instruction slice; idx is the index of the current
// instruction being examined.  The idx+1 lookahead is used by rules that inspect
// the immediately following opcode (e.g. the GAS-before-CALL rule).
//
// A rule returns (Violation, true) on failure and (Violation{}, false) on pass.
type Rule func(insns []Insn, idx int) (Violation, bool)

// RuleSet is an ordered collection of Rules applied in sequence.
// Rules are evaluated per-instruction; the first violation found stops the scan.
type RuleSet []Rule

// Validate scans code against all rules in rs.
// It returns a pointer to the first Violation found, or nil if the code is clean.
// The scan stops at the first violation (fail-fast semantics appropriate for
// pool admission).
func (rs RuleSet) Validate(code []byte) *Violation {
	insns := Scan(code)
	for i := range insns {
		for _, rule := range rs {
			if v, ok := rule(insns, i); ok {
				return &v
			}
		}
	}
	return nil
}

// Extend returns a new RuleSet containing all rules in rs followed by extra.
// rs itself is not modified.
func (rs RuleSet) Extend(extra ...Rule) RuleSet {
	out := make(RuleSet, len(rs)+len(extra))
	copy(out, rs)
	copy(out[len(rs):], extra)
	return out
}
