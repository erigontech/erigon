// Copyright 2024 The Erigon Authors
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

package chain

import "fmt"

type RulesName string

const (
	AuRaRules   RulesName = "aura"
	EtHashRules RulesName = "ethash"
	BorRules    RulesName = "bor"
)

// ValidRulesNames is the set of recognised consensus engine names.
var ValidRulesNames = map[RulesName]struct{}{
	AuRaRules:   {},
	EtHashRules: {},
	BorRules:    {},
	"":          {}, // empty is valid (defaults to ethash)
}

// Validate returns an error if the RulesName is not a recognised consensus engine.
func (r RulesName) Validate() error {
	if _, ok := ValidRulesNames[r]; !ok {
		return fmt.Errorf("unsupported consensus engine %q (supported: aura, bor, ethash)", r)
	}
	return nil
}
