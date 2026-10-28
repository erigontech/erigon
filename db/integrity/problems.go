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

package integrity

import (
	"fmt"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
)

// problems tallies the failures a check reported instead of stopping on. failFast decides where a
// check stops, not whether the run failed, so a check that was asked to carry on still has to come
// back as a failure. Safe for concurrent use: checks fan out over block ranges.
type problems struct{ n atomic.Int64 }

// report hands the error back when failFast is set, so the caller stops there. Otherwise it logs
// and tallies the problem, and returns nil so the caller carries on.
func (p *problems) report(failFast bool, err error) error {
	if failFast {
		return err
	}
	p.n.Add(1)
	log.Error(err.Error())
	return nil
}

// verdict is what a check returns once it has walked everything it was given.
func (p *problems) verdict(check string) error {
	if n := p.n.Load(); n > 0 {
		return fmt.Errorf("%w: %s: found %d problem(s), listed above", ErrIntegrity, check, n)
	}
	return nil
}
