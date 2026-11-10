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

package notifications

import (
	"bytes"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// RefreshAccounts appends final account values for addresses in an incomplete
// batch's recorded account changes, which may precede omitted updates. The getter
// must include uncommitted state because the batch can reach consumers before commit.
func (b *BlockBatchNotification) RefreshAccounts(getter kv.TemporalGetter) error {
	if !b.StateChangesIncomplete || len(b.Changes) == 0 {
		return nil
	}
	final := b.Changes[len(b.Changes)-1]
	final.Txs, final.Changes = nil, nil
	seen := make(map[common.Address]struct{})
	for _, block := range b.Changes {
		for _, change := range block.Changes {
			if change.Action != ActionUpsert && change.Action != ActionUpsertCode && change.Action != ActionRemove {
				continue
			}
			if _, ok := seen[change.Address]; ok {
				continue
			}
			seen[change.Address] = struct{}{}
			data, _, err := getter.GetLatest(kv.AccountsDomain, change.Address[:], kv.GetLatestOptions{})
			if err != nil {
				return fmt.Errorf("refresh notification account %s: %w", change.Address, err)
			}
			updated := AccountChange{Address: change.Address, Action: ActionRemove}
			if len(data) > 0 {
				var account accounts.Account
				if err := accounts.DeserialiseV3(&account, data); err != nil {
					return fmt.Errorf("decode notification account %s: %w", change.Address, err)
				}
				updated.Action = ActionUpsert
				updated.Incarnation = account.Incarnation
				updated.Data = bytes.Clone(data)
			}
			final.Changes = append(final.Changes, updated)
		}
	}
	if len(final.Changes) > 0 {
		b.Changes = append(b.Changes, final)
	}
	return nil
}
