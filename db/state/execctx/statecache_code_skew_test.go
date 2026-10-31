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

package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func seedAccountCode(t *testing.T, db kv.TemporalRwDB, addr common.Address, account accounts.Account, code []byte) {
	t.Helper()

	ctx := t.Context()
	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedSD, err := execctx.NewSharedDomains(ctx, seedTx, log.New())
	require.NoError(t, err)
	defer seedSD.Close()
	seedSD.SetTxNum(10)
	require.NoError(t, seedSD.DomainPut(kv.AccountsDomain, seedTx, addr[:], accounts.SerialiseV3(&account), 10, nil))
	require.NoError(t, seedSD.DomainPut(kv.CodeDomain, seedTx, addr[:], code, 10, nil))
	require.NoError(t, seedSD.Commit(ctx, seedTx))
}

func TestCodeReadBetweenAccountAndCodeWritesDoesNotPoisonCodeCache(t *testing.T) {
	oldCode := []byte{0x60, 0x01}
	newCode := []byte{0x60, 0x02, 0x00}
	oldHash := crypto.Keccak256Hash(oldCode)
	newHash := crypto.Keccak256Hash(newCode)

	tests := []struct {
		name                string
		accountFirst        bool
		readSize            bool
		cachesCommittedCode bool
	}{
		{name: "account_first/GetCode", accountFirst: true, cachesCommittedCode: true},
		{name: "account_first/GetCodeSize", accountFirst: true, readSize: true, cachesCommittedCode: true},
		{name: "code_first_guard/GetCode"},
		{name: "code_first_guard/GetCodeSize", readSize: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			db := newTestDb(t, 16)
			var addr common.Address
			addr[0] = 0xea

			oldAcc := accounts.Account{Nonce: 64, CodeHash: accounts.InternCodeHash(oldHash)}
			newAcc := accounts.Account{Nonce: 68, CodeHash: accounts.InternCodeHash(newHash)}
			seedAccountCode(t, db, addr, oldAcc, oldCode)

			sc := cache.NewDefaultStateCache()
			t.Cleanup(sc.Close)
			require.True(t, sc.FillsEnabled(), "the cache assertions need reader fills (STATE_CACHE_FILLS)")
			roTx, err := db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer roTx.Rollback()

			read := func(getter execctxapi.StateGetter) any {
				if tt.readSize {
					size, ok, err := getter.GetCodeSize(addr[:], 21)
					require.NoError(t, err)
					require.True(t, ok)
					return size
				}
				code, ok, err := getter.GetCode(addr[:], 21)
				require.NoError(t, err)
				require.True(t, ok)
				return code
			}
			runBatch := func(readBetween bool) any {
				sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
				require.NoError(t, err)
				defer sd.Close()
				sd.BindStateCache(sc)
				getter := sd.AsStateGetter(roTx, execctxapi.StateGetterOptions{})

				if tt.accountFirst {
					require.NoError(t, sd.DomainPut(kv.AccountsDomain, roTx, addr[:], accounts.SerialiseV3(&newAcc), 20, nil))
					if readBetween {
						read(getter)
					}
					require.NoError(t, sd.DomainPut(kv.CodeDomain, roTx, addr[:], newCode, 20, nil))
				} else {
					require.NoError(t, sd.DomainPut(kv.CodeDomain, roTx, addr[:], newCode, 20, oldCode))
					if readBetween {
						read(getter)
					}
					require.NoError(t, sd.DomainPut(kv.AccountsDomain, roTx, addr[:], accounts.SerialiseV3(&newAcc), 20, nil))
				}

				return read(getter)
			}
			readCommitted := func() any {
				sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
				require.NoError(t, err)
				defer sd.Close()
				sd.BindStateCache(sc)
				return read(sd.AsStateGetter(roTx, execctxapi.StateGetterOptions{}))
			}

			var oldValue, newValue any = oldCode, newCode
			if tt.readSize {
				oldValue, newValue = len(oldCode), len(newCode)
			}

			assert.Equal(t, newValue, runBatch(true), "read after both writes")
			cached, ok := sc.View(nil).GetCodeByHash(oldHash[:])
			assert.Equal(t, tt.cachesCommittedCode, ok, "content cache entry for committed code")
			if ok {
				assert.Equal(t, oldCode, cached, "content cache bytes for committed code")
			}
			_, ok = sc.View(nil).GetCodeByHash(newHash[:])
			assert.False(t, ok, "content cache entry for uncommitted code")
			_, ok = sc.View(nil).GetCodeSizeByHash(newHash[:])
			assert.False(t, ok, "size cache entry for uncommitted code")
			assert.Equal(t, oldValue, readCommitted(), "fresh reader of committed state")
			assert.Equal(t, newValue, runBatch(false), "re-executed batch, no read between writes")
		})
	}
}
