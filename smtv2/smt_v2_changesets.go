package smtv2

import (
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/holiman/uint256"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/smt/pkg/utils"
)

func init() {
	var err error
	commonHashCache, err = NewCommonHashCache()
	if err != nil {
		panic("could not create commonHashCache")
	}
}

var zeroUint256 = uint256.NewInt(0)

var commonHashCache *CommonHashCache

type CommonHashCache struct {
	nonceKeyLru   *lru.Cache[common.Address, SmtKey]
	balanceKeyLru *lru.Cache[common.Address, SmtKey]
}

func NewCommonHashCache() (*CommonHashCache, error) {
	nonceKeyLru, err := lru.New[common.Address, SmtKey]("commonHashCache", 1000)
	if err != nil {
		return nil, err
	}

	balanceKeyLru, err := lru.New[common.Address, SmtKey]("commonHashCache", 1000)
	if err != nil {
		return nil, err
	}

	return &CommonHashCache{
		nonceKeyLru:   nonceKeyLru,
		balanceKeyLru: balanceKeyLru,
	}, nil
}

func (c *CommonHashCache) GetNonceKey(addr common.Address) SmtKey {
	val, found := c.nonceKeyLru.Get(addr)
	if found {
		return val
	}

	nonceKey := AddrKeyNonce(addr)
	c.nonceKeyLru.Add(addr, nonceKey)

	return nonceKey
}

func (c *CommonHashCache) GetBalanceKey(addr common.Address) SmtKey {
	val, found := c.balanceKeyLru.Get(addr)
	if found {
		return val
	}

	balanceKey := AddrKeyBalance(addr)
	c.balanceKeyLru.Add(addr, balanceKey)

	return balanceKey
}

type AccountChange struct {
	isNew bool
	old   accounts.Account
	new   accounts.Account
}

type BytesChanged struct {
	old []byte
	new []byte
}

type ChangesetMapper struct {
	accountChanges map[common.Address]AccountChange
	codeChanges    map[common.Address]BytesChanged
	storageChanges map[common.Address]map[common.Hash]BytesChanged
}

func NewChangesetMapper() *ChangesetMapper {
	return &ChangesetMapper{
		accountChanges: make(map[common.Address]AccountChange),
		codeChanges:    make(map[common.Address]BytesChanged),
		storageChanges: make(map[common.Address]map[common.Hash]BytesChanged),
	}
}

func (c *ChangesetMapper) ProcessNewAccount(addr common.Address, account accounts.Account, code []byte) {
	_, found := c.accountChanges[addr]
	if found {
		return
	}
	c.accountChanges[addr] = AccountChange{isNew: true, new: account}

	if len(code) > 0 {
		c.ProcessAccountCodeChange(addr, []byte{}, code)
	}
}

func (c *ChangesetMapper) ProcessAccountChange(addr common.Address, old accounts.Account, new accounts.Account) {
	change, found := c.accountChanges[addr]
	if !found {
		change = AccountChange{isNew: false, old: old, new: new}
	} else {
		change.new = new
	}
	c.accountChanges[addr] = change
}

func (c *ChangesetMapper) ProcessAccountCodeChange(addr common.Address, old []byte, new []byte) {
	change, found := c.codeChanges[addr]
	if !found {
		change = BytesChanged{old: old, new: new}
	} else {
		change.new = new
	}
	c.codeChanges[addr] = change
}

func (c *ChangesetMapper) ProcessStorageChange(addr common.Address, key common.Hash, old []byte, new []byte) {
	change, found := c.storageChanges[addr]
	if !found {
		change = make(map[common.Hash]BytesChanged)
	}

	record, found := change[key]
	if !found {
		record = BytesChanged{old: old, new: new}
	} else {
		record.new = new
	}
	change[key] = record
	c.storageChanges[addr] = change
}

func (c *ChangesetMapper) GenerateChangeEntries() ([]ChangeSetEntry, error) {
	changeSet := make([]ChangeSetEntry, 0)

	for addr, change := range c.accountChanges {
		if change.isNew {
			if change.new.Nonce != 0 {
				nonceValue := ScalarToSmtValue8FromBytes(big.NewInt(int64(change.new.Nonce)))
				changeSet = append(changeSet, ChangeSetEntry{
					Type:  ChangeSetEntryType_Add,
					Key:   commonHashCache.GetNonceKey(addr),
					Value: nonceValue,
				})
			}

			if change.new.Balance.Cmp(zeroUint256) > 0 {
				balanceValue := ScalarToSmtValue8FromBytes(change.new.Balance.ToBig())
				changeSet = append(changeSet, ChangeSetEntry{
					Type:  ChangeSetEntryType_Add,
					Key:   commonHashCache.GetBalanceKey(addr),
					Value: balanceValue,
				})
			}
		} else {
			// first the nonce
			if change.old.Nonce != change.new.Nonce {
				nonceKey := commonHashCache.GetNonceKey(addr)
				newVal := ScalarToSmtValue8FromBytes(big.NewInt(int64(change.new.Nonce)))
				oldVal := ScalarToSmtValue8FromBytes(big.NewInt(int64(change.old.Nonce)))

				setType := ChangeSetEntryType_Change
				if oldVal.IsZero() && !newVal.IsZero() {
					setType = ChangeSetEntryType_Add
				} else if !oldVal.IsZero() && newVal.IsZero() {
					setType = ChangeSetEntryType_Delete
				}

				changeSet = append(changeSet, ChangeSetEntry{
					Type:          setType,
					Key:           nonceKey,
					Value:         newVal,
					OriginalValue: oldVal,
				})

			}

			// then the balance
			if change.old.Balance != change.new.Balance {
				balanceKey := commonHashCache.GetBalanceKey(addr)
				newVal := ScalarToSmtValue8FromBytes(change.new.Balance.ToBig())
				oldVal := ScalarToSmtValue8FromBytes(change.old.Balance.ToBig())

				// special case where the balance starts at 0 and ends at 0 after some changes in between
				if newVal.IsZero() && oldVal.IsZero() {
					continue
				}

				setType := ChangeSetEntryType_Change
				if newVal.IsZero() {
					setType = ChangeSetEntryType_Delete
				} else if oldVal.IsZero() {
					setType = ChangeSetEntryType_Add
				}
				changeSet = append(changeSet, ChangeSetEntry{
					Type:          setType,
					Key:           balanceKey,
					Value:         newVal,
					OriginalValue: oldVal,
				})
			}
		}
	}

	for addr, change := range c.codeChanges {
		keyContractCode := AddrKeyCode(addr)
		keyContractLength := AddrKeyLength(addr)

		newContractCode, newContractLength, err := GetCodeAndLengthNoBig(change.new)
		if err != nil {
			return nil, err
		}

		oldContractCode, oldContractLength, err := GetCodeAndLengthNoBig(change.old)
		if err != nil {
			return nil, err
		}

		setType := ChangeSetEntryType_Change
		if newContractCode.IsZero() && oldContractCode.IsZero() {
			continue
		} else if oldContractCode.IsZero() && !newContractCode.IsZero() {
			setType = ChangeSetEntryType_Add
		} else if newContractCode.IsZero() {
			setType = ChangeSetEntryType_Delete
		}

		changeSet = append(changeSet, ChangeSetEntry{
			Type:          setType,
			Key:           keyContractCode,
			Value:         newContractCode,
			OriginalValue: oldContractCode,
		})

		setType = ChangeSetEntryType_Change
		if newContractLength.IsZero() && oldContractLength.IsZero() {
			continue
		} else if oldContractLength.IsZero() && !newContractLength.IsZero() {
			setType = ChangeSetEntryType_Add
		} else if newContractLength.IsZero() {
			setType = ChangeSetEntryType_Delete
		}
		changeSet = append(changeSet, ChangeSetEntry{
			Type:          setType,
			Key:           keyContractLength,
			Value:         newContractLength,
			OriginalValue: oldContractLength,
		})
	}

	for addr, change := range c.storageChanges {
		for key, value := range change {
			newVal, err := getStorageValue(value.new)
			if err != nil {
				return nil, err
			}

			oldVal, err := getStorageValue(value.old)
			if err != nil {
				return nil, err
			}

			paddedKey := fmt.Sprintf("0x%032x", key)
			storageKey := KeyContractStorageWithoutBig(addr, common.HexToHash(paddedKey))

			if oldVal.IsZero() && newVal.IsZero() {
				// no-op this case.  It means that a storage slot was set to something but within
				// the same changeset it was later set to zero, so there is nothing to do in the
				// tree here
				continue
			}

			if oldVal.IsZero() && !newVal.IsZero() {
				// nothing old so this must be an add
				changeSet = append(changeSet, ChangeSetEntry{
					Type:          ChangeSetEntryType_Add,
					Key:           storageKey,
					Value:         newVal,
					OriginalValue: oldVal,
				})
			} else if newVal.IsZero() {
				// nothing on the new side so this must be a delete
				changeSet = append(changeSet, ChangeSetEntry{
					Type:          ChangeSetEntryType_Delete,
					Key:           storageKey,
					OriginalValue: oldVal,
				})
			} else {
				// something on both sides so this must be a change
				changeSet = append(changeSet, ChangeSetEntry{
					Type:          ChangeSetEntryType_Change,
					Key:           storageKey,
					Value:         newVal,
					OriginalValue: oldVal,
				})
			}
		}
	}

	// sort the change set lexographically by key
	slices.SortFunc(changeSet, func(i, j ChangeSetEntry) int {
		return lexographicalCompareKeys(i.Key, j.Key)
	})

	return changeSet, nil
}

func mustGetCodeValue(code []byte) SmtValue8 {
	codeValue, _, err := GetCodeAndLengthNoBig(code)
	if err != nil {
		panic(err)
	}
	return codeValue
}

func mustGetContractLength(code []byte) SmtValue8 {
	_, length, err := GetCodeAndLengthNoBig(code)
	if err != nil {
		panic(err)
	}
	return length
}

func getStorageValue(value []byte) (SmtValue8, error) {
	return BytesToSmtValue8(value), nil
}

func mustGetStorageValue(value []byte) SmtValue8 {
	val, err := getStorageValue(value)
	if err != nil {
		panic(err)
	}
	return val
}

func convertBytecodeToBigInt(bytecode string) (*big.Int, int, error) {
	bi := utils.HashContractBytecodeBigInt(bytecode)
	parsedBytecode := strings.TrimPrefix(bytecode, "0x")

	if len(parsedBytecode)%2 != 0 {
		parsedBytecode = "0" + parsedBytecode
	}

	bytecodeLength := len(parsedBytecode) / 2

	if len(bytecode) == 0 {
		bytecodeLength = 0
		bi = big.NewInt(0)
	}

	return bi, bytecodeLength, nil
}
