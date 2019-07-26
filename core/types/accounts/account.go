package accounts

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type ExtAccount struct {
	Nonce   uint64
	Balance *big.Int
	Incarnation uint8
}

// Account is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
// DESCRIBED: docs/programmers_guide/guide.md#ethereum-state
type Account struct {
	Nonce       uint64
	Balance     *big.Int
	Root        common.Hash // merkle root of the storage trie
	CodeHash    []byte
	Incarnation		uint8
	StorageSize *uint64
}

type accountWithoutStorage struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
	Incarnation	 uint8
}
type RLPAccount struct {
	Nonce       uint64
	Balance     *big.Int
	Root        common.Hash // merkle root of the storage trie
	CodeHash    []byte
	StorageSize *uint64
}
type RLPAccountWithoutStorage struct {
	Nonce       uint64
	Balance     *big.Int
	Root        common.Hash // merkle root of the storage trie
	CodeHash    []byte
}


const (
	accountSizeWithoutData            = 1
	minAccountSizeWithRootAndCodeHash = 60
)

var emptyCodeHash = crypto.Keccak256(nil)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

func (a *Account) Encode(ctx context.Context) ([]byte, error) {
	var toEncode interface{}

	fmt.Println("core/types/accounts/account.go:51 encode version", a.GetIncarnation())
	if a.IsEmptyCodeHash() && a.IsEmptyRoot() {
		if (a.Balance == nil || a.Balance.Sign() == 0) && a.Nonce == 0 {
			fmt.Println("Encode OneByte")
			return []byte{byte(192)}, nil
		}

		toEncode = new(ExtAccount).
			fill(a).
			setDefaultBalance()
		fmt.Println("Encode extAccount")
	} else {
		acc := newAccountCopy(a)
		toEncode = acc

		if acc.StorageSize != nil {
			fmt.Println("Encode with storage size")
			return rlp.EncodeToBytes(toEncode)
		}

		if !params.GetForkFlag(ctx, params.IsEIP2027Enabled) {
			fmt.Println("Encode accountWithoutStorage")
			toEncode = &accountWithoutStorage{
				Nonce:    acc.Nonce,
				Balance:  acc.Balance,
				Root:     acc.Root,
				CodeHash: acc.CodeHash,
				Incarnation:  acc.Incarnation,
			}
		}
	}

	enc,err:=rlp.EncodeToBytes(toEncode)
	fmt.Println("core/types/accounts/account.go:99 Encode", enc)
	return enc,err
}

func (a *Account) EncodeRLP(ctx context.Context) ([]byte, error) {
	acc := newAccountCopy(a)
	toEncode := interface{}(RLPAccount{
		Nonce:acc.Nonce,
		Balance:acc.Balance,
		Root:acc.Root,
		CodeHash:acc.CodeHash,
	})

	if acc.StorageSize != nil {
		return rlp.EncodeToBytes(toEncode)
	}

	if acc.StorageSize == nil || !params.GetForkFlag(ctx, params.IsEIP2027Enabled) {
		toEncode = &RLPAccountWithoutStorage{
			Nonce:    acc.Nonce,
			Balance:  acc.Balance,
			Root:     acc.Root,
			CodeHash: acc.CodeHash,
		}
	}

	enc,err:=rlp.EncodeToBytes(toEncode)
	fmt.Println("core/types/accounts/account.go:124 EncodeRLP", enc)
	return enc, err
}

func (a *Account) Decode(enc []byte) error {
	fmt.Println("core/types/accounts/account.go:127 Decode len", len(enc))
	fmt.Println("enc - ", enc)
	switch encodedLength := len(enc); {
	case encodedLength == 0:

	case encodedLength == accountSizeWithoutData:
		fmt.Println("encodedLength == accountSizeWithoutData")
		a.Balance = new(big.Int)
		a.setCodeHash(emptyCodeHash)
		a.Root = emptyRoot

	case encodedLength < minAccountSizeWithRootAndCodeHash:
		fmt.Println("encodedLength < minAccountSizeWithRootAndCodeHash", len(enc))

		var extData ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			return err
		}

		a.fillFromExtAccount(extData)
	default:
		fmt.Println("default")
		dataWithoutStorage := &accountWithoutStorage{}
		err := rlp.DecodeBytes(enc, dataWithoutStorage)
		if err == nil {
			a.fillAccountWithoutStorage(dataWithoutStorage)
			return nil
		}

		if err.Error() != "rlp: input list has too many elements for accounts.accountWithoutStorage" {
			return err
		}
		fmt.Println("default to Account")
		dataWithStorage := &Account{}
		if err := rlp.DecodeBytes(enc, &dataWithStorage); err != nil {
			return err
		}

		a.fill(dataWithStorage)
	}

	// We dont want to broke old state, so keep StorageSize nil if not set
	if a.StorageSize != nil && *a.StorageSize == 0 {
		a.StorageSize = nil
	}

	return nil
}

func (a *Account) DecodeRLP(enc []byte) error {
	dataWithoutStorage := &RLPAccountWithoutStorage{}
	err := rlp.DecodeBytes(enc, dataWithoutStorage)
	if err == nil {
		a.Root = dataWithoutStorage.Root
		a.CodeHash = dataWithoutStorage.CodeHash
		a.Balance = dataWithoutStorage.Balance
		a.Nonce = dataWithoutStorage.Nonce

		return nil
	}

	if err.Error() != "rlp: input list has too many elements for accounts.accountWithoutStorage" {
		return err
	}
	fmt.Println("default to Account")
	dataWithStorage := &RLPAccount{}
	if err := rlp.DecodeBytes(enc, &dataWithStorage); err != nil {
		return err
	}
	a.Root = dataWithStorage.Root
	a.CodeHash = dataWithStorage.CodeHash
	a.Balance = dataWithStorage.Balance
	a.Nonce = dataWithStorage.Nonce
	a.StorageSize=dataWithStorage.StorageSize
	return nil
}
func DecodeRLP(enc []byte) (*Account, error) {
	if len(enc) == 0 {
		return nil, nil
	}

	acc := new(Account)
	err := acc.DecodeRLP(enc)
	return acc, err
}



func Decode(enc []byte) (*Account, error) {
	if len(enc) == 0 {
		return nil, nil
	}

	acc := new(Account)
	err := acc.Decode(enc)
	return acc, err
}

func newAccountCopy(srcAccount *Account) *Account {
	return new(Account).
		fill(srcAccount).
		setDefaultBalance().
		setDefaultCodeHash().
		setDefaultRoot()
}

func (a *Account) fill(srcAccount *Account) *Account {
	a.Root = srcAccount.Root

	a.CodeHash = make([]byte, len(srcAccount.CodeHash))
	copy(a.CodeHash, srcAccount.CodeHash)

	a.setDefaultBalance()
	a.Balance.Set(srcAccount.Balance)

	a.Nonce = srcAccount.Nonce
	a.Incarnation = srcAccount.Incarnation

	if srcAccount.StorageSize != nil {
		a.StorageSize = new(uint64)
		*a.StorageSize = *srcAccount.StorageSize
	}

	return a
}

func (a *Account) setCodeHash(codeHash []byte) {
	a.CodeHash = make([]byte, len(codeHash))
	copy(a.CodeHash, codeHash)
}

func (a *Account) fillAccountWithoutStorage(srcAccount *accountWithoutStorage) *Account {
	a.Root = srcAccount.Root

	a.setCodeHash(srcAccount.CodeHash)

	a.setDefaultBalance()
	a.Balance.Set(srcAccount.Balance)

	a.Nonce = srcAccount.Nonce
	a.Incarnation=srcAccount.Incarnation

	a.StorageSize = nil

	return a
}

func (a *Account) fillFromExtAccount(srcExtAccount ExtAccount) *Account {
	a.Nonce = srcExtAccount.Nonce

	a.setDefaultBalance()
	a.Balance.Set(srcExtAccount.Balance)

	a.CodeHash = emptyCodeHash

	a.Root = emptyRoot
	a.Incarnation=srcExtAccount.Incarnation

	return a
}

func (a *Account) setDefaultBalance() *Account {
	if a.Balance == nil {
		a.Balance = new(big.Int)
	}

	return a
}

func (a *Account) setDefaultCodeHash() *Account {
	if a.IsEmptyCodeHash() {
		a.CodeHash = emptyCodeHash
	}

	return a
}

func (a *Account) setDefaultRoot() *Account {
	if a.IsEmptyRoot() {
		a.Root = emptyRoot
	}

	return a
}

func (a *Account) GetIncarnation() uint8  {
	return a.Incarnation
}
func (a *Account) SetIncarnation(v uint8)  {
	a.Incarnation = v
}
func (a *Account) IsEmptyCodeHash() bool {
	return a.CodeHash == nil || bytes.Equal(a.CodeHash[:], emptyCodeHash)
}

func (a *Account) IsEmptyRoot() bool {
	return a.Root == emptyRoot || a.Root == common.Hash{}
}

func (extAcc *ExtAccount) fill(srcAccount *Account) *ExtAccount {
	extAcc.setDefaultBalance()
	extAcc.Balance.Set(srcAccount.Balance)

	extAcc.Nonce = srcAccount.Nonce
	extAcc.Incarnation = srcAccount.Incarnation

	return extAcc
}

func (extAcc *ExtAccount) setDefaultBalance() *ExtAccount {
	if extAcc.Balance == nil {
		extAcc.Balance = new(big.Int)
	}

	return extAcc
}
