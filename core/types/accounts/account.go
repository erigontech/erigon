package accounts

import (
	"github.com/ledgerwatch/turbo-geth/crypto"
	"math/big"

	"bytes"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type ExtAccount struct {
	Nonce   uint64
	Balance *big.Int
}

// Account is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
type Account struct {
	Nonce       uint64
	Balance     *big.Int
	Root        common.Hash // merkle root of the storage trie
	CodeHash    []byte
	StorageSize *uint64
}

type accountWithoutStorage struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
}

const ApproxSizeOfCompressedAccount = 60

var emptyCodeHash = crypto.Keccak256(nil)
var emptyCodeHashH = common.BytesToHash(emptyCodeHash)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

func (a *Account) Encode(enableStorageSize bool) ([]byte, error) {
	var data []byte
	var err error
	if (a.CodeHash == nil || a.IsEmptyCodeHash()) && (a.Root == emptyRoot || a.Root == common.Hash{}) {
		if (a.Balance == nil || a.Balance.Sign() == 0) && a.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			extAccount := new(ExtAccount).
				fill(a).
				setDefaultBalance()

			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return nil, err
			}
		}
	} else {
		acc := newAccountCopy(a)

		if !enableStorageSize || acc.StorageSize == nil {
			accBeforeEIP2027 := &accountWithoutStorage{
				Nonce:    acc.Nonce,
				Balance:  acc.Balance,
				Root:     acc.Root,
				CodeHash: acc.CodeHash,
			}

			data, err = rlp.EncodeToBytes(accBeforeEIP2027)
			if err != nil {
				return nil, err
			}
		} else {
			data, err = rlp.EncodeToBytes(a)
			if err != nil {
				return nil, err
			}
		}
	}
	return data, err
}

func (a *Account) EncodeRLP(enableStorageSize bool) ([]byte, error) {
	acc := newAccountCopy(a)
	if !enableStorageSize {
		accBeforeEIP2027 := &accountWithoutStorage{
			Nonce:    acc.Nonce,
			Balance:  acc.Balance,
			Root:     acc.Root,
			CodeHash: acc.CodeHash,
		}
		return rlp.EncodeToBytes(accBeforeEIP2027)
	}
	return rlp.EncodeToBytes(acc)
}

func (a *Account) Decode(enc []byte) error {
	if len(enc) == 0 {
		return nil
	}

	// Kind of hacky
	if len(enc) == 1 {
		a.Balance = new(big.Int)
		a.setCodeHash(emptyCodeHash)
		a.Root = emptyRoot
	} else if len(enc) < ApproxSizeOfCompressedAccount {
		var extData ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			return err
		}

		a.fillFromExtAccount(extData)
	} else {
		dataWithoutStorage := &accountWithoutStorage{}
		if err := rlp.DecodeBytes(enc, dataWithoutStorage); err != nil {
			if err.Error() != "rlp: input list has too many elements for accounts.accountWithoutStorage" {
				return err
			}

			dataWithStorage := &Account{}
			if err := rlp.DecodeBytes(enc, &dataWithStorage); err != nil {
				return err
			}

			a.fill(dataWithStorage)
		} else {
			a.fillAccountWithoutStorage(dataWithoutStorage)
		}
	}

	return nil

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

	if a.Balance == nil {
		a.Balance = new(big.Int)
	}
	a.Balance.Set(srcAccount.Balance)

	a.Nonce = srcAccount.Nonce

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

	if a.Balance == nil {
		a.Balance = new(big.Int)
	}
	a.Balance.Set(srcAccount.Balance)

	a.Nonce = srcAccount.Nonce

	a.StorageSize = nil

	return a
}

func (a *Account) fillFromExtAccount(srcExtAccount ExtAccount) *Account {
	a.Nonce = srcExtAccount.Nonce

	if a.Balance == nil {
		a.Balance = new(big.Int)
	}
	a.Balance.Set(srcExtAccount.Balance)

	a.CodeHash = emptyCodeHash

	a.Root = emptyRoot

	return a
}

func (a *Account) setDefaultBalance() *Account {
	if a.Balance == nil {
		a.Balance = new(big.Int)
	}

	return a
}

func (a *Account) setDefaultCodeHash() *Account {
	if a.CodeHash == nil {
		a.CodeHash = emptyCodeHash
	}

	return a
}

func (a *Account) setDefaultRoot() *Account {
	if a.Root == (common.Hash{}) {
		a.Root = emptyRoot
	}

	return a
}

func (a *Account) IsEmptyCodeHash() bool {
	return bytes.Equal(a.CodeHash[:], emptyCodeHash)
}

func (extAcc *ExtAccount) fill(srcAccount *Account) *ExtAccount {
	if extAcc.Balance == nil {
		extAcc.Balance = new(big.Int)
	}

	extAcc.Balance.Set(srcAccount.Balance)

	extAcc.Nonce = srcAccount.Nonce

	return extAcc
}

func (extAcc *ExtAccount) setDefaultBalance() *ExtAccount {
	if extAcc.Balance == nil {
		extAcc.Balance = new(big.Int)
	}

	return extAcc
}
