package accounts

import (
	"github.com/ledgerwatch/turbo-geth/crypto"
	"math/big"

	"bytes"
	"fmt"
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
	Nonce       uint64
	Balance     *big.Int
	Root        common.Hash // merkle root of the storage trie
	CodeHash    []byte
}

var emptyCodeHash = crypto.Keccak256(nil)
var emptyCodeHashH = common.BytesToHash(emptyCodeHash)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

func (a *Account) Encode(enableStorageSize bool) ([]byte, error) {
	var data []byte
	var err error
	if (a.CodeHash == nil || bytes.Equal(a.CodeHash, emptyCodeHash)) && (a.Root == emptyRoot || a.Root == common.Hash{}) {
		if (a.Balance == nil || a.Balance.Sign() == 0) && a.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount ExtAccount
			extAccount.Nonce = a.Nonce
			extAccount.Balance = a.Balance
			if extAccount.Balance == nil {
				extAccount.Balance = new(big.Int)
			}
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return nil, err
			}
		}
	} else {
		acc := *a
		if acc.Balance == nil {
			acc.Balance = new(big.Int)
		}
		if acc.CodeHash == nil {
			acc.CodeHash = emptyCodeHash
		}
		if acc.Root == (common.Hash{}) {
			acc.Root = emptyRoot
		}

		if enableStorageSize || acc.StorageSize == nil {
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

			data, _ = rlp.EncodeToBytes(a)
		} else {
			data, err = rlp.EncodeToBytes(a)
			if err != nil {
				return nil, err
			}
		}
	}
	return data, err

}

func (a *Account) Decode(enc []byte) error {
	if enc == nil || len(enc) == 0 {
		//fmt.Println("--- 1")
		return nil
	}

	// Kind of hacky
	fmt.Println("--- 5", len(enc))
	if len(enc) == 1 {
		a.Balance = new(big.Int)
		a.CodeHash = emptyCodeHash
		a.Root = emptyRoot
	} else if len(enc) < 60 {
		//fixme возможно размер после добавления поля изменился. откуда взялась константа 60?
		var extData ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			fmt.Println("--- 6", err)
			return err
		}
		a.Nonce = extData.Nonce
		a.Balance = extData.Balance
		a.CodeHash = emptyCodeHash
		a.Root = emptyRoot
	} else {
		var dataWithoutStorage Account
		if err := rlp.DecodeBytes(enc, &dataWithoutStorage); err != nil {
			if err.Error() != "rlp: input list has too many elements for state.Account" {
				fmt.Println("--- 7", err)
				return err
			}

			var dataWithStorage Account
			if err := rlp.DecodeBytes(enc, &dataWithStorage); err != nil {
				fmt.Println("--- 8", err)
				return err
			}

			*a = dataWithStorage
		} else {
			a.Nonce = dataWithoutStorage.Nonce
			a.Balance = dataWithoutStorage.Balance
			a.CodeHash = dataWithoutStorage.CodeHash
			a.Root = dataWithoutStorage.Root
		}
	}

	fmt.Println("--- 9", a)
	return nil

}

//
//func encodingToAccount(enc []byte) (*accounts.Account, error) {
//	if enc == nil || len(enc) == 0 {
//		//fmt.Println("--- 1")
//		return nil, nil
//	}
//	var data accounts.Account
//	// Kind of hacky
//	fmt.Println("--- 5", len(enc))
//	if len(enc) == 1 {
//		data.Balance = new(big.Int)
//		data.CodeHash = emptyCodeHash
//		data.Root = emptyRoot
//	} else if len(enc) < 60 {
//		//fixme возможно размер после добавления поля изменился. откуда взялась константа 60?
//		var extData accounts.ExtAccount
//		if err := rlp.DecodeBytes(enc, &extData); err != nil {
//			fmt.Println("--- 6", err)
//			return nil, err
//		}
//		data.Nonce = extData.Nonce
//		data.Balance = extData.Balance
//		data.CodeHash = emptyCodeHash
//		data.Root = emptyRoot
//	} else {
//		var dataWithoutStorage Account
//		if err := rlp.DecodeBytes(enc, &dataWithoutStorage); err != nil {
//			if err.Error() != "rlp: input list has too many elements for state.Account" {
//				fmt.Println("--- 7", err)
//				return nil, err
//			}
//
//			var dataWithStorage accounts.Account
//			if err := rlp.DecodeBytes(enc, &dataWithStorage); err != nil {
//				fmt.Println("--- 8", err)
//				return nil, err
//			}
//
//			data = dataWithStorage
//		} else {
//			data.Nonce = dataWithoutStorage.Nonce
//			data.Balance = dataWithoutStorage.Balance
//			data.CodeHash = dataWithoutStorage.CodeHash
//			data.Root = dataWithoutStorage.Root
//		}
//	}
//
//	fmt.Println("--- 9", data)
//	return &data, nil
//}

//state.go
//func encodingToAccount(enc []byte) (*accounts.Account, error) {
//	if enc == nil || len(enc) == 0 {
//		return nil, nil
//	}
//	var data accounts.Account
//	// Kind of hacky
//	if len(enc) == 1 {
//		data.Balance = new(big.Int)
//		data.CodeHash = emptyCodeHash
//		data.Root = emptyRoot
//	} else if len(enc) < 60 {
//		var extData accounts.ExtAccount
//		if err := rlp.DecodeBytes(enc, &extData); err != nil {
//			return nil, err
//		}
//		data.Nonce = extData.Nonce
//		data.Balance = extData.Balance
//		data.CodeHash = emptyCodeHash
//		data.Root = emptyRoot
//	} else {
//		if err := rlp.DecodeBytes(enc, &data); err != nil {
//			return nil, err
//		}
//	}
//	return &data, nil
//}
