package smt

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/dgravesa/go-parallel/parallel"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

func (s *SMT) SetAccountState(ethAddr string, balance, nonce *big.Int) (*big.Int, error) {
	keyBalance := utils.KeyEthAddrBalance(ethAddr)
	keyNonce := utils.KeyEthAddrNonce(ethAddr)

	if _, err := s.InsertKA(keyBalance, balance); err != nil {
		return nil, err
	}

	ks := utils.EncodeKeySource(utils.KEY_BALANCE, utils.ConvertHexToAddress(ethAddr), common.Hash{})
	if err := s.Db.InsertKeySource(keyBalance, ks); err != nil {
		return nil, err
	}

	auxRes, err := s.InsertKA(keyNonce, nonce)
	if err != nil {
		return nil, err
	}

	ks = utils.EncodeKeySource(utils.KEY_NONCE, utils.ConvertHexToAddress(ethAddr), common.Hash{})
	if err := s.Db.InsertKeySource(keyNonce, ks); err != nil {
		return nil, err
	}

	return auxRes.NewRootScalar.ToBigInt(), nil
}

func (s *SMT) SetAccountStorage(addr libcommon.Address, acc *accounts.Account) error {
	if acc != nil {
		n := new(big.Int).SetUint64(acc.Nonce)
		_, err := s.SetAccountState(addr.String(), acc.Balance.ToBig(), n)
		return err
	}

	_, err := s.SetAccountState(addr.String(), big.NewInt(0), big.NewInt(0))
	return err
}

func (s *SMT) SetContractBytecode(ethAddr string, bytecode string) error {
	keyContractCode := utils.KeyContractCode(ethAddr)
	keyContractLength := utils.KeyContractLength(ethAddr)

	bi, bytecodeLength, err := convertBytecodeToBigInt(bytecode)
	if err != nil {
		return err
	}

	_, err = s.InsertKA(keyContractCode, bi)
	if err != nil {
		return err
	}

	ks := utils.EncodeKeySource(utils.SC_CODE, utils.ConvertHexToAddress(ethAddr), common.Hash{})

	err = s.Db.InsertKeySource(keyContractCode, ks)

	if err != nil {
		return err
	}

	_, err = s.InsertKA(keyContractLength, big.NewInt(int64(bytecodeLength)))
	if err != nil {
		return err
	}

	ks = utils.EncodeKeySource(utils.SC_LENGTH, utils.ConvertHexToAddress(ethAddr), common.Hash{})

	err = s.Db.InsertKeySource(keyContractLength, ks)

	if err != nil {
		return err
	}

	return err
}

func (s *SMT) SetContractStorage(ethAddr string, storage map[string]string, progressChan chan uint64) (*big.Int, error) {
	storageKeys := make([]string, len(storage))
	ii := 0
	for k := range storage {
		storageKeys[ii] = k
		ii++
	}

	chm := make(map[string]*utils.NodeValue8)
	vhm := make(map[string][4]uint64)
	storageKeyCount := len(storageKeys)

	//no need to parallelize too low amount of computations
	if len(storage) > 100 {
		cpuNum := parallel.DefaultNumGoroutines()

		keyArray := make([][]string, cpuNum)
		cVArray := make([][]*utils.NodeValue8, cpuNum)
		hashArray := make([][][4]uint64, cpuNum)

		operationsPerCpu := storageKeyCount/cpuNum + storageKeyCount%cpuNum
		for i := 0; i < len(hashArray); i++ {
			keyArray[i] = make([]string, operationsPerCpu)
			cVArray[i] = make([]*utils.NodeValue8, operationsPerCpu)
			hashArray[i] = make([][4]uint64, operationsPerCpu)
		}

		var wg sync.WaitGroup
		wg.Add(cpuNum)

		var err error
		for i := 0; i < cpuNum; i++ {
			go func(cpuI int) {
				defer wg.Done()
				count := 0
				for j := cpuI; j < storageKeyCount; j += cpuNum {
					k := storageKeys[j]
					v := storage[k]
					if v == "" {
						continue
					}

					c, h, e := calcHashVal(v)
					if e != nil {
						err = e
						return
					}
					keyArray[cpuI][count] = k
					cVArray[cpuI][count] = c
					hashArray[cpuI][count] = h
					count++
				}
			}(i)
		}
		wg.Wait()

		if err != nil {
			return nil, err
		}

		for i := 0; i < len(keyArray); i++ {
			for j := 0; j < len(keyArray[i]); j++ {
				k := keyArray[i][j]
				if k == "" {
					continue
				}

				c := cVArray[i][j]
				h := hashArray[i][j]
				chm[k] = c
				vhm[k] = h
			}
		}

	} else {
		for _, k := range storageKeys {
			v := storage[k]
			if v == "" {
				continue
			}

			c, h, e := calcHashVal(v)
			if e != nil {
				return nil, e
			}
			chm[k] = c
			vhm[k] = h
		}
	}

	auxRes, err := s.InsertStorage(ethAddr, &storage, &chm, &vhm, progressChan)
	if err != nil {
		return nil, err
	}

	return auxRes.NewRootScalar.ToBigInt(), nil
}

func (s *SMT) SetStorage(ctx context.Context, logPrefix string, accChanges map[libcommon.Address]*accounts.Account, codeChanges map[libcommon.Address]string, storageChanges map[libcommon.Address]map[string]string) ([]*utils.NodeKey, []*utils.NodeValue8, error) {
	var isDelete bool
	var err error

	storageChangesInitialCapacity := 0
	for _, storage := range storageChanges {
		storageChangesInitialCapacity += len(storage)
	}

	initialCapacity := len(accChanges)*2 + len(codeChanges)*2 + storageChangesInitialCapacity
	keysBatchStorage := make([]*utils.NodeKey, 0, initialCapacity)
	valuesBatchStorage := make([]*utils.NodeValue8, 0, initialCapacity)

	for addr, acc := range accChanges {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf(fmt.Sprintf("[%s] Context done", logPrefix))
		default:
		}
		ethAddr := addr.String()
		keyBalance := utils.KeyEthAddrBalance(ethAddr)
		keyNonce := utils.KeyEthAddrNonce(ethAddr)

		balance := big.NewInt(0)
		nonce := big.NewInt(0)
		if acc != nil {
			balance = acc.Balance.ToBig()
			nonce = new(big.Int).SetUint64(acc.Nonce)
		}

		keysBatchStorage = append(keysBatchStorage, &keyBalance)
		if valuesBatchStorage, isDelete, err = appendToValuesBatchStorageBigInt(valuesBatchStorage, balance); err != nil {
			return nil, nil, err
		}
		if !isDelete {
			if err = s.InsertKeySource(&keyBalance, utils.KEY_BALANCE, &addr, &common.Hash{}); err != nil {
				return nil, nil, err
			}
		} else {
			if err = s.DeleteKeySource(&keyBalance); err != nil {
				return nil, nil, err
			}

		}

		keysBatchStorage = append(keysBatchStorage, &keyNonce)
		if valuesBatchStorage, isDelete, err = appendToValuesBatchStorageBigInt(valuesBatchStorage, nonce); err != nil {
			return nil, nil, err
		}
		if !isDelete {
			if err = s.InsertKeySource(&keyNonce, utils.KEY_NONCE, &addr, &common.Hash{}); err != nil {
				return nil, nil, err
			}
		} else {
			if err = s.DeleteKeySource(&keyNonce); err != nil {
				return nil, nil, err
			}
		}
	}

	for addr, code := range codeChanges {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf(fmt.Sprintf("[%s] Context done", logPrefix))
		default:
		}

		ethAddr := addr.String()
		keyContractCode := utils.KeyContractCode(ethAddr)
		keyContractLength := utils.KeyContractLength(ethAddr)

		bi, bytecodeLength, err := convertBytecodeToBigInt(code)
		if err != nil {
			return nil, nil, err
		}

		keysBatchStorage = append(keysBatchStorage, &keyContractCode)
		if valuesBatchStorage, isDelete, err = appendToValuesBatchStorageBigInt(valuesBatchStorage, bi); err != nil {
			return nil, nil, err
		}
		if !isDelete {
			if err = s.InsertKeySource(&keyContractCode, utils.SC_CODE, &addr, &common.Hash{}); err != nil {
				return nil, nil, err
			}
		} else {
			if err = s.DeleteKeySource(&keyContractCode); err != nil {
				return nil, nil, err
			}
		}

		keysBatchStorage = append(keysBatchStorage, &keyContractLength)
		if valuesBatchStorage, isDelete, err = appendToValuesBatchStorageBigInt(valuesBatchStorage, big.NewInt(int64(bytecodeLength))); err != nil {
			return nil, nil, err
		}
		if !isDelete {
			if err = s.InsertKeySource(&keyContractLength, utils.SC_LENGTH, &addr, &common.Hash{}); err != nil {
				return nil, nil, err
			}
		} else {
			if err = s.DeleteKeySource(&keyContractLength); err != nil {
				return nil, nil, err
			}
		}
	}

	for addr, storage := range storageChanges {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf(fmt.Sprintf("[%s] Context done", logPrefix))
		default:
		}
		ethAddr := addr.String()
		ethAddrBigInt := utils.ConvertHexToBigInt(ethAddr)
		ethAddrBigIngArray := utils.ScalarToArrayBig(ethAddrBigInt)

		for k, v := range storage {
			keyStoragePosition := utils.KeyContractStorage(ethAddrBigIngArray, k)
			valueBigInt := convertStrintToBigInt(v)
			keysBatchStorage = append(keysBatchStorage, &keyStoragePosition)
			if valuesBatchStorage, isDelete, err = appendToValuesBatchStorageBigInt(valuesBatchStorage, valueBigInt); err != nil {
				return nil, nil, err
			}
			if !isDelete {
				sp, _ := utils.StrValToBigInt(k)
				hash := common.BigToHash(sp)
				if err = s.InsertKeySource(&keyStoragePosition, utils.SC_STORAGE, &addr, &hash); err != nil {
					return nil, nil, err
				}
			} else {
				if err = s.DeleteKeySource(&keyStoragePosition); err != nil {
					return nil, nil, err
				}
			}
		}
	}

	insertBatchCfg := NewInsertBatchConfig(ctx, logPrefix, true)
	if _, err = s.InsertBatch(insertBatchCfg, keysBatchStorage, valuesBatchStorage, nil, nil); err != nil {
		return nil, nil, err
	}

	return keysBatchStorage, valuesBatchStorage, nil
}

func (s *SMT) InsertKeySource(nodeKey *utils.NodeKey, key int, accountAddr *libcommon.Address, storagePosition *libcommon.Hash) error {
	ks := utils.EncodeKeySource(key, *accountAddr, *storagePosition)
	return s.Db.InsertKeySource(*nodeKey, ks)
}

func (s *SMT) DeleteKeySource(nodeKey *utils.NodeKey) error {
	return s.Db.DeleteKeySource(*nodeKey)
}

func calcHashVal(v string) (*utils.NodeValue8, [4]uint64, error) {
	val := convertStrintToBigInt(v)

	x := utils.ScalarToArrayBig(val)
	value, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, [4]uint64{}, err
	}

	h := utils.Hash(value.ToUintArray(), utils.BranchCapacity)

	return value, h, nil
}

func convertStrintToBigInt(v string) *big.Int {
	base := 10
	if strings.HasPrefix(v, "0x") {
		v = v[2:]
		base = 16
	}

	val, _ := new(big.Int).SetString(v, base)
	return val
}

func appendToValuesBatchStorageBigInt(valuesBatchStorage []*utils.NodeValue8, value *big.Int) ([]*utils.NodeValue8, bool, error) {
	nodeValue, err := utils.NodeValue8FromBigInt(value)
	if err != nil {
		return nil, false, err
	}
	return append(valuesBatchStorage, nodeValue), nodeValue.IsZero(), nil
}

func convertBytecodeToBigInt(bytecode string) (*big.Int, int, error) {
	var parsedBytecode string
	hashedBytecode := utils.HashContractBytecode(bytecode)

	if strings.HasPrefix(bytecode, "0x") {
		parsedBytecode = bytecode[2:]
	} else {
		parsedBytecode = bytecode
	}

	if len(parsedBytecode)%2 != 0 {
		parsedBytecode = "0" + parsedBytecode
	}

	bytecodeLength := len(parsedBytecode) / 2

	bi := utils.ConvertHexToBigInt(hashedBytecode)

	if len(bytecode) == 0 {
		bytecodeLength = 0
		bi = big.NewInt(0)
	}

	return bi, bytecodeLength, nil
}
