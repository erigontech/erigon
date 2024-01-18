package smt

import (
	"math/big"
	"strings"
	"sync"

	"github.com/dgravesa/go-parallel/parallel"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

func (s *SMT) SetAccountState(ethAddr string, balance, nonce *big.Int) (*big.Int, error) {
	keyBalance, err := utils.KeyEthAddrBalance(ethAddr)
	if err != nil {
		return nil, err
	}
	keyNonce, err := utils.KeyEthAddrNonce(ethAddr)
	if err != nil {
		return nil, err
	}

	_, err = s.InsertKA(keyBalance, balance)
	if err != nil {
		return nil, err
	}

	ks := utils.EncodeKeySource(utils.KEY_BALANCE, utils.ConvertHexToAddress(ethAddr), common.Hash{})
	err = s.Db.InsertKeySource(keyBalance, ks)
	if err != nil {
		return nil, err
	}

	auxRes, err := s.InsertKA(keyNonce, nonce)

	if err != nil {
		return nil, err
	}

	ks = utils.EncodeKeySource(utils.KEY_NONCE, utils.ConvertHexToAddress(ethAddr), common.Hash{})
	err = s.Db.InsertKeySource(keyNonce, ks)
	if err != nil {
		return nil, err
	}

	return auxRes.NewRootScalar.ToBigInt(), err
}

func (s *SMT) SetContractBytecode(ethAddr string, bytecode string) error {
	keyContractCode, err := utils.KeyContractCode(ethAddr)
	if err != nil {
		return err
	}
	keyContractLength, err := utils.KeyContractLength(ethAddr)
	if err != nil {
		return err
	}

	hashedBytecode, err := utils.HashContractBytecode(bytecode)
	if err != nil {
		return err
	}

	var parsedBytecode string

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

func (s *SMT) SetContractStorage(ethAddr string, storage map[string]string) (*big.Int, error) {
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

	auxRes, err := s.InsertStorage(ethAddr, &storage, &chm, &vhm)
	if err != nil {
		return nil, err
	}

	return auxRes.NewRootScalar.ToBigInt(), nil
}

func calcHashVal(v string) (*utils.NodeValue8, [4]uint64, error) {
	base := 10
	if strings.HasPrefix(v, "0x") {
		v = v[2:]
		base = 16
	}

	val, _ := new(big.Int).SetString(v, base)

	x := utils.ScalarToArrayBig(val)
	value, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, [4]uint64{}, err
	}

	h, err := utils.Hash(value.ToUintArray(), utils.BranchCapacity)
	if err != nil {
		return nil, [4]uint64{}, err
	}

	return value, h, nil
}
