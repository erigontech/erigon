package smtv2

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon/core/types/accounts"
)

type accountChanges struct {
	address common.Address
	account *accounts.Account
	code    []byte
	storage map[string][]byte
}

type AccountChangeCollector struct {
	etl         *etl.Collector
	workerChan  chan accountChanges
	workerCount int
	wg          *sync.WaitGroup
	err         error
	ctx         context.Context
	cancel      context.CancelFunc
	mtx         sync.Mutex
	collected   int
}

func NewAccountChangeCollector(etl *etl.Collector) *AccountChangeCollector {
	workerChan := make(chan accountChanges)
	wg := &sync.WaitGroup{}
	workerCount := runtime.NumCPU()
	ctx, cancel := context.WithCancel(context.Background())

	result := &AccountChangeCollector{
		etl:         etl,
		workerChan:  workerChan,
		workerCount: workerCount,
		wg:          wg,
		ctx:         ctx,
		cancel:      cancel,
		mtx:         sync.Mutex{},
	}

	result.startWorkers()

	return result
}

func (c *AccountChangeCollector) startWorkers() {
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *AccountChangeCollector) worker() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case changes, ok := <-c.workerChan:
			if !ok {
				return
			}
			if err := c.processAccountChanges(changes); err != nil {
				c.err = err
				c.cancel()
			}
		}
	}
}

func (c *AccountChangeCollector) SignalNoMoreChanges() {
	close(c.workerChan)
}

func (c *AccountChangeCollector) Wait() error {
	c.wg.Wait()
	return c.err
}

func (c *AccountChangeCollector) ProcessAccount(address common.Address, account *accounts.Account, code []byte, storage map[string][]byte) error {
	select {
	case <-c.ctx.Done():
		return fmt.Errorf("context cancelled due to error: %w", c.err)
	case c.workerChan <- accountChanges{address: address, account: account, code: code, storage: storage}:
		return nil
	}
}

func (c *AccountChangeCollector) processAccountChanges(changes accountChanges) error {
	keyBalance := AddrKeyBalance(changes.address)
	keyNonce := AddrKeyNonce(changes.address)

	balance := ScalarToSmtValue8FromBytes(changes.account.Balance.ToBig())
	nonce := ScalarToSmtValue8FromBytes(big.NewInt(int64(changes.account.Nonce)))

	if !balance.IsZero() {
		c.collectAccountValue(keyBalance, balance)
	}

	if !nonce.IsZero() {
		c.collectAccountValue(keyNonce, nonce)
	}

	codeValue, lengthValue, err := GetCodeAndLengthNoBig(changes.code)
	if err != nil {
		return err
	}

	if !codeValue.IsZero() {
		keyContractCode := AddrKeyCode(changes.address)
		c.collectAccountValue(keyContractCode, codeValue)
		keyContractLength := AddrKeyLength(changes.address)
		c.collectAccountValue(keyContractLength, lengthValue)
	}

	for k, v := range changes.storage {
		storageVal, err := getStorageValue(v)
		if err != nil {
			return err
		}

		storageKey := KeyContractStorageWithoutBig(changes.address, common.HexToHash(k))

		c.collectAccountValue(storageKey, storageVal)
	}

	return nil
}

func (c *AccountChangeCollector) collectAccountValue(key SmtKey, value SmtValue8) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.collected++

	path := key.GetPath()
	pathBytes := make([]byte, len(path))
	for i, v := range path {
		pathBytes[i] = byte(v)
	}

	// convert value to bytes
	valueBytes := value.ToBytes()

	c.etl.Collect(pathBytes, valueBytes)
}

func (c *AccountChangeCollector) TotalCollected() int {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.collected
}
