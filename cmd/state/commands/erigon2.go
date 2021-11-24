package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/holiman/uint256"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(erigon2Cmd)
	withDatadir(erigon2Cmd)
	rootCmd.AddCommand(erigon2Cmd)
}

var erigon2Cmd = &cobra.Command{
	Use:   "erigon2",
	Short: "Exerimental command to re-execute blocks from beginning using erigon2 state representation",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Erigon2(genesis, logger, block, datadir)
	},
}

func Erigon2(genesis *core.Genesis, logger log.Logger, blockNum uint64, datadir string) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	historyDb, err := kv2.NewMDBX(logger).Path(path.Join(datadir, "chaindata")).Open()
	if err != nil {
		return err
	}
	historyTx, err1 := historyDb.BeginRo(context.Background())
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	//chainConfig := genesis.Config
	//vmConfig := vm.Config{}

	//noOpWriter := state.NewNoopWriter()
	interrupt := false
	block := uint64(0)
	for !interrupt {
		block++
		if block >= blockNum {
			break
		}
		blockHash, err := rawdb.ReadCanonicalHash(historyTx, blockNum)
		if err != nil {
			return err
		}
		var b *types.Block
		b, _, err = rawdb.ReadBlockWithSenders(historyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		if block%1000 == 0 {
			log.Info("Processed", "blocks", block)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}

	}
	return nil
}

// Implements StateReader and StateWriter
type RW struct {
}

func (rw *RW) ReadAccountData(address common.Address) (*accounts.Account, error) {
	return nil, nil
}

func (rw *RW) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	return nil, nil
}

func (rw *RW) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	return nil, nil
}

func (rw *RW) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return 0, nil
}

func (rw *RW) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (rw *RW) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	return nil
}

func (rw *RW) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (rw *RW) DeleteAccount(address common.Address, original *accounts.Account) error {
	return nil
}

func (rw *RW) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	return nil
}

func (rw *RW) CreateContract(address common.Address) error {
	return nil
}
