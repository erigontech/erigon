package services

import (
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

const gasPrice = 912345678

var (
	signer = types.LatestSigner(params.AllCliqueProtocolChanges)
)

// CreateTransaction creates a transaction depending on the type of transaction being passed
func CreateTransaction(txType models.TransactionType, addr string, value, nonce uint64) (*types.Transaction, common.Address, error) {
	if txType == models.NonContractTx {
		tx, address, err := createNonContractTx(addr, value, nonce)
		if err != nil {
			return nil, common.Address{}, fmt.Errorf("failed to create non-contract transaction: %v", err)
		}
		return tx, address, nil
	}
	return nil, common.Address{}, models.ErrInvalidTransactionType
}

// createNonContractTx returns a signed transaction and the recipient address
func createNonContractTx(addr string, value, nonce uint64) (*types.Transaction, common.Address, error) {
	toAddress := common.HexToAddress(addr)

	// create a new transaction using the parameters to send
	transaction := types.NewTransaction(nonce, toAddress, uint256.NewInt(value), params.TxGas, uint256.NewInt(gasPrice), nil)

	// sign the transaction using the developer 0signed private key
	signedTx, err := types.SignTx(transaction, *signer, models.DevSignedPrivateKey)
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("failed to sign non-contract transaction: %v", err)
	}

	return &signedTx, toAddress, nil
}
