package services

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"io"
	"io/fs"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

var (
	ErrReadContract      = errors.New("contract read error")
	ErrInvalidPrivateKey = errors.New("invalid private key")
)

type Store interface {
	GetNonce(address string) int
}

func NewRawTxGenerator(privateKey string) *RawTxGenerator {
	return &RawTxGenerator{
		privateKey: privateKey,
	}
}

type RawTxGenerator struct {
	privateKey string
}

func (g RawTxGenerator) CreateFromFS(ctx context.Context, fileSystem fs.FS, db kv.RoDB, contractFileName string, salt []byte, gas uint64, writer io.Writer) error {
	privateKey, err := crypto.HexToECDSA(g.privateKey)
	if err != nil {
		return ErrInvalidPrivateKey
	}

	address, err := addressFromPrivateKey(privateKey)
	if err != nil {
		return err
	}

	nonce, err := getNonce(ctx, db, address)
	if err != nil {
		return err
	}

	contract, err := fs.ReadFile(fileSystem, contractFileName)
	if err != nil {
		return ErrReadContract
	}

	enc := make([]byte, hex.EncodedLen(len(contract)))
	hex.Encode(enc, contract)

	tx := types.StarknetTransaction{
		CommonTx: types.CommonTx{
			Nonce: nonce + 1,
			Value: uint256.NewInt(1),
			Gas:   gas,
			Data:  enc,
		},
		Salt:   salt,
		FeeCap: uint256.NewInt(875000000),
		Tip:    uint256.NewInt(100000),
	}

	sighash := tx.SigningHash(params.FermionChainConfig.ChainID)

	signature, _ := crypto.Sign(sighash[:], privateKey)
	signer := types.MakeSigner(params.FermionChainConfig, 1)

	signedTx, err := tx.WithSignature(*signer, signature)
	if err != nil {
		return err
	}

	err = signedTx.MarshalBinary(writer)
	if err != nil {
		return errors.New("can not save signed tx")
	}

	return nil
}

func addressFromPrivateKey(privateKey *ecdsa.PrivateKey) (common.Address, error) {
	publicKey := privateKey.Public()
	publicKeyECDSA, _ := publicKey.(*ecdsa.PublicKey)
	return crypto.PubkeyToAddress(*publicKeyECDSA), nil
}

func getNonce(ctx context.Context, db kv.RoDB, address common.Address) (uint64, error) {
	var nonce uint64 = 0

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nonce, fmt.Errorf("cannot open tx: %w", err)
	}
	blockNumber, err := stages.GetStageProgress(tx, stages.Execution)
	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return nonce, nil
	}
	return acc.Nonce, nil
}
