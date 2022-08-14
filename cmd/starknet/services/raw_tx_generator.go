package services

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/adapter"
)

var (
	ErrReadContract      = errors.New("contract read error")
	ErrInvalidPrivateKey = errors.New("invalid private key")
)

type Config struct {
	ContractFileName string
	Salt             []byte
	Gas              uint64
	Nonce            uint64
}

func NewRawTxGenerator(privateKey string) *RawTxGenerator {
	return &RawTxGenerator{
		privateKey: privateKey,
	}
}

type RawTxGenerator struct {
	privateKey string
}

func (g RawTxGenerator) CreateFromFS(ctx context.Context, fileSystem fs.FS, db kv.RoDB, config *Config, writer *bytes.Buffer) error {
	privateKey, err := crypto.HexToECDSA(g.privateKey)
	if err != nil {
		return ErrInvalidPrivateKey
	}

	address, err := addressFromPrivateKey(privateKey)
	if err != nil {
		return err
	}

	nonce, err := getNonce(ctx, db, address, config.Nonce)
	if err != nil {
		return err
	}

	contract, err := fs.ReadFile(fileSystem, config.ContractFileName)
	if err != nil {
		return ErrReadContract
	}

	enc := make([]byte, hex.EncodedLen(len(contract)))
	hex.Encode(enc, contract)

	tx := types.StarknetTransaction{
		CommonTx: types.CommonTx{
			Nonce: nonce + 1,
			Value: uint256.NewInt(1),
			Gas:   config.Gas,
			Data:  enc,
		},
		Salt:   config.Salt,
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

	err = signedTx.(rlp.Encoder).EncodeRLP(writer)
	signedTxRlp := writer.Bytes()
	writer.Reset()
	writer.WriteString(hexutil.Encode(signedTxRlp))

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

func getNonce(ctx context.Context, db kv.RoDB, address common.Address, configNonce uint64) (uint64, error) {
	if configNonce != 0 {
		return configNonce, nil
	}

	var nonce uint64 = 0

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nonce, fmt.Errorf("cannot open tx: %w", err)
	}
	defer tx.Rollback()
	blockNumber, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nonce, err
	}
	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if err != nil {
		return nonce, err
	}

	if acc == nil {
		return 0, nil
	}

	return acc.Nonce, nil
}
