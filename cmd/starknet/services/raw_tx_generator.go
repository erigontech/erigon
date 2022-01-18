package services

import (
	"encoding/hex"
	"errors"
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

func NewRawTxGenerator(privateKey string) RawTxGenerator {
	return RawTxGenerator{
		privateKey: privateKey,
	}
}

type RawTxGenerator struct {
	privateKey string
}

func (g RawTxGenerator) CreateFromFS(fileSystem fs.FS, contractFileName string, salt []byte, gas uint64, writer io.Writer) error {
	privateKey, err := crypto.HexToECDSA(g.privateKey)
	if err != nil {
		return ErrInvalidPrivateKey
	}

	contract, err := fs.ReadFile(fileSystem, contractFileName)
	if err != nil {
		return ErrReadContract
	}

	enc := make([]byte, hex.EncodedLen(len(contract)))
	hex.Encode(enc, contract)

	tx := types.StarknetTransaction{
		CommonTx: types.CommonTx{
			Nonce: 0,
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
