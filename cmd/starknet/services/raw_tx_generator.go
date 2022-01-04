package services

import (
	"encoding/hex"
	"errors"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"golang.org/x/crypto/sha3"
	"io"
	"io/fs"
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

func (g RawTxGenerator) CreateFromFS(fileSystem fs.FS, contractFileName string, salt []byte, writer io.Writer) error {
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
			Nonce: 1,
			Value: uint256.NewInt(1),
			Gas:   1,
			Data:  enc,
			Salt:  salt,
		},
	}

	signature, _ := crypto.Sign(sha3.New256().Sum(nil), privateKey)
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
