package vote

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/pkg/errors"

	"github.com/prysmaticlabs/prysm/crypto/bls"
	validatorpb "github.com/prysmaticlabs/prysm/proto/prysm/v1alpha1/validator-client"
	"github.com/prysmaticlabs/prysm/validator/accounts/iface"
	"github.com/prysmaticlabs/prysm/validator/accounts/wallet"
	"github.com/prysmaticlabs/prysm/validator/keymanager"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

const (
	voteSignerTimeout = time.Second * 5
)

type VoteSigner struct {
	km     *keymanager.IKeymanager
	pubKey [48]byte
}

func NewVoteSigner(blsPasswordPath, blsWalletPath string) (*VoteSigner, error) {
	dirExists, err := wallet.Exists(blsWalletPath)
	if err != nil {
		log.Error("Check BLS wallet exists error: %v.", err)
		return nil, err
	}
	if !dirExists {
		log.Error("BLS wallet did not exists.")
		return nil, fmt.Errorf("BLS wallet did not exists.")
	}

	walletPassword, err := ioutil.ReadFile(blsPasswordPath)
	if err != nil {
		log.Error("Read BLS wallet password error: %v.", err)
		return nil, err
	}
	log.Info("Read BLS wallet password successfully")

	w, err := wallet.OpenWallet(context.Background(), &wallet.Config{
		WalletDir:      blsWalletPath,
		WalletPassword: string(walletPassword),
	})
	if err != nil {
		log.Error("Open BLS wallet failed: %v.", err)
		return nil, err
	}
	log.Info("Open BLS wallet successfully")

	km, err := w.InitializeKeymanager(context.Background(), iface.InitKeymanagerConfig{ListenForChanges: false})
	if err != nil {
		log.Error("Initialize key manager failed: %v.", err)
		return nil, err
	}
	log.Info("Initialized keymanager successfully")

	ctx, cancel := context.WithTimeout(context.Background(), voteSignerTimeout)
	defer cancel()

	pubKeys, err := km.FetchValidatingPublicKeys(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch validating public keys")
	}

	return &VoteSigner{
		km:     &km,
		pubKey: pubKeys[0],
	}, nil
}

func (signer *VoteSigner) SignVote(vote *types.VoteEnvelope) error {
	// Sign the vote, fetch the first pubKey as validator's bls public key.
	pubKey := signer.pubKey
	blsPubKey, err := bls.PublicKeyFromBytes(pubKey[:])
	if err != nil {
		return errors.Wrap(err, "convert public key from bytes to bls failed")
	}

	voteDataHash := vote.Data.Hash()

	ctx, cancel := context.WithTimeout(context.Background(), voteSignerTimeout)
	defer cancel()

	signature, err := (*signer.km).Sign(ctx, &validatorpb.SignRequest{
		PublicKey:   pubKey[:],
		SigningRoot: voteDataHash[:],
	})
	if err != nil {
		return err
	}

	copy(vote.VoteAddress[:], blsPubKey.Marshal()[:])
	copy(vote.Signature[:], signature.Marshal()[:])
	return nil
}
