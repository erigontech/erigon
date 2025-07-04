// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package keystore

import (
	"errors"
	"fmt"
	ethereum "github.com/erigontech/erigon"
	"math/big"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/types"
)

// keystoreWallet implements the Account interface for the original
// keystore.
type keystoreWallet struct {
	account  Account   // Single account contained in this wallet
	keystore *KeyStore // Keystore where the account originates from
}

// URL implements Account, returning the URL of the account within.
func (w *keystoreWallet) URL() URL {
	return w.account.URL
}

// Status implements Account, returning whether the account held by the
// keystore wallet is unlocked or not.
func (w *keystoreWallet) Status() (string, error) {
	w.keystore.mu.RLock()
	defer w.keystore.mu.RUnlock()

	if _, ok := w.keystore.unlocked[w.account.Address]; ok {
		return "Unlocked", nil
	}
	return "Locked", nil
}

// Open implements Account, but is a noop for plain wallets since there
// is no connection or decryption step necessary to access the list of accounts.
func (w *keystoreWallet) Open(passphrase string) error { return nil }

// Close implements Account, but is a noop for plain wallets since there
// is no meaningful open operation.
func (w *keystoreWallet) Close() error { return nil }

// Accounts implements Account, returning an account list consisting of
// a single account that the plain keystore wallet contains.
func (w *keystoreWallet) Accounts() []Account {
	return []Account{w.account}
}

// Contains implements Account, returning whether a particular account is
// or is not wrapped by this wallet instance.
func (w *keystoreWallet) Contains(account Account) bool {
	return account.Address == w.account.Address && (account.URL == (URL{}) || account.URL == w.account.URL)
}

// Derive implements Account, but is a noop for plain wallets since there
// is no notion of hierarchical account derivation for plain keystore accounts.
func (w *keystoreWallet) Derive(path DerivationPath, pin bool) (Account, error) {
	return Account{}, ErrNotSupported
}

// SelfDerive implements Account, but is a noop for plain wallets since
// there is no notion of hierarchical account derivation for plain keystore accounts.
func (w *keystoreWallet) SelfDerive(bases []DerivationPath, chain ethereum.ChainReader) {
}

// signHash attempts to sign the given hash with
// the given account. If the wallet does not wrap this particular account, an
// error is returned to avoid account leakage (even though in theory we may be
// able to sign via our shared keystore backend).
func (w *keystoreWallet) signHash(account Account, hash []byte) ([]byte, error) {
	// Make sure the requested account is contained within
	if !w.Contains(account) {
		return nil, ErrUnknownAccount
	}
	// Account seems valid, request the keystore to sign
	return w.keystore.SignHash(account, hash)
}

// SignData signs keccak256(data). The mimetype parameter describes the type of data being signed.
func (w *keystoreWallet) SignData(account Account, mimeType string, data []byte) ([]byte, error) {
	return w.signHash(account, crypto.Keccak256(data))
}

// SignDataWithPassphrase signs keccak256(data). The mimetype parameter describes the type of data being signed.
func (w *keystoreWallet) SignDataWithPassphrase(account Account, passphrase, mimeType string, data []byte) ([]byte, error) {
	// Make sure the requested account is contained within
	if !w.Contains(account) {
		return nil, ErrUnknownAccount
	}
	// Account seems valid, request the keystore to sign
	return w.keystore.SignHashWithPassphrase(account, passphrase, crypto.Keccak256(data))
}

// SignText implements Account, attempting to sign the hash of
// the given text with the given account.
func (w *keystoreWallet) SignText(account Account, text []byte) ([]byte, error) {
	return w.signHash(account, TextHash(text))
}

// SignTextWithPassphrase implements Account, attempting to sign the
// hash of the given text with the given account using passphrase as extra authentication.
func (w *keystoreWallet) SignTextWithPassphrase(account Account, passphrase string, text []byte) ([]byte, error) {
	// Make sure the requested account is contained within
	if !w.Contains(account) {
		return nil, ErrUnknownAccount
	}
	// Account seems valid, request the keystore to sign
	return w.keystore.SignHashWithPassphrase(account, passphrase, TextHash(text))
}

var (
	ErrUnknownAccount = errors.New("unknown account")
	ErrNotSupported   = errors.New("not supported")
)

// SignTx implements Account, attempting to sign the given transaction
// with the given account. If the wallet does not wrap this particular account,
// an error is returned to avoid account leakage (even though in theory we may
// be able to sign via our shared keystore backend).
func (w *keystoreWallet) SignTx(account Account, tx types.Transaction, chainID *big.Int) (types.Transaction, error) {
	// Make sure the requested account is contained within
	if !w.Contains(account) {
		return nil, ErrUnknownAccount
	}
	// Account seems valid, request the keystore to sign
	return w.keystore.SignTx(account, tx, chainID)
}

// SignTxWithPassphrase implements Account, attempting to sign the given
// transaction with the given account using passphrase as extra authentication.
func (w *keystoreWallet) SignTxWithPassphrase(account Account, passphrase string, tx types.Transaction, chainID *big.Int) (types.Transaction, error) {
	// Make sure the requested account is contained within
	if !w.Contains(account) {
		return nil, ErrUnknownAccount
	}
	// Account seems valid, request the keystore to sign
	return w.keystore.SignTxWithPassphrase(account, passphrase, tx, chainID)
}

// AuthNeededError is returned by backends for signing requests where the user
// is required to provide further authentication before signing can succeed.
//
// This usually means either that a password needs to be supplied, or perhaps a
// one time PIN code displayed by some hardware device.
type AuthNeededError struct {
	Needed string // Extra authentication the user needs to provide
}

// NewAuthNeededError creates a new authentication error with the extra details
// about the needed fields set.
func NewAuthNeededError(needed string) error {
	return &AuthNeededError{
		Needed: needed,
	}
}

// Error implements the standard error interface.
func (err *AuthNeededError) Error() string {
	return fmt.Sprintf("authentication needed: %s", err.Needed)
}
