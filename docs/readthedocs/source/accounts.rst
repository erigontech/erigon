========
Accounts
========
The accounts package implements integrates support for various kind of wallets into TurboGeth.
As of now, the following type of wallets are supported:

    * Keystore
    * Smart Card wallet
    * USB Wallets (E.g ledger)
    * External Signers

Keystore
========

The Keystore wallets refers to the wallets contained into the keystore/ directory, which can be generated with `personal.newAccount` in the console.

The implementantion works with the following components:
    * `watcher`, which listen to file system events in the keystore dir, so that it can tell `accountCache` when to scan.
    * `fileCache`, track changes in keystore directory.
    * `accountCache`, a live index of all accounts in the keystore (uses `watcher`).
    * `Key`, it is an address-private_key pair, that is given by `Keystore` when there is an unlock.
    * `keystoreWallet`, repressent a wallet
    * `keyStorePlain`, it handles plain(unencrypted) keystore.
    * `keyStorePassphrase`, it handles encrypted json keystore.
    * `Keystore`, it repressent the keystore itself, it uses all the components cited above accordingly to context.

Keystore: `watcher`
===================
source: `(accounts/keystore/watch.go)`

it contains a reference to an `accountCache`.

listen to file system events in the keystore dir, so that it can tell `accountCache` when to scan.

.. code-block:: go

    func (w *watcher) start();

`start()` starts the `loop()` goroutine.

.. code-block:: go

    func (w *watcher) close();

`stop()` closes the `loop()` goroutine.

.. code-block:: go

    func (w *watcher) loop();

`loop()` is to be used as a goroutine by `start`. It listen for file system events
into the keystore directory and when some are found it makes the `accountCache` scan
the keystore to find out what changed by invoking `scanAccounts()` from `accountCache`

Keystore: `fileCache`
=====================
source: `(accounts/keystore/fileCache.go)`

keeps track of changes in keystore directory.

.. code-block:: go

    func (fc *fileCache) scan(keyDir string) (mapset.Set, mapset.Set, mapset.Set, error)

Arguments:
    * keyDir: directory of Keystore
Returns (if successful) :
    * set of created files
    * set of deleted files
    * set of changed files

`scan()` returns what file has been added, deleted anc changed after a file system event reported by
`watcher`, its purpose is to tell `accountCache` how to modify the list of accounts accordingly to the new changes.

Keystore: `accountCache`
========================
source: `(accounts/keystore/accountCache.go)`

live index of accounts in keystore. make use of
fileCache and watcher. when watcher detects an event,
accountCache get from fileCache the changes and updates itself accordingly

.. code-block:: go

    func (ac *accountCache) accounts() []accounts.Account;

returns the accounts in keystore

.. code-block:: go

    func (ac *accountCache) hasAddress(addr common.Address) bool;

returns wheter there is a certain address in keystore

.. code-block:: go

    func (ac *accountCache) add(newAccount accounts.Account);

add a new account in live index, this is executed only if `fileCache` detects a file creation or
changes in an existing one.

.. code-block:: go

    func (ac *accountCache) delete(removed accounts.Account);

remove an Account in live index, this is executed only if `fileCache` detects a file deletion or
changes in an existing one.

.. code-block:: go

    func (ac *accountCache) scanAccounts() error;

it's executed by `watcher` when a file system event in the keystore directory is detected.
it first gets what changed, deleted and added through `fileCache`.
then for each added file, it `add()` them to account list, for file deleted it
`delete()` from the account list the deleted one and if it detects a change: it first
`delete()` the account and then `add()` it back again immediately after.

Keystore: `Key`
===============
source: `(accounts/keystore/key.go)`

`Key` is a address-private_key pair. it is used in plain keystores as a json and in
other external components.

.. code-block:: go

    type Key struct {
        Id uuid.UUID
        Address common.Address
        PrivateKey *ecdsa.PrivateKey
    }

Keystore: `keystoreWallet`
==========================

source: `(accounts/keystore/wallet.go)`

it function as a wallet from the keystore.
it is an accounts.Wallet and its the type of wallet use in `(accounts/keystore/keystore.go)`.
it does **not** implement the following methods from wallet since it behaves like a plain wallet:

    * `open()`
    * `close()`
    * `SelfDerive()`
    * `Derive()`

.. code-block:: go

    func (w *keystoreWallet) Status() (string, error);

it checks from the Keystore if the wallet is unlocked or not. the string is:

.. code-block:: go

    "Unlocked" // unlocked wallet
    "Locked"   // locked wallet

"Locked" is returned if the wallet is locked, and "Unlocked" if it is unlocked.

.. code-block:: go

    func (w *keystoreWallet) Accounts() []accounts.Account;

returns a list of 1 element which is the account repressented by the wallet.

.. code-block:: go

    func (w *keystoreWallet) Contains(account accounts.Account) bool

returns true if account is the same as wallet's.

.. code-block:: go

    func (w *keystoreWallet) Contains(account accounts.Account) bool

returns true if account is the same as wallet's.

.. code-block:: go

    func (w *keystoreWallet) SignData(account accounts.Account, mimeType string, data []byte) ([]byte, error);
    func (w *keystoreWallet) SignDataWithPassphrase(account accounts.Account, passphrase, mimeType string, data []byte) ([]byte, error);
    func (w *keystoreWallet) SignText(account accounts.Account, text []byte) ([]byte, error);
    func (w *keystoreWallet) SignTextWithPassphrase(account accounts.Account, passphrase string, text []byte) ([]byte, error);
    func (w *keystoreWallet) SignTx(account accounts.Account, tx *types.Transaction, chainID *big.Int) (*types.Transaction, error);
    func (w *keystoreWallet) SignTxWithPassphrase(account accounts.Account, passphrase string, tx *types.Transaction, chainID *big.Int) (*types.Transaction, error);

the functions above are all used for signing related tasks by using the wallet's account.

Keystore: `keystorePlain` and `keystorePassphrase`
==================================================

source: `(accounts/keystore/plain.go)`

source: `(accounts/keystore/passphrase.go)`

they both implements these same methods:

.. code-block:: go

    GetKey(addr common.Address, filename, auth string) (*Key, error)
    StoreKey(dir, auth string, scryptN, scryptP int) (accounts.Account, error)

which are used to store and retrieve the private key of an account into the keystore, the difference beetwen
keystorePlain and keystorePassphrase is that keyStorePlain does not require a password and
the private key is in plain text in the keystore dir, while keystorePassphrase is encrypted with a password.

Keystore: `Keystore`
====================

source: `(accounts/keystore/keystore.go)`

the Keystore object is a cluster in which all the above components are used accordingly,
it uses `fileCache`, `watcher` and `accountCache` to keep track of the keystore directory and have access to the wallets and
has the power to unlock and used each wallets inside the keystore wallet.

.. code-block:: go

    NewKeyStore(keydir string, scryptN, scryptP int) *KeyStore;
    NewPlaintextKeyStore(keydir string) *KeyStore;

the two function create a plainKeystore or a passphrase encrypted one.

.. code-block:: go

    func (ks *KeyStore) Wallets() []accounts.Wallet;

return every accounts in the keystore.

.. code-block:: go

    func (ks *KeyStore) Subscribe(sink chan<- accounts.WalletEvent) event.Subscription

creates an async subscription to receive notifications on the addition or removal of keystore wallets.

.. code-block:: go

    func (ks *KeyStore) HasAddress(addr common.Address) bool {

returns wheter given account is in keystore.

.. code-block:: go

    func (ks *KeyStore) Accounts() []accounts.Account {

returns all accounts in keystore.

.. code-block:: go

    func (ks *KeyStore) Delete(a accounts.Account, passphrase string);

delete an account from keystore. passphrase is needed.

.. code-block:: go

    func (ks *KeyStore) SignData(account accounts.Account, mimeType string, data []byte) ([]byte, error);
    func (ks *KeyStore) SignDataWithPassphrase(account accounts.Account, passphrase, mimeType string, data []byte) ([]byte, error);
    func (ks *KeyStore) SignText(account accounts.Account, text []byte) ([]byte, error);
    func (ks *KeyStore) SignTextWithPassphrase(account accounts.Account, passphrase string, text []byte) ([]byte, error);
    func (ks *KeyStore) SignTx(account accounts.Account, tx *types.Transaction, chainID *big.Int) (*types.Transaction, error);
    func (ks *KeyStore) SignTxWithPassphrase(account accounts.Account, passphrase string, tx *types.Transaction, chainID *big.Int) (*types.Transaction, error);

the functions above are all used for signing related tasks by using the wallet's account.

.. code-block:: go

    func (ks *KeyStore) Lock(addr common.Address) error

lock a certain account in the keystore and remove the its private key from memory

.. code-block:: go

    func (ks *KeyStore) TimedUnlock(a accounts.Account, passphrase string, timeout time.Duration) error {

unlock an account for a given amount of time.

.. code-block:: go

    func (ks *KeyStore) NewAccount(passphrase string) (accounts.Account, error)

generates a new `Key` and store the related account in the keystore.

.. code-block:: go

    func (ks *KeyStore) Export(a accounts.Account, passphrase, newPassphrase string) (keyJSON []byte, err error);

export given account to json format encrypted with newPassphrase.

.. code-block:: go

    func (ks *KeyStore) Import(a accounts.Account, passphrase, newPassphrase string) (accounts.Account, error);

import given account and encrypt key with newPassphrase.

.. code-block:: go

    func (ks *KeyStore) ImportECDSA(priv *ecdsa.PrivateKey, passphrase string) (accounts.Account, error) {

stores the given key into the key directory, encrypting it with the passphrase.

.. code-block:: go

    func (ks *KeyStore) Update(a accounts.Account, passphrase, newPassphrase string) error {

changes the passphrase of an existing account.

.. code-block:: go

    func (ks *KeyStore) ImportPreSaleKey(keyJSON []byte, passphrase string) (accounts.Account, error);

decrypts the given Ethereum presale wallet and stores
a key file in the key directory. The key file is encrypted with the same passphrase.