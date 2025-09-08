================
Erigon Types
================

This document will list each major types defined and used by Erigon.
They are found in `github.com/erigontech/erigon/execution/types` and `github.com/erigontech/erigon-lib/common`

Address and Hash
================

package: `github.com/erigontech/erigon-lib/common`

.. code-block:: go

    type Address [20]byte

Address represents the 20 byte address of an Ethereum account.


.. code-block:: go

    type Hash [32]byte

Hash represents the 32 byte Keccak256 hash of arbitrary data.
Address represents the 20 byte standard Ethereum Address

Both Hash and Address are bytes arrays.

Transaction
===========

.. code-block:: go

    type Transaction struct {
        data txdata
        time time.Time

        // caches
        hash atomic.Value
        size atomic.Value
        from atomic.Value
    }

    type txdata struct {
        AccountNonce uint64
        Price        uint256.Int
        GasLimit     uint64
        Recipient    *common.Address
        Amount       uint256.Int
        Payload      []byte

        // Signature values
        V uint256.Int
        R uint256.Int
        S uint256.Int

        // This is only used when marshaling to JSON.
        Hash *common.Hash `json:"hash" rlp:"-"`
    }

    type Transactions []*Transaction

represent an Ethereum Transaction.

**from**

Transaction's sender.

**txData.AccountNonce**

Transaction's nonce.

**txdata.Price**

price of each unit of gas.

**txdata.GasLimit**

Maximun amount of units of gas the transaction can use.

**txdata.Recipient**

Recipient of the transaction.

**txdata.Amount**

Amount of ether sent along the transaction.

**txdata.Payload**

transaction's data, it's meant to execute contracts code.

**Transactions type**

an alias for an array of Transaction. Instead of []Transaction, Transactions can be used.

.. code-block:: go

    type Transaction struct {
        data txdata
        time time.Time

        // caches
        hash atomic.Value
        size atomic.Value
        from atomic.Value
    }

    type txdata struct {
        AccountNonce uint64
        Price        uint256.Int
        GasLimit     uint64
        Recipient    *common.Address
        Amount       uint256.Int
        Payload      []byte

        // Signature values
        V uint256.Int
        R uint256.Int
        S uint256.Int

        // This is only used when marshaling to JSON.
        Hash *common.Hash `json:"hash" rlp:"-"`
    }

    type Transactions []*Transaction

represent an Ethereum Transaction.

Block Header
============

package: `github.com/erigontech/erigon/execution/types`

.. code-block:: go

    type Header struct {
        ParentHash  common.Hash
        UncleHash   common.Hash
        Coinbase    common.Address
        Root        common.Hash
        TxHash      common.Hash
        ReceiptHash common.Hash
        Difficulty  *big.Int
        Number      *big.Int
        GasLimit    uint64
        GasUsed     uint64
        Time        uint64
        Extra       []byte
        MixDigest   common.Hash
        Nonce       BlockNonce
    }

It represents a block Header.

**ParentHash**

Its the hash of the block that comes before the Header's block.

**UncleHash**

It's the uncle hash if there is.

**Coinbase**

It's the address of the miner that mined the block.

**Root**

Merkel root of the Header.

**TxHash**

The hash of the block's transactions.

**ReceiptHash**

The hash of the block's transactions receipts.

**Difficulty**

The Total Difficulty of the block.

**Number**

The associated block Number.

**GasLimit**

The block's gas limit.

**GasUsed**

The gas used by the transactions included in the block.

**Time**

Block's timestamp.

Block
=====

.. code-block:: go

    type Block struct {
        header       *Header
        uncles       []*Header
        transactions Transactions

        hash atomic.Value
        size atomic.Value

        td *big.Int

        // These fields are used by package eth to track
        // inter-peer block relay.
        ReceivedAt   time.Time
        ReceivedFrom interface{}
    }

represent a block of the chain.

**header**

Block's Header.

**uncles**

Block's uncles headers block.

**transactions**

Array of transaction included in the block.

**td**

total difficulty accumulated up to the block. sum of all prev blocks difficulties + block difficulty.

Account
=======

package: `github.com/erigontech/erigon/execution/types/accounts`

.. code-block:: go

    type Account struct {
        Initialised bool
        Nonce       uint64
        Balance     uint256.Int
        Root        common.Hash
        CodeHash    common.Hash
        Incarnation uint64
    }


**Nonce**

Number of the type uint64.

nonce of the account (aka. the transaction of the account)

**Balance**

Balance is denominated in wei, and there 10^18 wei in each Ether.

**Root**

Merkle root of the smart contract storage, organised into a tree. Non-contract accounts cannot have storage, therefore root makes sense only for smart contract accounts.

**Code hash**

Hash of the bytecode (deployed code) of a smart contract.

**Incarnation**

a digit which increases each SELFDESTRUCT or CREATE2 opcodes. In fact, it would be possible to create Account with very big storage (increase storage size during many blocks). 
Then delete this account (SELFDESTRUCT). This attack vector would cause nodes to hang for several minutes.

**Important Note: Accounts are not directly linked to their addresses, they are linked as key-value in the database**
