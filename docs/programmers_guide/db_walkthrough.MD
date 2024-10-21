This document attempts to explain how Erigon organises its persistent data in its database and how this organisation is
different from go-ethereum, the project from which it is derived. We start from a very simple genesis block and then
apply one block containing an ETH transfer. For each step, we show visualisations produced by the code available in
Erigon and code added to a fork of go-ethereum.

Genesis in Erigon
====================
For the genesis block, we generate 3 different private keys and construct Ethereum addresses from them.
Then, we endow one of the accounts with 9 ETH, and two others with 0.2 and 0.3 ETH, respectively.
This is how the initial state trie looks:

![genesis_state](state_0.png)

In this and other illustrations, the colored boxes correspond to hexadecimal digits (a.k.a. nibbles), with values 0..f.
Here is the palette:

![hex_palette](hex_palette.png)

First thing to note about the illustration of the initial state trie is that the leaves correspond to our accounts with their
ETH endowments. Account nonces, in our case all 0s, are also shown. If you count the number of coloured boxes, from top
to bottom, up to any of the account leaves you will get 64. Since each nibble occupies half a byte, that makes each "key"
in the state trie 32 bytes long. However, account addresses are only 20 bytes long. The reason we get 32 and not 20 is
that all the keys (in our case account addresses) are processed by the `Keccak256` hash function (which has 32 byte
output) before they are inserted into the trie. If we wanted to see what the corresponding account addresses were, we
will have to look into the database. Here is what Erigon would persist after generating such a genesis block:

![genesis_db](changes_0.png)

The Erigon database is a key-value store organised in tables (also commonly referred to as "buckets"). This is the list
of the __main__ tables in the database:

- __Headers__
- __Block Bodies__
- __Header Numbers__
- __Receipts__
- __PlainState__
- __History Of Accounts__
- __Change Sets__
- __HashedState__
- __IntermediateTrieHashes__
- __Tx Senders__


Table "Headers"
---------------

This table stores information about block headers. For each block there are three types of block header records with the following (key, value) formats:

* __FULL HEADER__: contains the complete block header with all its information parameters
    - _key_ : 8-byte big-endian block number + 32-byte block hash
    - _value_ : the RLP-encoded block header structured as defined in the [Ethereum Yellow Paper](https://github.com/ethereum/yellowpaper):
        * parent hash: 32-byte hash of the parent block
        * uncles hash: 32-byte hash of the uncle blocks
        * coinbase: 20-byte address beneficiary of mining reward
        * state root: 32-byte root hash of the state trie
        * transactions root: 32-byte root hash of the trie structure made up of the block transactions
        * receipts root: 32-byte root hash of the trie structure made up of the transaction receipts
        * logs bloom: 256-byte Bloom filter composed from indexable information in each log entry from the transaction receipts
        * difficulty: 8-byte scalar value corresponding to the difficulty level of this block
        * block number: 8-byte scalar value equal to the number of ancestor blocks in the chain (aka block height)
        * gas limit: 8-byte scalar value equal to the current limit of gas expenditure per block
        * gas used: 8-byte scalar value equal to the total gas used in transactions in this block
        * timestamp: 8-byte scalar value equal to the Unix epoch timestamp at the inception of this block
        * extra data: up to 32-byte array of additional relevant data for this block (at least for Eth-hash consensus engine, for Clique could be more)
        * mix_hash: 64-byte hash proving, combined with nonce, the computation amount spent in block mining
        * nonce: 8-byte value proving, combined with mix hash, the computation amount spent in block mining
* __TOTAL DIFFICULTY HEADER__: contains the total mining difficulty (TD) of the chain ending in such specific block
    - _key_ : 8-byte big-endian block number + 32-byte block hash + `0x74` suffix (ASCII code for `t` character)
    - _value_ : variable-length RLP-encoded total difficulty value: the cumulative difficulty value from first block to this one
* __CANONICAL HEADER__: contains the block hash
    - key: 8-byte big-endian block number + `0x6E` suffix (ASCII code for `n` character)
    - value: 32-byte block hash

as shown in the following picture for the block 0 (from top to bottom the three types of records, key on the left and value on the right):

![genesis_db_headers](changes_0_Headers.png)

You can check that for the genesis block

* block number 0 is encoded as 0x0000000000000000 (see 8-byte zeros start of each key)
* block hash is 0x61eb...aba1
* logs bloom is 256-byte zeros (see the 8 white rows of all 32-byte 0s)
* total difficulty is 0x80, which is RLP encoding of value 0

Table "Block Bodies"
---------------------

![genesis_db_block_bodies](changes_0_BlockBodies.png)

The keys in this table are concatenations of 8-byte encoding of the block number and 32-byte block hash.
The values are RLP-encoded list of 2 structures:

1. List of transactions
2. List of ommers (a.k.a uncles)

In the case of the genesis block, all of these lists are empty (RLP encodings `0xC0`), and the prefix `0xC3` means
in RLP "some number of sub-structures with the total length of 3 bytes".


Next three tables, "Last Header", "Last Fast", and "Last Block", always contain just one record each, and their
keys are always the same, the ASCII-encodings of the strings `LastHeader`, `LastFast`, and `LastBlock`, respectively.
The values record the block/header hash of the last header, receipt or block chains that the node has managed to sync
from its peers in the network. The value in "Last Fast" bucket is not really used at the moment, because Erigon
does not support Fast Sync.

Table "Header Numbers"
-----------------------

Is a mapping of 32-byte header/block hashes to the corresponding block numbers (encoded
in 8 bytes):

![genesis_db_header_numbers](changes_0_HeaderNumbers.png)

Table "Receipts"
-----------------

Records the list of transaction receipts for each block:

![genesis_db_receipts](changes_0_Receipts.png)

The 8 bytes of the key (or 16 nibbles, equaling to 0s here) encode the block number, which is 0 for the Genesis block.
The value is the CBOR-encoded list of receipts. In our case, there were
no transactions in the Genesis block, therefore, we have CBOR encoding of an empty list, `0xf6`.

Table "PlainState"
-----------------

![genesis_db_accounts](changes_0_PlainState.png)

Store together Accounts and Storage. 

Accounts: key="account address", value="current state of each account". 
Storage: key="account address+incarnation+storage hash", value="current value of storage".

The accounts encoded by function `account.go:EncodeForStorage()` so that the first byte of a 
field is its length and bytes afterward the field itself. 
They all start with a fieldset of `0x02`, for each bit set into the fieldSet a field is present.
In this case `0x02` in binary is `10` meaning that only the second field is set (the balance).
the order in the fieldset is the following:
* 1st bit: Nonce
* 2nd bit: Balance
* 3rd bit: Incarnation
* 4th bit: Code Hash

Therefore, immediately after the fieldset we have the length in byte of the balance: `0x08` (`8` bytes). 
the following 8 bytes (aka. 16 nibbles) contains the hexadecimal value of the 
balance that in the first record would be: `0x7ce66c50e2840000`.

````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x7ce66c50e2840000
9000000000000000000
````

Which is 9 followed by 18 zeros, which is 9 ETH (1 ETH = 10^18 wei).

The third bit of the field set represent the Incarnation. The Incarnation is a Erigon specific attribute, which is
used to make removal and revival of contract accounts (now possible with `CREATE2` since Constantinopole) efficient
with Erigon's database layout. For now it will suffice to say that all non-contract accounts will have 
incarnation 0 (not set), and all contract accounts will start their existence with incarnation 1.

Contract accounts may also contain a contract code hash and storage root. These two pieces of information would make
the record in the "Accounts" bucket contain 5 instead of 3 fields.

Table "History Of Accounts"
----------------------------

![genesis_db_history_of_accounts](changes_0_HistoryOfAccounts.png)

key="account addresses + incarnation", value="encoded array of block numbers, where account changed/created".

The history of accounts records how the accounts changed at each block. But, instead of recording,
at each change, the value that the accounts had *after* the change, it records what value the accounts
had *before* the change. That explains the empty values here -- it records the fact that these
three accounts in questions did not exist prior to the block 0.

Table "Change Sets"
--------------------
 
Records the history of changes in accounts and contract storage. But, unlike in "History of Accounts"
and "History of Storage", where keys are derived from accounts' addresses
(key="address hash + block number"), in the "Change Sets" table, keys are derived from the block numbers:

![genesis_db_change_sets](changes_0_AccountChanges.png)

In the cases of our Genesis block, the key is composed from the encoding of the block number (`0x20`), and the
ASCII-code of `hAT` (meaning **h**istory of **A**counts **T**rie).
The "Change Set" table records changes that happen to accounts and contract storage slots at every block.
It is important to note that the values recorded in the "Changes Set" table are not the values the accounts
(or storage slots) had AFTER the change, it records what value the accounts (or storage slots)
had BEFORE the change. That explains the empty values here - it records the fact that these
three accounts in question did not exist prior to the block 0.
The encoding of the values in the records is tailored for fast access and binary search. It has 5 parts:
1. Number of keys-value pairs, encoded as a 4-byte (32-bit) number. In this example, it is `0x00000003`, which means
there are 3 key-value pairs
2. Size of each key, also encoded as a 32-bit number. All keys are the same size, which makes it possible to
access them without deserialisation. In this example, it is `0x00000020`, which 32, meaning that all keys are
32 bytes long.
3. Keys themselves. In our examples, these are the coloured boxes before the streak of white 0s. Keys are sorted
lexicographically. This, together with the keys being the same size, allows binary search without deserialisation,
as well as linear-time merge of multiple changesets.
4. Value offsets. These offsets mark the beginning of the next, 5th part as offset 0. First value has offset 0.
In our example, all values are empty strings, therefore we see 3 zero offsets (24 white boxes with zeros in them).
5. Values themselves. In our example, they are empty, so this 5th part is not present.

Tables "HashedState" and "IntermediateTrieHashes"
--------------------------------------------------

These tables are used to calculate Merkle Trie root hash of whole current state. For Genesis block, these tables
are not populated, because the root hash is known in advance.

Block with 1 Ethereum transaction in Erigon
===============================================
The first block contains transaction that transfers 0.001 ETH from the account holding 9 ETH to
a new account with the address `0x0100000000000000000000000000000000000000` (this is what one gets
specifying `common.Address{1}` in the code).
This is how the state trie after Block 1 is inserted looks like:

![block1_state](state_2.png)

First thing to note about the illustration of the state trie is that the nonce of the first account is now 1 since 
a transaction from that address has been executed and the amount of ETH of that account is now 
8.999 since the transaction had a value of 0.001 ETH. Usually, the transaction fee would also
be paid, but in our simplified example, we set gas price to 0, therefore no fees are deducted.
A new account has been added to the state trie, the recipient of the transaction.
In fact, it has 0.001 ETH (the value of the transaction) and its nonce is set to 0.
What follows are excerpts from the Erigon database after block 1 has been inserted. 
These excerpts only show the records that have changed, and
omit the records that are the same as before the insertion of the block.

![block1_db](changes_1.png)

Table "Receipts"
----------------

Table `Receipts` now has some non-empty information for the block 1, because we had 1 transaction:

![block1_db_receipts](changes_1_Receipts.png)

The first 8 bytes of the key (or 16 nibbles, 15 zeroes followed by `1`) encode the block number, which is 1.
Receipt records are only kept for "canonical" blocks, and not for forked blocks, that is why receipt keys
do not include 32-byte block hash.

The value is the CBOR-encoded list of receipts. In our case, a transaction was sent and as a matter of fact,
instead of having `0xC0` (that represent an empty list) we have `0xc6c501825208c0` that is the receipt for the 0.001 ETH transaction.
First prefix, `c6` means that the following 6 bytes is a sequence of RLP-encoded sub-structures. 
In our case, there is only one sub-structure, and its
prefix `c5` means that the following 5 bytes is a sequence of RLP-encoded sub-structures. 
There are 3 of them: `01`, `825208`, and `c0`. The first encodes
number 1, which means the "Success" status of the transaction (0 would mean failure). 
Next, we have a byte array of length 2 (prefix `82`), `5208`, which is
21000 in hexadecimal, the cumulative gas used by all transactions so far. 
And the last structure, `c0` is RLP encoding of an empty list. Usually, the log (events) emitted by the transaction, 
would be here. But since our transaction did not emit anything, the list is empty.

Table "Senders"
---------------
![block1_db_senders](changes_1_Senders.png)
The reason Erigon keeps the list of transaction sender addresses for each transaction has to do with the fact that
transactions themselves do not contain this information directly. Sender's address can be "recovered" from the digital
signature, but this recovery can be computationally intensive, therefore we "memoise" it.


Table "History Of Accounts"
---------------------------

![block1_db_history_of_accounts](changes_1_HistoryOfAccounts.png)

The first entry has a non-empty value. This is the sender of 0.001 ETH.
As history bucket records the value of this account *before* the change, we should
expect the same value here as this account had at the Genesis.
The other two records contain empty values, which means these account were non-existent previously. 
These are the account of the recipient of 0.001 ETH (`0x0100000000000000000000000000000000000000`), 
and the miner account `0x0000000000000000000000000000000000000000`.
The keys of all three records have the common suffix of `0x21` instead of `0x20`, which is 
simply encoding of the block number 1 instead of block number 0.

**WARNING** We are planning a change, called `THIN_HISTORY`, after which the bucket 
"History of Accounts" will not store the values of the accounts,
but only the block numbers when those accounts changed.

Next bucket is "Headers"

![block1_db_headers](changes_1_Headers.png)

A new header has been generated since a new block has been mined.
As a matter of fact, the keys for the first two records start with a block number of 1 followed by
the block hash (or header hash, which is the same thing) that is 
different than the Genesis block. Additionally, these two records for the new block
that respectively represent the values in the headers and the mining difficulty differs from the Genesis Block.

Next bucket is "Block Bodies":

![block1_db_block_bodies](changes_1_BlockBodies.png)

The keys in this bucket are concatenations of 8-byte encoding of the block number and 32-byte block hash.
The new record is the representation of block 1.

![block1_db_change_sets](changes_1_AccountChanges.png)

Bucket "Change Sets" recorded the change in account that occurred *before* the 0.001 ETH transaction. You can notice that unlike the Genesis block, this time the key has a prefix of `0x21` since now the blockchain is 1 block apart from the Genesis block.
Like in the ChangeSet in the Genesis block, there are 3 new records. But unlike the ChangeSet in the genesis block, one of the records (the last one) has
a non-empty value.

The next bucket is "PlainState":

![block1_db_accounts](changes_1_PlainState.png)

The values are the current state of each account. 
* The first record has a field set of `0x3` (`11`) meaning that this time the account has a nonce as well. The nonce length is `0x01`, therefore the nonce is stored in the byte immediately after the length of the field: `0x01`. The balance is now changed to `0x7ce2ded23dbd8000`.
The value `0x7ce2ded23dbd8000` is the hexadecimal value for:
````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x7ce2ded23dbd8000
8999000000000000000
````
Which is 9 ETH - 0.001 ETH, this means that this account is the sender.
* in the second value the field set is `0x02` meaning that only the balance is set. 
The value `0x1bc16d674ec80000` (balance) is the hexadecimal value for:
````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x1bc16d674ec80000
2000000000000000000
````
Which is 2 ETH that probably represent the mining reward.

* in the third value we have prefix `0x06` (`110`), meaning that only balance and incarnation are set. the balance is now `0x038d7ea4c68000` .Start python and do this:
````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x038d7ea4c68000
1000000000000000
````
Which is 0.001 ETH that is exactly how much has been sent to the recipient address, meaning that this account represent the recipient of the transaction.



Because Erigon does it completely different from go-ethereum, 
we created [dedicated article](guide.md#organising-ethereum-state-into-a-merkle-tree) for it. 

Genesis in go-ethereum
------------------------------

Now we will create the same Genesis state and block in go-ethereum (in archive mode to make sure we compare like for like).
Here is how the database looks like. Since go-ethereum uses LevelDB, and LevelDB does not have a concept of "buckets" (or
"tables"), go-ethereum emulates them by adding table-specific prefixes to all the keys, with the exception of the keys that
describe the state trie (bucket "Hashes" in our example). In the illustration, these prefixes are mostly removed for better
comparison with Erigon. They were not removed only for the buckets "LastBlock", "LastHeader" and "LastFast", because
otherwise they key would be empty.

![geth_genesis_db](geth_changes_0.png)

The buckets "Headers", "Config", "Last Header", "Last Fast", "Last Block", all look identical
to those in the Erigon database. We will walk through the ones that are different.

In the bucket "Block Bodies", the value is slightly different:

![geth_genesis_block_bodies](geth_changes_0_b_5.png)

The difference is that the block body has 2 elements instead of 3 in Erigon. The missing element is the list
of the sender addresses that go-ethereum does not store, but recomputes after loading or caches in memory.

The buckets "Accounts", "History Of Accounts", and "Change Sets" are missing, because go-ethereum uses a very
different mechanism for storing the state and its history:

![geth_genesis_hashes](geth_changes_0_hashes_0.png)

In the illustration showing the state trie, one can find 4 parts of the diagram that consist of the coloured boxes
(that excludes the leaves that contain account balances and nonces). These parts are usually called "trie nodes",
and in the diagram above we see 2 types of trie nodes:
1. Branch node. This is the horizontal line of 3 coloured boxes on the top. It branches the traversal of the state
trie from top to bottom 3-ways.
2. Leaf node. These are 3 vertical lines of 63 coloured boxes.

Each type of trie nodes can be serialised (using RLP encoding), to convert it to a string of bytes. What we see in
the values of the records in the "Hashes" bucket just above are the RLP-encodings of these 4 trie nodes.
What we see in the keys of these records are the results of `Keccak256` function applied to the values. In a way,
this is similar to the "Preimages" bucket, with the different type of values.

If you look closely, you may notice that the keys of the last 3 records are actually contained inside the value
of the first record. This is because the first value corresponds to that 3-way branch node, and the hashes of the
leaf nodes are used like "pointers" to those nodes. Continuing the "pointer" analogy, you can say that
"dereferencing" these pointers mean fetching the corresponding records from this "Hashes" bucket. Using such
"derederencing" process, one can traverse the state trie from the top to any leaf at the bottom. Each step in
such traversal requires finding the corresponding record in the "Hashes" bucket.


Block 1 in go-ethereum
----------------------

Now we will create the same Transaction for go-ethereum.

![geth_block1_db](geth_changes_1.png)

The buckets "Receipts", "Headers", "Config", "Last Header", "Last Fast", "Last Block", all look identical
to those in the Erigon database. We will walk through the ones that are different.

In the bucket "Block Bodies", the value is slightly different:

![geth_block1_block_bodies](geth_changes_1_b_4.png)

The difference is that the block body is missing the sender address since the send in go-ethereum is computed and in Erigon is stored. 
For the rest in this case the same changes occurs in terms of block number and block hash.

![geth_block1_hashes](geth_changes_1_hashes_5.png)

First of all a new branch node has been generated for the block. 
That's the first record because it contains in its value the key of the other key records. 
The three leaf nodes contains the `Keccak256` of the change in states of the account of the recipient, 
sender and miner of the block.
