Database Walkthrough
==============================

This document attempts to explain how Turbo-Geth organises its persistent data in its database,
how this organisation is different from go-ethereum, the project from which it is derived.
We start from a very simple genesis block, and then apply 7 blocks, each containing either
ETH transfers, or interactions with smart contracts, or both. For each step, we use visualisation
produced by the code available in turbo-geth, and the code which currently resides on a fork
of go-ethereum, but there is an intention to feed it upstream, if there is interest.

Genesis in Turbo-Geth
------------------------------
For the genesis block, we generate 3 different private keys and construct Ethereum addresses from them.
Then, we endow one of the accounts with 9 ETH, and two others with 0.2 and 0.3 ETH, respectively.
This is how the initial state trie looks like:

![genesis_state](state_0.dot.gd.png)

In this, and other illustrations, the colored boxes correspond to hexadecimal digits (a.k.a nibbles), with values 0..F.
Here is the palette:

![hex_palette](hex_palette.dot.gd.png)

First thing to note about the illustration of the state trie is that the leaves correspond to our accounts
with their ETH endowments. Account nonces, in our case all 0s, are also shown. If you count number of coloured
boxes you can to go through top to bottom to each any of the account leaves, you will get 64. If each nibble occupies
half of a byte, that makes each "key" in the state trie 32 bytes long. But account addresses are only 20 bytes long.
The reason why we get 32 and not 20 is that all the keys (in our case account addresses) are processed by `Keccak256`
hash function (which has 32 byte output) before they are inserted into the trie.
If we wanted to see what the corresponding account addresses were, we will have to look into the database. Here is
what turbo-geth would persist after generating such a genesis block:

![genesis_db](changes_0.dot.gd.png)

The database is organised in buckets (or some of you may be more used to the term "tables"). The first bucket
contains "Preimages". By preimage here we mean the reverse of the `Keccak256` function. Lets zoom into it

![genesis_db_preimages](changes_0_secure-key-_11.dot.gd.png)

If you now look closely at the
keys stored in this bucket (the strings of coloured boxes with digits in them, on the left of the connecting lines),
and compare them with the paths you have to walk top to bottom to our accounts in the genesis state, you will see that they
match. And the corresponding values (the strings of coloured boxes on the right of the connecting lines) are the
account addresses - they are 40 nibbles, or 20 bytes long.

The next bucket, "Receipts", records the list of transaction receipts for each block:

![genesis_db_receipts](changes_0_r_10.dot.gd.png)

The first 8 bytes of the key (or 16 nobbles, equaling to 0s here) encode the block number, which is 0 for the Genesis block.
The remaining 32 bytes of the key encode the block hash. The value is the RLP-encoded list of receipts. In our case, there were
no transactions in the Genesis block, therefore, we have RLP encoding of an empty list, `0xC0`.

Next bucket is "Transacqtion index":

![genesis_db_history_of_accounts](changes_0_hAT_9.dot.gd.png)

Next bucket is "History Of Accounts":

![genesis_db_history_of_accounts](changes_0_hAT_9.dot.gd.png)

The three keys here are almost the same as the keys in the "Preimages" bucket, and the same as
the keys in the state trie. There is a suffix `0x20` added to all of them. It encodes the block
number 0, in a turbo-geth-specific encoding. Three most significant bits (`001` here) encode the
total length (in bytes) of the encoding. The rest of the bits encode the number.
The absense of values in the illustrations signifies that the values are empty strings.
The history of accounts records how the accounts changed at each block. But, instead of recording,
at each change, the value that the accounts had AFTER the change, it records what value the accounts
had BEFORE the change. That explains the empty values here - it records the fact that these
three accounts in questions did not exist prior to the block 0.

**WARNING** The layout of this bucket might change very soon, and it will only contain the
timeseries of block numbers in which particular account has changed.

Next bucket is "Headers", it records information about block headers from various angles.

![genesis_db_headers](changes_0_h_8.dot.gd.png)

The keys for the first two records start with 8-byte encoding of the block number (0), followed by
the block hash (or header hash, which is the same thing). The second record also has a suffix `0x74`,
which is ASCII code for `t`. The records of the first type store the actual headers in their values.
The records of the second type store total mining difficulty (TD) of  the chain ending in that specific header.
In our case it is `0x80`, which is RLP encoding of 0.
The records of the third type have their keys composed of 8-byte encoding of the block number (0 here), and
suffix `0x6E`, which is ASCII code for `n`. These records allow lookuping up header/block hash given block
number. They are also called "canonical header" records, because there might be multiple headers for given
block number, and only one of them is deemed "canonical" at a time.

Next bucket is "Block Bodies":

![genesis_db_block_bodies](changes_0_b_6.dot.gd.png)

The keys in this bucket are concatenations of 8-byte encoding of the block number and 32-byte block hash.
The values are RLP-encoded list of 3 structures:
1. List of transactions
2. List transaction sender addresses, one for each transaction in the first list
3. List of ommers (a.k.a uncles)
In the case of the genesis block, all of these lists are empty (RLP encodings `0xC0`), and the prefix `0xC3` means
in RLP "some number of sub-structures with the total length of 3 bytes".

The reason turbo-geth keeps the list of transaction sender addresses for each transaction has to do with the fact that
transactions themselves do not contain this information directly. Sender's address can be "recovered" from the digital
signature, but this recovery can be computationally intensive, therefore we "memoise" it.

Next three buckets, "Last Header", "Last Fast", and "Last Block", always contain just one record each, and their
keys are always the same, the ASCII-encodings of the strings `LastHeader`, `LastFast`, and `LastBlock`, respectively.
The values record the block/header hash of the last header, receipt or block chains that the node has managed to sync
from its peers in the network. The value in "Last Fast" bucket is not really used at the moment, because turbo-geth
does not support Fast Sync.

Next bucket, "Header Numbers", is a mapping of 32-byte header/block hashes to the corresponding block numbers (encoded
in 8 bytes):

![genesis_db_header_numbers](changes_0_H_2.dot.gd.png)

Bucket "Change Sets" record the history of changes in accounts and contract storage. But, unlike in "History of Accounts"
and "History of Storage" (this bucket will appear later), where keys are derived from accounts' addresses
(key = address hash + block number), in the "Change Sets" bucket, keys are derived from the block numbers:

![genesis_db_change_sets](changes_0_ChangeSet_1.dot.gd.png)

In the cases of our genesis block, the keys is composed from the encoding of the block number (`0x20`), and the
ASCII-code of `hAT` (meaning **h**istory of **A**counts **T**rie).
The "Change Set" bucket records changes that happen to accounts and contract storage slots at every block.
It is important to node that the values recorded in the "Changes Set" bucket are not the values the accounts
(or storage slots) had AFTER the change, it records what value the accounts (or storage slots)
had BEFORE the change. That explains the empty values here - it records the fact that these
three accounts in questions did not exist prior to the block 0.
The encoding of the values in the records is tailored for fast access and binary search. It has 5 parts:
1. Number of keys-value pairs, encoded as a 4-byte (32-bit) number. In this example, it is `0x00000003`, which means
there are 3 key-value pairs
2. Size of each key, also encoded as a 32-bit number. All keys are the same size, which makes it possible to
access them without deserialisation. In this example, it is `0x00000020`, which 32, meaning that all keys are
32 bytes long.
3. Keys themselves. In our examples, these are the coloured boxes before the streak of white 0s. Keys are sorted
lexicographically. This, together with the keys being the same size, allows binary search without desearialision,
as well as linear-time merge of multiple changesets.
4. Value offsets. These offsets mark the beginning of the next, 5th part as offset 0. First value has offset 0.
In our example, all values are empty strings, therefore we see 3 zero offsets (24 white boxes with zeros in them).
5. Values themselves. In our example, they are empty, so this 5th part is not present.

The next bucket is "Accounts":

![genesis_db_accounts](changes_0_AT_0.dot.gd.png)

The keys of the records in this bucket are the same as in the bucket "Preimage", and they are also the same as
the keys in the state trie. The values are the current state of each account. In this special case, all three
accounts are RLP-encoded as the list of 3 nested structures. Lets take the first value and analyse.
Prefix `0xCB` means that it is an RLP structure (list) of some number of nested structures with the total length
of 11 bytes (`B` is 11). Then, we can see 3 nested structures following: `0x80`, `0x887ce66c50e2840000`, and `0x80`.
The first one encodes nonce, which is 0, the second one encodes balance. Prefix `0x88` means "byte array 8 bytes long".
The value `0x7ce66c50e2840000` is the hexadecimal value for... .Start python and do this:
````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x7ce66c50e2840000
9000000000000000000
````
Which is 9 followed by 18 zeros, which is 9 ETH (1 ETH = 10^18 wei).

The third `0x80` is the encoding of accounts' incarnation. Incarnation is a turbo-geth specific attribute, which is
used to make removal and revival of contract accounts (now possible with `CREATE2` since Constantinopole) efficient
with turbo-geth's database layout. For now it will suffice to say that all non-contract accounts will have 
incarnation 0, and all contract accounts will start their existence with incarnation 1.

Contract accounts may also contract code hash and storage root, and these two pieces of information would make the record
in the "Accounts" bucket contain 5 instead of 3 fields.

Genesis in go-ethereum
------------------------------

Now we will create the same Genesis state and block in go-ethereum (in archive mode to make sure we compare like for like).
Here is how the database looks like. Since go-ethereum uses LevelDB, and LevelDB does not have a concept of "Buckets" (or
"Tables"), go-ethereum emulates them by adding table-specific prefixes to all the keys, with the exception of the keys that
describe the state trie (bucket "Hashes" in our example). In the illustration, these prefixes are mostly removed for better
comparison with turbo-geth. They were not removed only for the buckets "LastBlock", "LastHeader" and "LastFast", because
othewise they key would be empty.

![geth_genesis_db](geth_changes_0.dot.gd.png)

The buckets "Preimages", "Receipts", "Headers", "Config", "Last Header", "Last Fast", "Last Block", all look identical
to those in the turbo-geth database. We will walk through the ones that are different.

In the bucket "Block Bodies", the value is slightly different:

![geth_genesis_block_bodies](geth_changes_0_b_5.dot.gd.png)

The difference is that the block body has 2 elements instead of 3 in turbo-geth. The missing element is the list
of the sender addresses that go-ethereum does not store, but recomputes after loading or caches in memory.

The buckets "Accounts", "History Of Accounts", and "Change Sets" are missing, because go-ethereum uses a very
different mechanism for storing the state and its history:

![geth_genesis_hashes](geth_changes_0_hashes_0.dot.gd.png)

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
of the first record. This is because the first value correponds to that 3-way branch node, and the hashes of the
leaf nodes are used like "pointers" to thoese nodes. Continuing the "pointer" analogy, you can say that
"dereferencing" these pointers mean fetching the corresponding records from this "Hashes" bucket. Using such
"derederencing" process, one can traverse the state trie from the top to any leaf at the bottom. Each step in
such traversal requires finding the corresponding record in the "Hashes" bucket.

Block with 1 Ethereum transaction in Turbo-Geth
------------------------------
When the first block is inserted, we sign a transaction using the private key of the account with 9 ETH to address 0x1 with 0.001 ETH and then execute it.
This is how the state trie after Block 1 is inserted looks like:

![block1_state](state_2.dot.gd.png)

First thing to note about the illustration of the state trie is that the nonce of the first account is now 1 since a transaction from that address has been executed and the amount of ETH of that account is now 8.999 since the transaction had a value of 0.001 ETH. Additionally, a new account has been generated in the state trie. it's the recipient of the transaction. In fact, it has only 0.001 ETH (the value of the transaction) and it's nonce is set to 0.
how turbo-geth's database after such a block would be generated:

![block1_db](changes_1.dot.gd.png)

We have got now two more preimages:
* the preimage of the recipient of the transaction (account 0x0)
* the preimage of the signer

![block1_db_preimages](changes_1_secure-key-_11.dot.gd.png)

This happened because when the transaction was sent, a new entry for the account of the recipient (that didn't exist before) was generated and along with it his preimage.

![block1_db_receipts](changes_1_r_10.dot.gd.png)

The first 8 bytes of the key (or 16 nobbles, equaling to 0s here) encode the block number, which value increased to 1 in this case (the first 15 nibbles are 0s and the 16th nibble is 1);
The remaining 32 bytes of the key encode the block hash. The value is the RLP-encoded list of receipts. In our case, there a transaction was sent and as a matter of fact, instead of having `0XC0` (that repressent an empty list) we have `0xcbc501825208c0` that his the receipt for the 0.001 ETH transaction.

Next bucket is "History Of Accounts":

![block1_db_history_of_accounts](changes_1_hAT_8.dot.gd.png)

First of all, a new entry has been added and is the concatenation of the preimage of the account that sent the transaction with the block suffix. the first thing to notice is that the new entry has a block suffix of `0x21` instead of `0x20` since the change didn't occur in block Additionally, now the key has a value associated since before the changes the account changed existed actually existed. 

The value has the following meaning:

* the first two nibbles are the RLP prefix `0xCB`
* the following two nibbles repressent the nonce of the transaction `0x80` (0)
* the next 18 nibble is the balance BEFORE the transaction of the address `0x887ce66c50e2840000` 
  * the balance has a prefix of `0x88` that indicate there are 8 bytes ahead
  * the balance without the suffix is `0x7ce66c50e2840000` that is 9000000000000000000 WEI or 9.00 ETH.

**Note** The history of accounts records how the accounts changed at each block. But, instead of recording,
at each change, the value that the accounts had AFTER the change, it records what value the accounts
had BEFORE the change.

Next bucket is "Headers"

![block1_db_headers](changes_1_h_7.dot.gd.png)

A new headers has been generated since a new block has been mined.
As a matter of fact, In the keys for the first two records start with a block number of 1 followed by
the block hash (or header hash, which is the same thing) that is different than the Genesis Block. Additionally, these two records for the new block that respectively repressent the values in the headers and the mining difficulty differs from the Genesis Block.

Next bucket is "Block Bodies":

![block1_db_block_bodies](changes_1_b_6.dot.gd.png)

The keys in this bucket are concatenations of 8-byte encoding of the block number and 32-byte block hash.
The new record is the repressentation of block 1.

![genesis_db_change_sets](changes_1_ChangeSet_1.dot.gd.png)

Bucket "Change Sets" recorded the change in account that occurred BEFORE the 0.001 ETH transaction. you can notice that unlike the genesis block, this time the key has a prefix of `0x21` since now the blockchain is 1 block apart from the Genesis block.

**Note**
it records what value the accounts (or storage slots)
had BEFORE the change. In fact, it just store the existence of the address generated AFTER the genesis block.

**Reminder**
The encoding of the values in the records is tailored for fast access and binary search. It has 5 parts:
1. Number of keys-value pairs, encoded as a 4-byte (32-bit) number. In this example, it is `0x00000003`, which means
there are 3 key-value pairs
2. Size of each key, also encoded as a 32-bit number. All keys are the same size, which makes it possible to
access them without deserialisation. In this example, it is `0x00000020`, which 32, meaning that all keys are
32 bytes long.
3. Keys themselves. In our examples, these are the coloured boxes before the streak of white 0s. Keys are sorted
lexicographically. This, together with the keys being the same size, allows binary search without desearialision,
as well as linear-time merge of multiple changesets.
4. Value offsets. These offsets mark the beginning of the next, 5th part as offset 0. First value has offset 0.
In our example, all values are empty strings, therefore we see 3 zero offsets (24 white boxes with zeros in them).
1. Values themselves. The values are the concated list of the accounts keys that changed and the concatenated list of the accounts values associated.

The next bucket is "Accounts":

![genesis_db_accounts](changes_1_AT_0.dot.gd.png)

The values are the current state of each account. In this case the accounts are still RLP Encoded but the third differs a bit because instead of having `0xCB` as a prefix, it has `0xCA`. 
* in the first value we can see 3 nested structures following: `0x01`, `0x887ce2ded23dbd8000`, and `0x80`.
The first one encodes nonce, which is 1, since this address is the sender of the transaction, the second one encodes balance. Prefix `0x88` means "byte array 8 bytes long".
The value `0x7ce2ded23dbd8000` is the hexadecimal value for... .Start python and do this:
````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x7ce2ded23dbd8000
8999000000000000000
````
Which is 9 ETH - 0.001 ETH, this means that this account is the sender.
* in the second value we can see 3 nested structures following: `0x80`, `0x88`, and `0x80`.
The first one encodes nonce, which is 0, since this address is the miner of the block.
The value `0x1bc16d674ec80000` is the hexadecimal value for... .Start python and do this:
````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x1bc16d674ec80000
2000000000000000000
````
Which is 2 ETH that probably repressent the mining reward.

* in the third value we have prefix `0xCA`, meaning that this is still an RLP structure but has a length of 10 bytes instead of 11. we can see still see the 3 nested structures following: `0x80`, `0x87`, and `0x80`. you may notice that the prefix of the balance differs from the first two accounts as well. In fact, we have got `0x87` instead of `0x88`. This is because there 7 bytes needed for the balance instead of 8.
The balance is `0x038d7ea4c68000` is the hexadecimal value for... .Start python and do this:
````
$ python
Python 2.7.15
Type "help", "copyright", "credits" or "license" for more information.
>>> 0x038d7ea4c68000
1000000000000000
````
Which is 0.01 ETH that is exactly how much has been sent to the recipient address, meaning that this account repressent the recipient of the transaction.

Block 1 in go-ethereum
------------------------------

Now we will create the same Transaction for go-ethereum.

![geth_block1_db](geth_changes_1.dot.gd.png)

The buckets "Preimages", "Receipts", "Headers", "Config", "Last Header", "Last Fast", "Last Block", all look identical
to those in the turbo-geth database. We will walk through the ones that are different.

In the bucket "Block Bodies", the value is slightly different:

![geth_block1_block_bodies](geth_changes_1_b_4.dot.gd.png)

The difference is that the block body miss the sender address since the send in go-ethereum is computed and in turbo-geth is stored. for the rest in this case the same changes occurs in terms of block number and block hash.

![geth_block1_hashes](geth_changes_1_hashes_5.dot.gd.png)

First of all a new branch node has been generated for the block. that's the first record because it contains in its value the key of the other key records. The three leaf nodes contains the `Keccak256` of the change in states of the account of the recipient, sender and miner of the block.