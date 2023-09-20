#Changesets encoding
## Storage changeset encoding 
Storage encoding contains several blocks: Address hashes, Incarnations, Length of values, Values. AccountChangeSet is serialized in the following manner in order to facilitate binary search
### Address hashes
There are a lot of address hashes duplication in storage changeset when we have multiple changes in one contract. To avoid it we can store only unique address hashes.
First 4 bytes contains number or unique contract hashes in one changeset.
Then we store address hashes with sum of key hashes from first element.

For example: for `addrHash1Inc1Key1, addrHash1Inc1Key2, addrHash2Inc1Key1, addrHash2Inc1Key3` it stores
`2,addrHash1,2,addrHash2,4`  
### Incarnations
Currently, there are a few not default incarnations(!=1) in current state. That was the reason why we store incarnation only if it's not equal to fffffffe(inverted 1).
First part is 4 byte that contains number of not default incarnations
Then we store array of id of address hash(4 byte) plus incarnation(8 byte)
For example: for `addrHash1fffffffe..., addrHash1fffffffd...` it stores
`1,1,fffffffd`

### Values lengths
The default value length is 32(common.Hash), but if we remove leading 0 then average size became ~7. Because a length of value could be from 0 to 32 we need this section to be able to find quite fast value by key.
It is contiguous array of accumulating value indexes like `len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{N-1})`
To reduce cost of it we have three numbers: numOfUint8, numOfUint16, numOfUint32. They can answer to the question: How many lengths of values we can put to uint8, uint16, uint32.
This number could be huge if one of the contracts was suicided during block execution. Then we could have thousands of empty values, and we are able to store them in uint8(but it depends).
For example for values: "ffa","","faa" it stores `3,0,0,3,3,6`

### Values
Contiguous array of values.

### Finally
Value | Type | Comment
------------ | ------------- | -------------
numOfUniqueElements | uint32 |
Address hashes | [numOfUniqueElements]{[32]byte+[4]byte}  | [numOfUniqueElements](common.Hash + uint32) 
numOfNotDefaultIncarnations | uint32 | mostly - 0
Incarnations |  [numOfNotDefaultIncarnations]{[4]byte + [8]byte}  | []{idOfAddrHash(uint32) + incarnation(uint64)}
Keys | [][32]byte | []common.Hash
numOfUint8 | uint32 | 
numOfUint16 | uint32 |  
numOfUint32 | uint32 | 
Values lengths in uint8 | [numOfUint8]uint8 | 
Values lengths in uint16 | [numOfUint16]uint16 | 
Values lengths in uint32 | [numOfUint32]uint32 | 
Values | [][]byte | 




## Account changeset encoding
AccountChangeSet is serialized in the following manner in order to facilitate binary search. Account changeset encoding contains several blocks: Keys, Length of values, Values. Key is address hash of account. Value is CBOR encoded account without storage root and code hash.

### Keys
The number of keys N (uint32, 4 bytes)
Contiguous array of keys (N*32 bytes)
### Values lengthes
 Contiguous array of accumulating value indexes:
len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{N-1})
(4*N bytes since the lengths are treated as uint32).
### Values
Contiguous array of values.
### Finally
Value | Type | Comment
------------ | ------------- | -------------
num of keys | uint32 |
address hashes | [num of keys][32]byte | [num of keys]common.Hash
values lengthes | [num of keys]uint32
values | [num of keys][]byte
