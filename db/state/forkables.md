# Forkables

it's a way to store structures like blocks, headers, checkpoints etc. in TemporalDb.

## Unmarked forkable it's in usable condition

- It maps `uint64 -> bytes`
- `Num` and `RootNum` are primary key and secondary(sharding) key.
    - primary key is the identifier of the entity, while secondary key is an identifier that relates the entity to an aggregate (e.g. consensus structure)
    - example: in bor checkpoints, checkpointId is `Num`; while blockNumber is `RootNum`

- How to write to unmarked forkable (see forkable_interface.go):
    - you can use `Append` to put the data directly in mdbx
    - or via a `BufferedWriter`, which is similar to how SharedDomains uses domainWriters...

- did an experiment of implementing rcache as forkables (instead of domain), and it worked fine. The implementation was efficient as well.
    - [PR for rcache as forkables](https://github.com/erigontech/erigon/pull/17094/files)

### data in unmarked forkable

it needs to handle 2 kinds of data:

- primary key with no gaps (continuous)
- primary key with gaps

both give rise to different kinds of files...

#### no gaps (continuous) data

- in this case, we don't need to store any Num in .v file + there's no need for inverted index
- Index can be created simply by going over the .v file and incrementing from the "first Num in file" (i++)

optimization: a better index for this would be to just have ef of offsets into .v (let's call it .idxef file). No recsplit is needed. It's simply a ef file of offsets

#### primary key with gaps

- if size(data) is much greater than the key (.eg rcache..where key is 8 bytes, and value is >100bytes for 1 log + 1 topic), `PagedWriter` can be used (`pageSize`>1). This can be set by `ForkableCfg.ValuesOnCompressedPage` (For rcache history right now, this value is 16).
- with above, even if there are gaps in the data, we can create the index because .v contains keys as well. Furthermore, we only need to store `1/pageSize` the amount of keys in the index, because we only need to find the page where the key resides, then we can load & decompress the page and then seek into it. (`ProtoForkableTx#GetFromFile`). This leads to much smaller recsplit indexes.
- the case where data has gaps + data size is comparable to key size -- this scenario will be inefficient to store in the forkable. This is because inverted index (ef) is a better structure to store the `Num` with gaps. I haven't checked how much different it makes, but I think it can be made better.

## Marked forkable

- this is for structures like blocks or headers, which have two tables: CanonicalHash table, which maps `Num -> canonical hash`, and then a Values table, which maps `Num+CanonicalHash -> value`...
- lower level details of marked forkable is implemented (forkable.go), but it hasn't been floated up to kv_interface.go yet  
