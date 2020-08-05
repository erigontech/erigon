# Block header download process
This process is a collection of data structures and algorithms to perform the task of obtaining a consistent and verified header chain from peers over the network.
## Data structures
All data structures, except for the files, are kept in operating memory (RAM), and do not survive an interruption, a crash, or a powerdown. Therefore,
there is `RECOVER` algorithm described later, which initialises all transitent data structures from the files.
### The buffer
It is an append-only collection of headers, without any extra indices. The buffer is of a limited configurable size.
Apart from block headers, the buffer may contain another type of entries that we will call "tombstones". The tombstones mark certain block hashes as
undesirable (usually because they are part of of a fake chain produced for the purpose of frustrating the download process). Like ordinary header entries,
the tombstones contain block height information, so that they can be sorted together with the headers. As will be mentioned later, this sorting is performed
when the buffer is full and needs to be flushed into a file. Presence of tombstones allows skipping all the descendant headers of any tombstone, and not
persisting them into the files. This happens when an undesirable header is detected before its decendants were flushed into a file.
Otherwise, it is possible that some undesirable headers will get into the files, and they will need to be ignored when the files are getting processed and
entries committed to the database.
### The files
Once the buffer reaches its maximum size (such that adding anorher header would cause the buffer to exceed its size limit), the entries in the buffer are
sorted by the block height (and in the case of ties, by the block hash), in ascending order. Once sorted, the content of the buffer is flushed into a new
file, and the buffer is cleared. The files are supposed to persist in the event of an interruption, a crash, or a powerdown. The files are deleted once
the block headers have been successfully committed to the database, or they can be deleted manually in case of a corruption.
### Bad headers
Set of block hashes that are known to correspond to invalid or prohibited block headers. The bad headers set can be initialised with hard-coded values.
Not every invalid header needs to be added to the bad headers set. If the invalidity of a block header can be found just by examining at the header
itself (for example, malformed structure, excessive size, wrong Proof Of Work), such block header must not be added to the bad headers set. Only if
the evidence of the invalidity is not trivial to obtain, should a block header be added. For example, if a block header has valid Proof Of Work, but
it is a part of a header chain that leads to a wrong genesis block. Another example is a blocl header with correct Proof Of Work, but incorrect
State Root hash.
Newly found bad block headers need to be immediately written to the database, because the cost of discovering their invalidity again needs to be avoided.
### Chain segment
At any point in time, the headers that have been downloaded and placed into the buffer or into one of the files, can be viewed as a collection of chain
segments like this:

![header_download_1](header_download_1.png)

The reason why we may need to maintain multiple chain segments instead of just one, is this potential optimisation. If we have some hindsight knowledge
about the header chain that is being downloaded, we can hard-code the hashes of block headers at some intermediate block heights. For example, at every
height which is multiple of 4096. Assuming that the maximum height is on the order of tens of millions, we only need to hard-code around a thousand
32-byte values (block hash), together with total difficulty number (maximum 32 byte), which is trivial. Having these hard-coded anchors, we can
download different chain segments from different peers to better utilise the network connectivity.
Each segment has exactly one anchor. Every anchor maintains an integer attribute `powDepth`, meaning how "deep" after this anchor the ancestral block headers
need their Proof Of Work verified. Value `0` means that there is no need to verify Proof Of Work on any ancestral block headers. All hard-coded anchors come
with `powDepth=0`. Other anchors (created by announced blocks or block hashes), start with `powDepth` equal to some configured value (for example, `65536`,
which is equivalent to rought 16 days of block time), and every time the chain segement is extended by moving its anchor "down", the `powDepth` is also
decreased by the height of the extension.

![header_download_2](header_download_2.png)

Eventually, the growing chain segement can reach the configured size, at which point the `powDepth` attribute of its anchor will drop to `0`, and no
further Proof Of Work verification on the ancestors will be performed. Another possibility is that the anchor will match with a tip of another chain
segment, the chain segments will merge, and the anchor simply disappears, superseeded by the anchor of another chain segment.
There is also a potential optimisation, whereby the extension of a chain segment from the tip shall also decrease the `powDepth` value of its anchor.
However, we currently leave this for future editions.