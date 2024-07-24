================================
Block Bodies Compression: Snappy
================================

Issue
=====
Block bodies represent the biggest portion of the database as a bucket and we need to make such space more efficient. Thus, we use compression through snappy.

Snappy
======

Snappy is a compression/decompression library. It does not aim for maximum compression, or compatibility with any other compression library; instead, it aims for very high speeds and reasonable compression. For instance, compared to the fastest mode of zlib, Snappy is an order of magnitude faster for most inputs, but the resulting compressed files are anywhere from 20% to 100% bigger. On a single core of a Core i7 processor in 64-bit mode, Snappy compresses at about 250 MB/sec or more and decompresses at about 500 MB/sec or more.

Why Snappy
==========

In the case of Erigon, compression is needed to reduce database size, however we cannot have slow processing while doing so. thanks, to snappy we can reduce the size of block bodies without having to sacrifice a significant amount of time during the insertion of block bodies in the database in stage 2.
