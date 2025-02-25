<h1>Context</h1>

This is ported from https://github.com/shutter-network/shutter/tree/main/shlib/shcrypto to
avoid introducing a go-ethereum transitive dependency in Erigon.

In the long term, we can work with the shutter team to remove their dependency on go-ethereum
for their `shcrypto` package (doable). When that is done we will be able to simply use their
library directly.
