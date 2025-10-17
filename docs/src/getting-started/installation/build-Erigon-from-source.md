# Build Erigon from source
*How to build Erigon in Linux and MacOS from source*

The basic Erigon configuration is suitable for most users who simply want to run a node. To build the latest stable release, use the following command:

```bash
git clone --branch release/3.1 --single-branch https://github.com/erigontech/erigon.git
cd erigon
```

Next, compile the software using:

```bash
make erigon
```

To speed up the compilation process, you can specify the number of processors to use with the `-j<n>` option, where `<n>` is the number of processors you want to utilize. For example, if your machine has 22 processors and you want to use 20 of them, you can run:

```bash
make -j20 erigon
```

This will create the binary at `./build/bin/erigon`.