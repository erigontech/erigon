# Software Requirements

If you plan to compile Erigon from source, ensure that the following prerequisites are met.

Erigon works only from command line interface (CLI), so it is advisable to have a good confidence with basic commands.

> Building software from source can be complex. If you're not comfortable with technical tasks, we recommend you use other installation methods like pre-built images or Docker and skip these requirements.


### Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit <https://git-scm.com/downloads>.


### Build essential (only for Linux)

Install **Build-essential** and **Cmake**:

```bash
sudo apt install build-essential cmake -y
```

### Go Programming Language

Erigon utilizes Go (also known as Golang) version 1.24 or newer for part of its development. It is recommended to have a fresh Go installation. If you have an older version, consider deleting the /usr/local/go folder (you may need to use sudo) and re-extract the new version in its place.

To install the latest Go version, visit the official documentation at [https://golang.org/doc/install](https://golang.org/doc/install).

### C++ Compiler

This turns the C++ part of Erigon's code into a program your computer can run. You can use either **Clang** or **GCC**:

- For **Clang** follow the instructions at [https://clang.llvm.org/get_started.html](https://clang.llvm.org/get_started.html);
- For **GCC** (version 10 or newer): [https://gcc.gnu.org/install/index.html](https://gcc.gnu.org/install/index.html).

You can now proceed with Erigon [installation](installation.md).