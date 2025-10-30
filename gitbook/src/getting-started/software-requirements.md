---
description: 'Source Code Compilation: Required Tools (Git, Go, and Compilers)'
---

# Software Requirements

If you intend to <mark style="color:$primary;">build Erigon from source</mark>, you must first meet the necessary prerequisites.

However, if you choose to use the simpler **Docker** or **pre-built image** installation methods, you can ignore these requirements. Building from source is a technical task, so we recommend the other methods if you are not comfortable with command-line compilation.

{% tabs %}
{% tab title="Linux" %}
### Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit [https://git-scm.com/downloads](https://git-scm.com/downloads).

### Build essential and Cmake

Install **Build-essential** and **Cmake**:

```bash
sudo apt install build-essential cmake -y
```

### Go Programming Language

Erigon utilizes Go (also known as Golang) version 1.24 or newer for part of its development. It is recommended to have a fresh Go installation. If you have an older version, consider deleting the `/usr/local/go` folder (you may need to use `sudo`) and re-extract the new version in its place.

To install the latest Go version, visit the official documentation at [https://golang.org/doc/install](https://golang.org/doc/install).

### C++ Compiler

This turns the C++ part of Erigon's code into a program your computer can run. You can use either **Clang** or **GCC**:

* For **Clang** follow the instructions at [https://clang.llvm.org/get\_started.html](https://clang.llvm.org/get_started.html);
* For **GCC** (version 10 or newer): [https://gcc.gnu.org/install/index.html](https://gcc.gnu.org/install/index.html).
{% endtab %}

{% tab title="macOS" %}
### Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit [https://git-scm.com/downloads](https://git-scm.com/downloads).

### Go Programming Language

Erigon utilizes Go (also known as Golang) version 1.24 or newer for part of its development. It is recommended to have a fresh Go installation. If you have an older version, consider deleting the `/usr/local/go` folder (you may need to use `sudo`) and re-extract the new version in its place.

To install the latest Go version, visit the official documentation at [https://golang.org/doc/install](https://golang.org/doc/install).

### C++ Compiler

This turns the C++ part of Erigon's code into a program your computer can run. You can use either **Clang** or **GCC**:

* For **Clang** follow the instructions at [https://clang.llvm.org/get\_started.html](https://clang.llvm.org/get_started.html);
* For **GCC** (version 10 or newer): [https://gcc.gnu.org/install/index.html](https://gcc.gnu.org/install/index.html).
{% endtab %}

{% tab title="Windows" %}
Chocolatey package manager or WSL 2 depending on the chosen installation metho&#x64;_._ See [Windows](installation/windows.md) installation section for detailed instructions.
{% endtab %}
{% endtabs %}
