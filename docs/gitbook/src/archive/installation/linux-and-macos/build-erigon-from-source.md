---
description: How to build Erigon in Linux and MacOS from source
---

# Build from source

{% hint style="warning" %}
#### ⚠️ Warning: Installing Erigon from Source

Installing Erigon directly from source requires a strong foundation in Command Line Interface (CLI) operations and Linux/Unix environments.

This method is significantly more difficult than using [pre-built binaries](pre-built-binaries.md) or [Docker](../docker.md) images. You will be responsible for resolving dependency issues, configuring build tools, and manually managing all compilation and execution steps.

If you are a casual user or lack CLI expertise, we strongly recommend using the official pre-built binaries or Docker documentation.
{% endhint %}

## 1. Software Requirements

If you intend to build Erigon from source, you must first meet the necessary prerequisites.

{% tabs fullWidth="false" %}
{% tab title="Linux" %}
#### Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit [https://git-scm.com/downloads](https://git-scm.com/downloads).

#### Build essential and Cmake

Install **Build-essential** and **Cmake**:

```bash
sudo apt install build-essential cmake -y
```

#### Go Programming Language

Erigon utilizes Go (also known as Golang) version 1.24 or newer for part of its development. It is recommended to have a fresh Go installation. If you have an older version, consider deleting the `/usr/local/go` folder (you may need to use `sudo`) and re-extract the new version in its place.

To install the latest Go version, visit the official documentation at [https://golang.org/doc/install](https://golang.org/doc/install).

#### C++ Compiler

This turns the C++ part of Erigon's code into a program your computer can run. You can use either **Clang** or **GCC**:

* For **Clang** follow the instructions at [https://clang.llvm.org/get\_started.html](https://clang.llvm.org/get_started.html);
* For **GCC** (version 10 or newer): [https://gcc.gnu.org/install/index.html](https://gcc.gnu.org/install/index.html).
{% endtab %}

{% tab title="macOS" %}
#### Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit [https://git-scm.com/downloads](https://git-scm.com/downloads).

#### Go Programming Language

Erigon utilizes Go (also known as Golang) version 1.24 or newer for part of its development. It is recommended to have a fresh Go installation. If you have an older version, consider deleting the `/usr/local/go` folder (you may need to use `sudo`) and re-extract the new version in its place.

To install the latest Go version, visit the official documentation at [https://golang.org/doc/install](https://golang.org/doc/install).

#### C++ Compiler

This turns the C++ part of Erigon's code into a program your computer can run. You can use either **Clang** or **GCC**:

* For **Clang** follow the instructions at [https://clang.llvm.org/get\_started.html](https://clang.llvm.org/get_started.html);
* For **GCC** (version 10 or newer): [https://gcc.gnu.org/install/index.html](https://gcc.gnu.org/install/index.html).
{% endtab %}
{% endtabs %}

## 2. Building Erigon from Source

The basic Erigon configuration is suitable for most users who simply want to run a node. To ensure you are building a specific, stable release, use Git tags.

{% stepper %}
{% step %}
#### Clone the Erigon repository

First, clone the Erigon repository (you do not need to specify a branch):

{% include "../../../.gitbook/includes/git-clone-https-github.co....md" %}
{% endstep %}

{% step %}
#### Check Out the Desired Stable Version (Tag)

Next, fetch all available release tags:

{% include "../../../.gitbook/includes/git-fetch-tags.md" %}

Check out the desired version tag by replacing `<tag_name>` with the version you want. Normally latest stable version is the best, check the official [Release Notes](https://github.com/erigontech/erigon/releases). For example:

{% include "../../../.gitbook/includes/git-checkout-version.md" %}
{% endstep %}

{% step %}
#### Compile the Software

Compile the Erigon binary using the `make` command.

Standard Compilation:

```bash
make erigon
```

Fast Compilation (Recommended): to significantly speed up the process, specify the number of processors you want to use with the `-j<n>` option, where `<n>` is the number of processor you want to use (we recommend using a number slightly less than your total core count):

```bash
make -j<n> erigon
```
{% endstep %}
{% endstepper %}

The resulting executable binary will be created in the `./build/bin/erigon` path.

## 3. Running Erigon

After installation, you can run Erigon from your terminal:

```bash
./build/bin/erigon [options]
```

See Basic Usage for more info.

{% content-ref url="../../../get-started/fundamentals/basic-usage.md" %}
[basic-usage.md](../../../get-started/fundamentals/basic-usage.md)
{% endcontent-ref %}
