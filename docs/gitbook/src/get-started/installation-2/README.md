---
description: Erigon Installation Options
---

# Installation

To install Erigon, begin by choosing an installation method suited to your operating system and technical preference. Options range from the simplest to the most complex, including pre-built images, containers, or source compilation.

{% tabs %}
{% tab title="Linux" %}
<table data-view="cards"><thead><tr><th></th><th></th></tr></thead><tbody><tr><td><i class="fa-play">:play:</i></td><td><ol><li><a href="./#pre-built-binaries-linux-only"><strong>Pre-built Binaries</strong></a></li></ol></td></tr><tr><td><i class="fa-docker">:docker:</i></td><td><ol start="2"><li><a href="./#docker"><strong>Docker</strong></a></li></ol></td></tr><tr><td><i class="fa-gear">:gear:</i></td><td><ol start="3"><li><a href="./#build-from-source"><strong>Build from Source</strong></a></li></ol></td></tr></tbody></table>
{% endtab %}

{% tab title="macOS" %}
<table data-view="cards"><thead><tr><th></th><th></th></tr></thead><tbody><tr><td><i class="fa-docker">:docker:</i></td><td><ol><li><a href="./#docker"><strong>Docker</strong></a></li></ol></td></tr><tr><td><i class="fa-gear">:gear:</i></td><td><ol start="2"><li><a href="./#build-from-source"><strong>Build From Source</strong></a></li></ol></td></tr></tbody></table>
{% endtab %}

{% tab title="Windows" %}
<table data-view="cards"><thead><tr><th></th><th></th></tr></thead><tbody><tr><td><i class="fa-docker">:docker:</i></td><td><ol><li><a href="./#docker"><strong>Docker</strong></a></li></ol></td></tr><tr><td><i class="fa-windows">:windows:</i></td><td><ol start="2"><li><a href="./#native-compilation"><strong>Native Compilation</strong></a></li></ol></td></tr><tr><td><i class="fa-linux">:linux:</i></td><td><ol start="3"><li><a href="./#window-subsystem-for-linux-wsl"><strong>Windows Subsystem for Linux (WSL)</strong></a></li></ol></td></tr></tbody></table>
{% endtab %}
{% endtabs %}

### All Operating Systems

<details>

<summary>Docker</summary>

Docker is like a portable container for software. It packages Erigon and everything it needs to run, so you don't have to install complicated dependencies on your computer.

This Docker image is fully supported on **Linux**, **macOS**, and **Windows**.

_(Note: The container itself is built on multi-platform Linux architectures (linux/amd64 and linux/arm64), which is handled automatically by your Docker setup.)_

### Prerequisites

[Docker Engine](https://docs.docker.com/engine/install) if you run Linux or [Docker Desktop](https://docs.docker.com/desktop/) if you run macOS/Windows.

#### General Info

* The Docker images feature several binaries, including: `erigon`, `downloader`, `evm`, `caplin`, `capcli`, `integration`, `rpcdaemon`, `sentry`, and `txpool`.
* The multi-platform Docker image is available for `linux/amd64/v2` and `linux/arm64` platforms and is now based on Debian Bookworm. There's no need to pull a different image for another supported platform.
* All build flags are now passed to the release workflow, allowing users to view previously missed build information in the released binaries and Docker images. This change is also expected to result in better build optimization.
* Docker images now contain the label `org.opencontainers.image.revision`, which refers to the commit ID from the Erigon project used to build the artifacts.
* With recent updates, all build configurations are now included in the release process. This provides users with more comprehensive build information for both binaries and Docker images, along with enhanced build optimizations.
* Images are stored at [https://hub.docker.com/r/erigontech/erigon](https://hub.docker.com/r/erigontech/erigon).

### Download and start Erigon in Docker

Here are the steps to download and start Erigon in Docker.

**1. Check which version you want to download**

Check in the GitHub [Release Notes](https://github.com/erigontech/erigon/releases) page which version you want to download (normally latest is the best choice).

**2. Download Erigon container**

Download the chosen version replacing `<version_tag>` with the actual version:

```sh
docker pull erigontech/erigon:<version_tag>
```

For example:

{% include "../../.gitbook/includes/docker-pull-erigontech-erig....md" %}

**3. Start the Erigon container**

Start the Erigon container in your terminal:

{% code overflow="wrap" %}
```sh
docker run -it erigontech/erigon:<version_tag> <flags>
```
{% endcode %}

For example:

{% code overflow="wrap" %}
```sh
docker run -it erigontech/erigon:v3.2.2 --chain=hoodi --prune.mode=minimal --datadir /erigon-data
```
{% endcode %}

* `-v` connects a folder on your computer to the container (must have authorization)
* `-it` lets you see what's happening and interact with Erigon
* `--chain=hoodi` specifies which [network](../fundamentals/supported-networks.md) to sync
* `--prune.mode=minimal` tells Erigon to use minimal [Sync Mode](../fundamentals/sync-modes.md)
* `--datadir` tells Erigon where to store data inside the container

</details>

### Linux/macOS

<details>

<summary>Pre-built Binaries (Linux Only)</summary>

### 1. Select Your Processor Architecture and Download

Go to the Erigon [releases page](https://github.com/erigontech/erigon/releases) on GitHub and select the latest stable version (e.g., <code class="expression">space.vars.version</code>) or whichever version you prefer.

<figure><img src="../../.gitbook/assets/image (12).png" alt=""><figcaption></figcaption></figure>

Download the appropriate binary file for your processor architecture:

<table data-header-hidden><thead><tr><th width="210.88885498046875"></th><th width="185"></th><th></th></tr></thead><tbody><tr><td><strong>Processor Type</strong></td><td><strong>Binary File Type</strong></td><td><strong>Example File Name</strong></td></tr><tr><td>64-bit Intel/AMD</td><td>Debian Package (<code>.deb</code>)</td><td>erigon_<code class="expression">space.vars.version</code>_amd64.deb</td></tr><tr><td>64-bit ARM</td><td>Debian Package (<code>.deb</code>)</td><td>erigon_<code class="expression">space.vars.version</code>_arm64.deb</td></tr><tr><td>64-bit Intel/AMD</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v<code class="expression">space.vars.version</code>_linux_amd64.tar.gz</td></tr><tr><td>64-bit Intel/AMDv2</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v<code class="expression">space.vars.version</code>_amd64v2.tar.gz</td></tr><tr><td>64-bit ARM</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v<code class="expression">space.vars.version</code>_linux_arm64.tar.gz</td></tr></tbody></table>

Note that the Release Page Assets table contains also the **checksum** for each file and a checksum file.

### 2. Verifying Binary Integrity with Checksums

To verify the integrity and ensure your downloaded Erigon file hasn't been corrupted or tampered with, use the SHA256 checksums provided in the official release by following these steps:

**2.1 Generate the Checksum of Your Downloaded File**

Next, use the `sha256sum` command followed by the name of your downloaded binary (e.g., `erigon_v3.x.x_linux_amd64.tar.gz`).

```sh
sha256sum <DOWNLOADED_FILE_NAME>
```

Example with a `tar.gz` file:

```sh
sha256sum erigon_v3.2.2_linux_amd64.tar.gz
```

This command will output a long string (the computed checksum) followed by the file name.

**2.2 Compare the Checksums**

Compare the checksum found in the previous step with those in the Release Notes Assets table or the <kbd>erigon\_v3.x.x\_checksums.txt</kbd> file in the same table.

The two checksum strings must match exactly. If they do not match, the file is corrupted, and you should delete it and download it again.

### 3. Installing the Binary Executable

After downloading and verifying the checksum, follow the instructions below based on the file type you chose.

**a. Using Debian Package (`.deb`)**

This method uses your distribution's package manager (like <kbd>dpkg</kbd>) to install Erigon system-wide.

1.  Navigate to the directory where you downloaded the pre-built binary, e.g. Downloads:

    ```bash
    cd ~/Downloads
    ```
2.  Install the package:

    ```bash
    sudo dpkg -i erigon_3.x.x_amd64.deb
    ```

    (Replace the filename with your downloaded version)

**b. Using Compressed Archive (`.tar.gz`)**

This method gives you a standalone executable that can be run from any directory.

1.  Extract the archive:

    ```bash
    tar -xzf erigon_v3.x.x_linux_amd64.tar.gz
    ```

    (Replace the filename with your downloaded version)
2.  Move the resulting `erigon` executable to a directory included in your system's <kbd>$PATH</kbd>(e.g., <kbd>$/usr/local/bin</kbd>) to run it from anywhere:

    ```bash
    sudo mv erigon /usr/local/bin/
    ```

### 4. Running Erigon

After installation, you can run Erigon from your terminal:

```bash
erigon [options]
```

</details>

<details>

<summary>Build from Source</summary>

{% hint style="warning" %}
#### ⚠️ Warning: Installing Erigon from Source

Installing Erigon directly from source requires a strong foundation in Command Line Interface (CLI) operations and Linux/Unix environments.

This method is significantly more difficult than using pre-built binaries or Docker images. You will be responsible for resolving dependency issues, configuring build tools, and manually managing all compilation and execution steps.

If you are a casual user or lack CLI expertise, we strongly recommend using the official pre-built binaries or Docker documentation.
{% endhint %}

### 1. Software Requirements

If you intend to build Erigon from source, you must first meet the necessary prerequisites.

#### 1.1 Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit [https://git-scm.com/downloads](https://git-scm.com/downloads).

#### 1.2 Build essential and Cmake (Linux only)

Install **Build-essential** and **Cmake**:

```bash
sudo apt install build-essential cmake -y
```

#### 1.3 Go Programming Language

Erigon utilizes Go (also known as Golang) version 1.24 or newer for part of its development. It is recommended to have a fresh Go installation. If you have an older version, consider deleting the `/usr/local/go` folder (you may need to use `sudo`) and re-extract the new version in its place.

To install the latest Go version, visit the official documentation at [https://golang.org/doc/install](https://golang.org/doc/install).

#### 1.4 C++ Compiler

This turns the C++ part of Erigon's code into a program your computer can run. You can use either **Clang** or **GCC**:

* For **Clang** follow the instructions at [https://clang.llvm.org/get\_started.html](https://clang.llvm.org/get_started.html);
* For **GCC** (version 10 or newer): [https://gcc.gnu.org/install/index.html](https://gcc.gnu.org/install/index.html).

### 2. Building Erigon from Source

The basic Erigon configuration is suitable for most users who simply want to run a node. To ensure you are building a specific, stable release, use Git tags.

**2.1 Clone the Erigon repository**

First, clone the Erigon repository (you do not need to specify a branch):

```
git clone https://github.com/erigontech/erigon.git
cd erigon
```

**2.2 Check Out the Desired Stable Version (Tag)**

Next, fetch all available release tags:

```sh
git fetch --tags
```

Check out the desired version tag by replacing `<tag_name>` with the version you want. Normally latest stable version is the best, check the official [Release Notes](https://github.com/erigontech/erigon/releases). For example:

```sh
git checkout v3.2.2
```

**2.3 Compile the Software**

Compile the Erigon binary using the `make` command.

Standard Compilation:

```sh
make erigon
```

Fast Compilation (Recommended): to significantly speed up the process, specify the number of processors you want to use with the `-j<n>` option, where `<n>` is the number of processor you want to use (we recommend using a number slightly less than your total core count):

```sh
make -j<n> erigon
```

The resulting executable binary will be created in the `./build/bin/erigon` path.

### 3. Running Erigon

After installation, you can run Erigon from your terminal:

```sh
./build/bin/erigon [options]
```

</details>

### Windows

<details>

<summary>Native Compilation</summary>

### 1. Software Prerequisites

You must install the following software and set the below environment variables before compiling Erigon.

#### 1.1 Chocolatey

Install _Chocolatey package manager_ by following these [instructions](https://docs.chocolatey.org/en-us/choco/setup).

Once your Windows machine has the above installed, open the **Command Prompt** by typing "**cmd**" in the search bar and check that you have correctly installed Chocolatey:

```bash
choco -v
```

<figure><img src="../../.gitbook/assets/image (2).png" alt=""><figcaption></figcaption></figure>

#### 1.2 `cmake`, `make`, `mingw`

Now you need to install the following components: `cmake`, `make`, `mingw` by:

```bash
choco install cmake make mingw
```

{% hint style="warning" %}
**Important note about Anti-Virus:**

During the compiler detection phase of **MinGW**, some temporary executable files are generated to test the compiler capabilities. It's been reported that some anti-virus programs detect these files as possibly infected with the `Win64/Kryptic.CIS` Trojan horse (or a variant of it). Although these are false positives, we have no control over the 100+ vendors of security products for Windows and their respective detection algorithms and we understand that this may make your experience with Windows builds uncomfortable. To work around this, you can either set exclusions for your antivirus software specifically for the`build\bin\mdbx\CMakeFiles` subfolder of the cloned repo, or you can run Erigon using the other two options below.
{% endhint %}

#### 1.3 Git

Git is a tool that helps download and manage the Erigon source code. To install Git, visit [https://git-scm.com/downloads](https://git-scm.com/downloads).

#### **1.4 Set the System Environment Variable**

Make sure that the Windows System Path variable is set correctly. Use the search bar on your computer to search for “**Edit the system environment variable**”.

<figure><img src="../../.gitbook/assets/image (3).png" alt=""><figcaption></figcaption></figure>

Click the “**Environment Variables...**” button.

<figure><img src="../../.gitbook/assets/image (4).png" alt=""><figcaption></figcaption></figure>

Look down at the "**System variables**" box and double click on "**Path**" to add a new path.

<figure><img src="../../.gitbook/assets/image (5).png" alt=""><figcaption></figcaption></figure>

Then click on the "**New**" button and paste the following path:

```bash
 C:\ProgramData\chocolatey\lib\mingw\tools\install\mingw64\bin
```

<figure><img src="../../.gitbook/assets/image (6).png" alt=""><figcaption></figcaption></figure>

### 2. Clone the Erigon repository

Open the Command Prompt and type the following:

{% include "../../.gitbook/includes/git-clone-https-github.co....md" %}

Next, fetch all available release tags:

{% include "../../.gitbook/includes/git-fetch-tags.md" %}

Check out the desired version tag by replacing `<tag_name>` with the version you want. Normally latest stable version is the best, check the official [Release Notes](https://github.com/erigontech/erigon/releases). For example:

{% include "../../.gitbook/includes/git-checkout-version.md" %}

You might need to change the `ExecutionPolicy` to allow scripts created locally or signed by a trusted publisher to run. Open a Powershell session as Administrator and type:

```powershell
Set-ExecutionPolicy RemoteSigned
```

### 3. Compiling Erigon

This section outlines the two primary methods for compiling the Erigon client and its associated modules directly from the source code on a Windows environment. Compiling from source ensures you are running the latest version and gives you control over the final binaries.

You have two alternative options for compilation, both utilizing PowerShell: a quick, graphical method via File Explorer, and a more controlled, command-line method. All successfully compiled binaries will be placed in the `.\build\bin\` subfolder of your Erigon directory.

<table data-header-hidden><thead><tr><th width="185.22216796875"></th><th></th><th></th></tr></thead><tbody><tr><td><strong>Method</strong></td><td><strong>Pro</strong></td><td><strong>Con</strong></td></tr><tr><td>a. File Explorer (<code>wmake.ps1</code>)</td><td>Fastest and simplest; requires minimal command-line interaction.</td><td>Less control over which specific component is built (builds all modules by default).</td></tr><tr><td>b. PowerShell CLI</td><td>Provides granular control, allowing you to compile only specific components (e.g., just <code>erigon</code> or <code>rpcdaemon</code>).</td><td>Requires CLI familiarity and an additional step to modify the <code>ExecutionPolicy</code> for script permission.</td></tr></tbody></table>

#### a. File Explorer (`wmake.ps1`)

This is the fastest way which normally works for everyone. Open the File Explorer and go to the Erigon folder, then right click the `wmake` file and choose "**Run with PowerShell**".

<figure><img src="../../.gitbook/assets/image (7).png" alt=""><figcaption></figcaption></figure>

PowerShell will compile Erigon and all of its modules. All binaries will be placed in the `.\build\bin\` subfolder.

<figure><img src="../../.gitbook/assets/image (8).png" alt=""><figcaption></figcaption></figure>

#### b. PowerShell CLI

In the search bar on your computer, search for “**Windows PowerShell**” and open it.

<figure><img src="../../.gitbook/assets/image (9).png" alt=""><figcaption></figcaption></figure>

Change the working directory to "**erigon**"

```bash
cd erigon
```

<figure><img src="../../.gitbook/assets/image (10).png" alt=""><figcaption></figcaption></figure>

Before modifying security settings, ensure PowerShell script execution is allowed in your Windows account settings using the following command:

```powershell
Set-ExecutionPolicy Bypass -Scope CurrentUser -Force
```

This change allows script execution, but use caution to avoid security risks. Remember to only make these adjustments if you trust the scripts you intend to run. Unauthorized changes can impact system security. For more info read [Set-Execution Policy](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.security/set-executionpolicy?view=powershell-7.3) documentation.

Now you can compile Erigon and/or any of its component:

```powershell
.\wmake.ps1 [-target] <targetname>
```

For example, to build the Erigon executable write:

```powershell
.\wmake.ps1 erigon
```

<figure><img src="../../.gitbook/assets/image (11).png" alt=""><figcaption></figcaption></figure>

The executable binary `erigon.exe` should have been created in the `.\build\bin\` subfolder.

You can use the same command to build other binaries such as `RPCDaemon`, `TxPool`, `Sentry` and `Downloader`.

### 4. Running Erigon

To start Erigon place your command prompt in the `.\build\bin\` subfolder and use:

```powershell
start erigon.exe
```

or from any place use the full address of the executable:

```powershell
start C:\Users\username\AppData\Local\erigon.exe
```

</details>

<details>

<summary>Window Subsystem for Linux (WSL)</summary>

WSL enables you to run a complete GNU/Linux environment natively within Windows, offering Linux compatibility without the performance and resource overhead of traditional virtual machines.

#### Installation and Version

* Official Installation: Follow Microsoft's official guide to install WSL: [https://learn.microsoft.com/en-us/windows/wsl/install](https://learn.microsoft.com/en-us/windows/wsl/install)
* Required Version: WSL Version 2 is the only version supported by Erigon.

#### Building Erigon

Once WSL 2 is set up, you can build and run Erigon exactly as you would on a regular [#linux](./#linux "mention") distribution.

#### Performance and Data Storage

The location of your Erigon data directory (`datadir`) is the most crucial factor for performance in WSL.

| **Data Location**                                                         | **Performance & Configuration**                                                                                                                                                                                                              |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Recommended: Native Linux Filesystem (e.g., in your Linux home directory) | Optimal Performance. Erigon runs without restrictions, and the embedded RPC daemon works efficiently.                                                                                                                                        |
| Avoid: Mounted Windows Partitions (e.g., `/mnt/c/`, `/mnt/d/`)            | Significantly Affected Performance. This is due to: \<ul>\<li>The mounted drives using DrvFS (a slower network file system).\</li>\<li>The MDBX database locking the data for exclusive access, limiting simultaneous processes.\</li>\</ul> |

#### RPC Daemon Configuration

The choice of data location directly impacts how you must configure the RPC daemon:

| **Scenario**                                  | **RPC Daemon Requirement**                                                               |
| --------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Data on Native Linux Filesystem (Recommended) | Use the Embedded RPC Daemon. This is the highly preferred and most efficient method.     |
| Data on Mounted Windows Partition             | The `rpcdaemon` must be configured as a remote DB even when running on the same machine. |

{% hint style="warning" %}
⚠️ Warning: The remote DB RPC daemon is an experimental feature, is not recommended, and is extremely slow. Always aim to use the embedded RPC daemon by keeping your data on the native Linux filesystem.
{% endhint %}

#### Networking Notes

Be aware that the default WSL 2 environment uses its own internal IP address, which is distinct from the IP address of your Windows host machine.

If you need to connect to Erigon from an external network (e.g., opening a port on your home router for peering on port `30303`), you must account for this separate WSL 2 IP address when configuring NAT on your router.

</details>

Once you have Erigon installed, you can see Basic Usage to configure your node.

{% content-ref url="../fundamentals/basic-usage.md" %}
[basic-usage.md](../fundamentals/basic-usage.md)
{% endcontent-ref %}
