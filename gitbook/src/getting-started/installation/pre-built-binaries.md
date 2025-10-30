---
description: Downloading and Verifying Erigon Pre-built Binaries
---

# Pre-built binaries

You can download and install the latest version of Erigon directly from our GitHub releases page.

### 1. Select Your Platform and Download ⬇️

Go to the Erigon [releases page](https://github.com/erigontech/erigon/releases) on GitHub and select the latest stable version (e.g., <code class="expression">space.vars.version</code>) or whichever version you prefer.

<figure><img src="../../.gitbook/assets/image (1).png" alt=""><figcaption></figcaption></figure>

Download the appropriate binary file for your operating system and processor architecture:

<table data-header-hidden><thead><tr><th width="132"></th><th width="170"></th><th width="191"></th><th></th></tr></thead><tbody><tr><td><strong>Operating System</strong></td><td><strong>Processor Type</strong></td><td><strong>Binary File Type</strong></td><td><strong>Example File Name</strong></td></tr><tr><td>Linux</td><td>64-bit Intel/AMD</td><td>Debian Package (<code>.deb</code>)</td><td>erigon_3.x.x_amd64.deb</td></tr><tr><td>Linux</td><td>64-bit Intel/AMD</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v3.x.x_linux_amd64.tar.gz</td></tr><tr><td>Linux</td><td>64-bit ARM</td><td>Debian Package (<code>.deb</code>)</td><td>erigon_3.x.x_arm64.deb</td></tr><tr><td>Linux</td><td>64-bit ARM</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td>erigon_v3.x.x_linux_arm64.tar.gz</td></tr><tr><td>macOS</td><td>64-bit Intel/AMD</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td><span class="math">$\text{erigon\_3.x.x\_darwin\_amd64.tar.gz}$</span></td></tr><tr><td>macOS</td><td>64-bit ARM</td><td>Compressed Archive (<code>.tar.gz</code>)</td><td><span class="math">$\text{erigon\_3.x.x\_darwin\_arm64.tar.gz}$</span></td></tr></tbody></table>

### 2. Verifying Binary Integrity with Checksums

To verify the integrity and ensure your downloaded Erigon file hasn't been corrupted or tampered with, use the SHA256 checksums provided in the official release by following these steps:

{% stepper %}
{% step %}
### Download the Checksum File

First, download the official checksum file (`erigon_v3.x.x_checksums.txt`) from the same release page as your binary. (The example below uses `wget`, but you can also use your browser.)

```bash
BASE="https://github.com/erigontech/erigon/releases/download/v3.x.x"
wget $BASE/erigon_v3.x.x_checksums.txt
```

For example:

{% code overflow="wrap" %}
```bash
wget https://github.com/erigontech/erigon/releases/download/v3.2.1/erigon_v3.2.1_checksums.txt
```
{% endcode %}
{% endstep %}

{% step %}
### Generate the Checksum of Your Downloaded File

Next, use the appropriate command for your operating system and the name of your downloaded binary (e.g., `erigon_v3.x.x_linux_amd64.tar.gz`).

| **Operating System** | **Command to Generate Checksum**       |
| -------------------- | -------------------------------------- |
| Linux                | `sha256sum <DOWNLOADED_FILE_NAME>`     |
| macOS                | `shasum -a 256 <DOWNLOADED_FILE_NAME>` |

Example (Linux with a `tar.gz` file):

```bash
sha256sum erigon_v3.x.x_linux_amd64.tar.gz
```

This command will output a long string (the computed checksum) followed by the file name.
{% endstep %}

{% step %}
### Compare the Checksums

Compare the checksum generated in Step 2 with the checksum provided in the `erigon_v3.x.x_checksums.txt` file.

1. Open the `erigon_v3.x.x_checksums.txt` file.
2. Find the line corresponding to your downloaded file.
3. The two checksum strings must match exactly. If they do not match, the file is corrupted, and you should delete it and download it again.
{% endstep %}
{% endstepper %}

### 3. Installing the Binary Executable

After downloading and verifying the checksum, follow the instructions below based on the file type you chose.

#### Installation on Linux

**A. Using Debian Package (`.deb`)**

This method uses your distribution's package manager (like $$ $\text{dpkg}$ $$) to install Erigon system-wide.

1.  Navigate to the download directory:

    ```bash
    cd ~/Downloads
    ```
2.  Install the package:

    ```bash
    sudo dpkg -i erigon_3.x.x_amd64.deb
    ```

    (Replace the filename with your downloaded version.)

**B. Using Compressed Archive (`.tar.gz`)**

This method gives you a standalone executable that can be run from any directory.

1.  Extract the archive:

    ```bash
    tar -xzf erigon_v3.x.x_linux_amd64.tar.gz
    ```
2.  Move the resulting `erigon` executable to a directory included in your system's $$ $\text{PATH}$ $$ (e.g., $$ $\text{/usr/local/bin}$ $$) to run it from anywhere:

    ```bash
    sudo mv erigon /usr/local/bin/
    ```

#### Installation on macOS

macOS binaries are provided as compressed archives (`.tar.gz`).

1.  Extract the archive:

    ```bash
    tar -xzf erigon_3.x.x_darwin_amd64.tar.gz
    ```
2.  Move the resulting $$ $\text{erigon}$ $$ executable to a convenient location, such as $$ $\text{/usr/local/bin/}$ $$, to allow running it directly from the terminal:

    ```bash
    sudo mv erigon /usr/local/bin/
    ```

    You may need to grant execution permissions:

    ```bash
    sudo chmod +x /usr/local/bin/erigon
    ```

#### Running Erigon

After installation, you can run Erigon from your terminal:

```bash
erigon [options]
```

See Basic Usage for more info.

{% content-ref url="../../fundamentals/basic-usage.md" %}
[basic-usage.md](../../fundamentals/basic-usage.md)
{% endcontent-ref %}
